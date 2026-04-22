import grpc
from concurrent import futures
import os
import socket
import threading
import time

from utils.pb.books_database import booksdatabase_pb2
from utils.pb.books_database import booksdatabase_pb2_grpc

# Titles must match checkout line items after splitting on " by " (see orderqueue execute_order).
DEFAULT_INVENTORY = {
    "Harry Potter and the Philosopher's Stone": 500,
    "The Hobbit": 500,
}

class BooksDatabaseTwoPhaseParticipant(booksdatabase_pb2_grpc.BooksDatabaseServicer):
    def __init__(self, backup_stubs, role="backup", backup_registry=None):
        self.backups = backup_stubs
        self.role = role
        self.backup_registry = backup_registry
        self.store = {}
        self._prepared_updates = {}
        self._prepared_lock = threading.Lock()

    def _replica_label(self):
        return "BooksDB-primary" if self.role == "primary" else "BooksDB-backup"

    def _current_backup_stubs(self):
        if self.backup_registry is not None:
            return self.backup_registry.get_stubs()
        return self.backups

    def Read(self, request, context):
        title = request.title
        stock = self.store.get(title, 0)
        print(f"[{self._replica_label()}] Read title={title!r} stock={stock}")
        return booksdatabase_pb2.ReadResponse(stock=stock)

    def Write(self, request, context):
        title = request.title
        new_stock = request.new_stock
        self.store[title] = new_stock
        print(f"[{self._replica_label()}] Write title={title!r} new_stock={new_stock}")

        for index, backup in enumerate(self._current_backup_stubs()):
            try:
                backup.Write(request)
                print(f"[BooksDB-primary] replicated write to backup[{index}] title={title!r}")
            except Exception as replication_error:
                print(f"[BooksDB-primary] replication to backup[{index}] failed: {replication_error}")

        return booksdatabase_pb2.WriteResponse(success=True)

    def Prepare(self, request, context):
        order_id = request.order_id
        updates = list(request.updates)
        if not order_id:
            return booksdatabase_pb2.PrepareResponse(ready=False, message="missing order_id")
        if not updates:
            return booksdatabase_pb2.PrepareResponse(ready=False, message="no staged updates")

        for update in updates:
            if update.new_stock < 0:
                return booksdatabase_pb2.PrepareResponse(
                    ready=False,
                    message=f"negative stock for title {update.title}",
                )

        with self._prepared_lock:
            self._prepared_updates[order_id] = updates

        print(f"[BooksDB-2PC] Prepared order={order_id} staged_updates={len(updates)}")
        return booksdatabase_pb2.PrepareResponse(ready=True, message="prepared")

    def Commit(self, request, context):
        order_id = request.order_id
        with self._prepared_lock:
            staged_updates = self._prepared_updates.pop(order_id, None)

        if staged_updates is None:
            return booksdatabase_pb2.CommitResponse(success=False, message="order not prepared")

        for update in staged_updates:
            self.Write(
                booksdatabase_pb2.WriteRequest(title=update.title, new_stock=update.new_stock),
                context,
            )

        print(f"[BooksDB-2PC] Committed order={order_id} applied_updates={len(staged_updates)}")
        return booksdatabase_pb2.CommitResponse(success=True, message="committed")

    def Abort(self, request, context):
        order_id = request.order_id
        with self._prepared_lock:
            self._prepared_updates.pop(order_id, None)
        print(f"[BooksDB-2PC] Aborted order={order_id}")
        return booksdatabase_pb2.AbortResponse(aborted=True)

# Helper to get backup addresses from env (comma-separated)
def get_backup_addresses():
    return [addr for addr in os.getenv("BOOKSDB_BACKUPS", "").split(",") if addr]


def resolve_service_targets(service_name, port):
    try:
        infos = socket.getaddrinfo(service_name, port, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror as dns_error:
        print(f"[BooksDB-primary] backup discovery DNS failed for {service_name}:{port} err={dns_error}")
        return []
    ips = sorted({info[4][0] for info in infos})
    return [f"{ip}:{port}" for ip in ips]


class BackupRegistry:
    def __init__(self, static_targets, discovery_service, discovery_port, refresh_seconds):
        self.static_targets = list(static_targets)
        self.discovery_service = discovery_service
        self.discovery_port = discovery_port
        self.refresh_seconds = refresh_seconds
        self._lock = threading.Lock()
        self._entries = []
        self._thread = None
        self._stop = threading.Event()

    def _desired_targets(self):
        discovered = []
        if self.discovery_service:
            discovered = resolve_service_targets(self.discovery_service, self.discovery_port)
        return sorted(set(self.static_targets + discovered))

    def _refresh(self):
        desired = self._desired_targets()
        with self._lock:
            current = {entry[0]: entry for entry in self._entries}
            next_entries = []

            for target in desired:
                if target in current:
                    next_entries.append(current.pop(target))
                    continue
                channel = grpc.insecure_channel(target)
                stub = booksdatabase_pb2_grpc.BooksDatabaseStub(channel)
                next_entries.append((target, channel, stub))

            for _, stale_channel, _ in current.values():
                try:
                    stale_channel.close()
                except Exception:
                    pass

            self._entries = next_entries

        print(f"[BooksDB-primary] backup targets refreshed count={len(desired)} targets={desired!r}")

    def get_stubs(self):
        with self._lock:
            return [entry[2] for entry in self._entries]

    def start(self):
        self._refresh()

        def worker():
            while not self._stop.wait(self.refresh_seconds):
                self._refresh()

        self._thread = threading.Thread(target=worker, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        with self._lock:
            for _, channel, _ in self._entries:
                try:
                    channel.close()
                except Exception:
                    pass
            self._entries = []


def serve_books():
    role = os.getenv("BOOKSDB_ROLE", "").strip().lower() or "backup"
    backup_addresses = get_backup_addresses()
    backup_registry = None
    backup_stubs = []

    if role == "primary":
        backup_service = os.getenv("BOOKSDB_BACKUP_SERVICE", "").strip()
        backup_port = int(os.getenv("BOOKSDB_BACKUP_PORT", "50051"))
        refresh_seconds = float(os.getenv("BOOKSDB_DISCOVERY_INTERVAL_SECONDS", "5"))
        backup_registry = BackupRegistry(
            static_targets=backup_addresses,
            discovery_service=backup_service,
            discovery_port=backup_port,
            refresh_seconds=refresh_seconds,
        )
        backup_registry.start()
    else:
        # Backward-compatible static mode for non-primary instances.
        for addr in backup_addresses:
            channel = grpc.insecure_channel(addr)
            stub = booksdatabase_pb2_grpc.BooksDatabaseStub(channel)
            backup_stubs.append(stub)

    # Create the 2PC-capable primary/backup servicer.
    participant_servicer = BooksDatabaseTwoPhaseParticipant(
        backup_stubs=backup_stubs,
        role=role,
        backup_registry=backup_registry,
    )
    participant_servicer.store.update(DEFAULT_INVENTORY)
    print(
        f"[BooksDB] {role} started with {len(participant_servicer.store)} seeded title(s): "
        f"{list(participant_servicer.store.keys())}"
    )

    # Start the gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    booksdatabase_pb2_grpc.add_BooksDatabaseServicer_to_server(participant_servicer, server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print(
        f"[BooksDB] listening on 50051 role={role} backup_targets={backup_addresses!r}"
    )
    try:
        server.wait_for_termination()
    finally:
        if backup_registry is not None:
            backup_registry.stop()


if __name__ == "__main__":
    serve_books()
