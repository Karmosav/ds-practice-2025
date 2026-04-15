import grpc
from concurrent import futures
import os
from utils.pb.books_database import booksdatabase_pb2_grpc

# Titles must match checkout line items after splitting on " by " (see orderqueue execute_order).
DEFAULT_INVENTORY = {
    "Harry Potter and the Philosopher's Stone": 500,
    "The Hobbit": 500,
}


# Helper to get backup addresses from env (comma-separated)
def get_backup_addresses():
    return [addr for addr in os.getenv("BOOKSDB_BACKUPS", "").split(",") if addr]


def serve_books():
    # Create gRPC stubs for backup replicas
    backup_addresses = get_backup_addresses()
    backup_stubs = []
    for addr in backup_addresses:
        channel = grpc.insecure_channel(addr)
        stub = booksdatabase_pb2_grpc.BooksDatabaseStub(channel)
        backup_stubs.append(stub)

    # Create the PrimaryReplica servicer
    primary_servicer = booksdatabase_pb2_grpc.PrimaryReplica(backup_stubs)
    primary_servicer.store.update(DEFAULT_INVENTORY)
    role = "primary" if backup_stubs else "backup"
    print(
        f"[BooksDB] {role} started with {len(primary_servicer.store)} seeded title(s): "
        f"{list(primary_servicer.store.keys())}"
    )

    # Start the gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    booksdatabase_pb2_grpc.add_BooksDatabaseServicer_to_server(primary_servicer, server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print(
        f"[BooksDB] listening on 50051 role={role} backup_targets={backup_addresses!r}"
    )
    server.wait_for_termination()


if __name__ == "__main__":
    serve_books()
