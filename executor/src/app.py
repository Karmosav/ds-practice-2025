class ExecutorService:
    def __init__(self, executor_id, known_ids, queue_stub):
        self.executor_id = executor_id
        self.known_ids = known_ids  # IDs of other executors
        self.queue_stub = queue_stub
        self.leader_id = None

    def start_leader_election(self):
        # TODO: pick a leader using your chosen algorithm
        pass

    def run(self):
        # TODO: if I'm the leader, repeatedly dequeue and "execute" orders
        # else, wait or watch for changes in leadership
        pass


def launch_executor(executor_id, known_ids):
    # TODO: set up gRPC, connect to order queue
    # queue_stub = ...
    svc = ExecutorService(executor_id, known_ids, queue_stub=None)  # placeholder
    svc.start_leader_election()
    svc.run()