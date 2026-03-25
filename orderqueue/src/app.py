import threading

class OrderQueueService:
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = []  # Could be replaced with a priority structure

    def Enqueue(self, request, context):
        # TODO: lock queue, insert request.orderId
        # TODO: return success response
        pass

    def Dequeue(self, request, context):
        # TODO: lock queue, pop an order if available
        # TODO: return the dequeued order or an empty result
        pass


def serve_queue_service():
    # TODO: create gRPC server, add OrderQueueService, start server
    pass