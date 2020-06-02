from __future__ import annotations
from faws.sqs.queue import Queue
from faws.sqs import QueuesStorageType
from faws.sqs.queues_storage import build_queues_storage
from typing import List


class Queues:

    def __init__(self, queue_storage_type: QueuesStorageType, **kwargs):
        self._queues_storage = build_queues_storage(queue_storage_type, **kwargs)

    @property
    def queues(self):
        return self._queues_storage.queues

    def create_queue(self, queue_name: str) -> Queue:
        queue = self._queues_storage.get_queue(queue_name)
        if queue is not None:
            return queue
        queue = self._queues_storage.create_queue(queue_name)

        return queue

    def get_queues(self) -> List[Queue]:
        return self._queues_storage.get_queues()

    def get_queue(self, queue_name: str) -> Queue:
        return self._queues_storage.get_queue(queue_name)
