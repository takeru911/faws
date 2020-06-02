from __future__ import annotations
import enum
from abc import abstractmethod
from faws.sqs import Queue
from typing import List, Optional


def build_queues_storage(storage_type: QueueStorageType, **kwargs) -> QueuesStorage:
    return storage_type.value(**kwargs)


class QueuesStorage:

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def queues(self):
        raise NotImplementedError

    @abstractmethod
    def create_queue(self, queue_name) -> Queue:
        raise NotImplementedError

    @abstractmethod
    def get_queues(self) -> List[Queue]:
        raise NotImplementedError

    @abstractmethod
    def get_queue(self, queue_name: str) -> Queue:
        raise NotImplementedError


class InMemoryQueuesStorage(QueuesStorage):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._queues = {}

    @property
    def queues(self):
        return self._queues

    def create_queue(self, queue_name: str) -> Queue:
        if queue_name in self.queues:
            return self.queues[queue_name]
        queue = Queue(
            queue_name=queue_name
        )
        self._queues[queue_name] = queue

        return queue

    def get_queues(self) -> List[Queue]:
        return list(self.queues.values())

    def get_queue(self, queue_name: str) -> Optional[Queue]:
        return self.queues.get(queue_name)


class QueuesStorageType(enum.Enum):
    IN_MEMORY = InMemoryQueuesStorage
