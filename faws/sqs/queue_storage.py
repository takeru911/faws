from __future__ import annotations
import enum
from abc import abstractmethod
from typing import List, Optional
from faws.sqs import Queue
from faws.sqs.error import NonExistentQueue


def build_queues_storage(storage_type: QueuesStorageType, **kwargs) -> QueueStorage:
    return storage_type.value(**kwargs)


class QueueStorage:
    def __init__(self, **kwargs):
        pass

    @classmethod
    @abstractmethod
    def init_storage(cls):
        raise NotImplementedError

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

    @abstractmethod
    def delete_queue(self, queue_name: str):
        raise NotImplementedError


class InMemoryQueueStorage(QueueStorage):
    _queues = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def init_storage(cls):
        cls._queues = {}

    @property
    def queues(self):
        return InMemoryQueueStorage._queues

    def create_queue(self, queue_name: str) -> Queue:
        if queue_name in self.queues:
            return self.queues[queue_name]
        queue = Queue(queue_name=queue_name)
        InMemoryQueueStorage._queues[queue_name] = queue

        return queue

    def get_queues(self) -> List[Queue]:
        return list(self.queues.values())

    def get_queue(self, queue_name: str) -> Optional[Queue]:
        if queue_name not in self.queues:
            raise NonExistentQueue()
        return self.queues.get(queue_name)

    def delete_queue(self, queue_name: str) -> bool:
        self.get_queue(queue_name)
        del self._queues[queue_name]
        return True


class QueuesStorageType(enum.Enum):
    IN_MEMORY = InMemoryQueueStorage
