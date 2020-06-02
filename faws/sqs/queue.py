from __future__ import annotations
import datetime


class Queue:
    def __init__(self, queue_name: str):
        self._queue_name = queue_name
        self._queue_url = f"https://sqs-{self.queue_name}"
        self._created_at = datetime.datetime.now()

    @property
    def queue_name(self) -> str:
        return self._queue_name

    @property
    def queue_url(self) -> str:
        return self._queue_url

    @property
    def created_at(self) -> datetime:
        return self._created_at

    def __eq__(self, other: Queue) -> bool:
        return self.queue_url == other.queue_url \
               and self.queue_name == other.queue_name \
               and self.created_at == other.created_at

