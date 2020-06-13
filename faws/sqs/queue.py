from __future__ import annotations
import datetime
from faws.sqs.message import Message
from typing import Dict


class Queue:
    def __init__(self, queue_name: str):
        self._queue_name = queue_name
        self._queue_url = f"https://localhost:5000/queues/{self.queue_name}"
        self._created_at = datetime.datetime.now()
        self._messages = {}

    @property
    def queue_name(self) -> str:
        return self._queue_name

    @property
    def queue_url(self) -> str:
        return self._queue_url

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def messages(self):
        return self._messages

    def add_message(
            self,
            message_body: str,
            message_attributes: Dict = None,
            delay_seconds: int = 0,
            message_system_attributes: Dict = None,
            message_deduplication_id: str = None,
            message_group_id: str = None
    ) -> Message:
        message = Message(
            message_body,
            message_attributes=message_attributes,
            delay_seconds=delay_seconds,
            message_system_attributes=message_system_attributes,
            message_deduplication_id=message_deduplication_id,
            message_group_id=message_group_id
        )
        self._messages[message.message_id] = message
        return message

    def get_message(self) -> Message:
        for message_id, message in self.messages.items():
            if message.is_callable():
                return message

    def __eq__(self, other: Queue) -> bool:
        return self.queue_url == other.queue_url \
               and self.queue_name == other.queue_name \
               and self.created_at == other.created_at
