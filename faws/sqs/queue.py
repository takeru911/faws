from __future__ import annotations
import datetime
import uuid
import dataclasses
from typing import Dict, Optional


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


def generate_uuid() -> str:
    return str(uuid.uuid4())


@dataclasses.dataclass
class Message:
    message_body: str
    message_attributes: Optional[Dict] = None
    delay_seconds: int = 0
    message_system_attributes: Optional[Dict] = None
    message_deduplication_id: Optional[str] = None
    message_group_id: Optional[str] = None
    message_inserted_at: datetime = datetime.datetime.now()
    message_id: str = dataclasses.field(init=False)
    message_deliverable_time: datetime = dataclasses.field(init=False)

    def __post_init__(self):
        self.message_id = generate_uuid()
        self.message_deliverable_time = self.message_inserted_at + datetime.timedelta(seconds=self.delay_seconds)

    def is_callable(self):
        return True
