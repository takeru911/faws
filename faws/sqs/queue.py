from __future__ import annotations
import datetime
import re
from faws.sqs.message import Message
from typing import Dict, Optional


def name_from_url(queue_url: str) -> str:
    if "http" not in queue_url and "https" not in queue_url:
        raise ValueError(f"The address {queue_url} is not valid for this endpoint.")
    m = re.match(r"https*:\/\/.*\/(.*)", queue_url)

    if len(m.groups()) != 1:
        raise ValueError(f"The address {queue_url} is not valid for this endpoint.")
    return m.groups()[0]


class Queue:
    def __init__(self, queue_name: str, default_visibility_timeout: int = 30):
        self._queue_name = queue_name
        self._queue_url = f"https://localhost:5000/queues/{self.queue_name}"
        self._created_at = datetime.datetime.now()
        self._messages = {}
        self._default_visibility_timeout = default_visibility_timeout

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

    @property
    def default_visibility_timeout(self) -> int:
        return self._default_visibility_timeout

    def add_message(
        self,
        message_body: str,
        message_attributes: Dict = None,
        delay_seconds: int = 0,
    ) -> Message:
        message = Message(
            message_body,
            message_attributes=message_attributes,
            delay_seconds=delay_seconds,
        )
        self._messages[message.message_id] = message
        return message

    def get_message(self, visibility_timeout: int = None) -> Optional[Message]:
        for message_id, message in self.messages.items():
            if message.is_callable():
                if visibility_timeout is None:
                    message.update_deliverable_time(self.default_visibility_timeout)
                    return message
                message.update_deliverable_time(visibility_timeout)
                return message
        return None

    def __eq__(self, other: Queue) -> bool:
        return (
            self.queue_url == other.queue_url
            and self.queue_name == other.queue_name
            and self.created_at == other.created_at
        )
