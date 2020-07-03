from __future__ import annotations
import dataclasses
import datetime
import re
from typing import Dict, Optional, List
from faws.sqs.message import Message
from faws.sqs.message_storage import build_message_storage, MessageStorageType


def name_from_url(queue_url: str) -> str:
    if "http" not in queue_url and "https" not in queue_url:
        raise ValueError(f"The address {queue_url} is not valid for this endpoint.")
    m = re.match(r"https*://.*/(.*)", queue_url)

    if m is None:
        raise ValueError(f"The address {queue_url} is not valid for this endpoint.")

    return m.groups()[0]


class Queue:
    def __init__(self, queue_name: str, default_visibility_timeout: int = 30):
        self._queue_name = queue_name
        self._queue_url = f"https://localhost:5000/queues/{self.queue_name}"
        self._created_at = datetime.datetime.now()
        self._messages = build_message_storage(MessageStorageType.IN_MEMORY)
        self._default_visibility_timeout = default_visibility_timeout
        self._tags = {}

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

        return self._messages.add_message(message)

    def get_message(self, max_number_of_messages: int = 1, visibility_timeout: int = None) -> List[Message]:
        receive_messages = []
        for message in self._messages.get_messages():
            if message.is_callable():
                if visibility_timeout is None:
                    message.update_deliverable_time(self.default_visibility_timeout)
                message.update_deliverable_time(visibility_timeout)
                receive_messages.append(message)
            if len(receive_messages) == max_number_of_messages:
                return receive_messages
        return receive_messages

    def purge_message(self):
        self._messages.truncate_messages()

    def set_tag(self, tag: Tag):
        self._tags[tag.name] = tag

    def list_tags(self) -> List[Tag]:
        return list(self._tags.values())

    def __eq__(self, other: Queue) -> bool:
        return (
            self.queue_url == other.queue_url
            and self.queue_name == other.queue_name
            and self.created_at == other.created_at
        )


@dataclasses.dataclass()
class Tag:
    name: str
    value: str
