from __future__ import annotations
from abc import abstractmethod
from enum import Enum
from collections import OrderedDict
from typing import List
from faws.sqs.message import Message


def build_message_storage(
    message_storage_type: MessageStorageType, **kwargs
) -> MessageStorage:
    return message_storage_type.value(**kwargs)


class MessageStorage:
    @abstractmethod
    def get_messages(self, limit: int = 30, offset: int = 0) -> List[Message]:
        raise NotImplementedError

    @abstractmethod
    def add_message(self, message: Message):
        raise NotImplementedError

    @abstractmethod
    def truncate_messages(self):
        raise NotImplementedError


class InMemoryMessageStorage(MessageStorage):
    def __init__(self, **kwargs):
        self._messages = OrderedDict()

    def get_messages(self, limit: int = 30, offset: int = 0) -> List[Message]:
        messages = list(self._messages.values())
        for i in range(offset, len(self._messages), limit):
            yield messages[offset + i:limit]

    def add_message(self, message: Message):
        self._messages[message.message_id] = message
        return self._messages[message.message_id]

    def truncate_messages(self):
        self._messages = OrderedDict()


class MessageStorageType(Enum):
    IN_MEMORY = InMemoryMessageStorage
