import pytest
from faws.sqs.message import Message
from faws.sqs.message_storage import (
    MessageStorageType,
    MessageStorage,
    InMemoryMessageStorage,
    build_message_storage,
)


class InMemoryMessageStorageTest:
    @pytest.fixture
    def in_memory_messages(self) -> MessageStorage:
        s = InMemoryMessageStorage()
        for i in range(0, 100):
            s.add_message(Message(f"{i}"))
        return s

    def test_build_message_storage(self):
        actual = build_message_storage(MessageStorageType.IN_MEMORY)
        assert actual.__class__ == InMemoryMessageStorage

    @pytest.mark.parametrize("limit,offset", [(30, 0), (10, 30)])
    def test_get_messages(self, in_memory_messages: MessageStorage, limit: int, offset: int):
        messages = in_memory_messages.get_messages(limit, offset)
        actual_message_ids = ",".join([message.message_body for message in messages])
        expected = ",".join(str(i) for i in list(range(offset, limit)))

        assert actual_message_ids == expected

    def test_add_message(self, in_memory_messages: MessageStorage):
        message = Message("test")
        in_memory_messages.add_message(message)
        messages = in_memory_messages.get_messages(200)
        assert message in messages

    def test_truncate_messages(self, in_memory_messages: MessageStorage):
        in_memory_messages.truncate_messages()

        assert len(in_memory_messages._messages) == 0

