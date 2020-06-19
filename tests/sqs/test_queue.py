from datetime import datetime
from faws.sqs.queue import Queue, Message
from unittest import mock
import pytest


def test_equal():
    now = datetime(2020, 5, 28, 0, 0, 0)
    with mock.patch("datetime.datetime") as dt:
        dt.now.return_value = now
        queue = Queue("test_queue")
        same = Queue("test_queue")
        other = Queue("other_queue")
        assert queue == same
        assert queue != other



def test_add_message(dt):
    dt.return_value = datetime(2020, 5, 20, 0, 0, 0)
    queue = Queue("test-queue")
    message = queue.add_message("takerun")
    assert message.message_id in queue.messages

@mock.patch("datetime.datetime")
@pytest.mark.parametrize("message_attr,message", [
    {"message_body": "test", "message_attributes": None, "delay_seconds": 0},
    {"message_body": "test", "message_attributes": None, "delay_seconds": 10},
])
def test_get_message():
    queue = Queue("test-queue")

    with mock.patch("uuid.uuid4", return_value="1111"), mock.patch("datetime.datetime."):
        message = queue.add_message("takerun")

        assert queue.get_message() == message


def test_get_message_exist_uncallable_message():
    queue = Queue("test-queue")

    with mock.patch("faws.sqs.message.Message.is_callable", return_value=False):
        message = queue.add_message("takerun")
        assert queue.get_message() is None
