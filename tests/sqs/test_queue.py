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


@mock.patch("datetime.datetime")
@pytest.mark.parametrize("delay_seconds", [0, 10])
def test_add_message(dt, delay_seconds):
    now = datetime(2020, 5, 20, 0, 0, 0)
    expected_deliverable_time = datetime(2020, 5, 20, 0, 0, 0 + delay_seconds)
    dt.now.return_value = now
    queue = Queue("test-queue")
    message = queue.add_message("takerun", delay_seconds=delay_seconds)
    assert message.message_id in queue.messages and message.message_deliverable_time == expected_deliverable_time


@pytest.mark.parametrize(
    "visibility_timeout,deliverable_second", [(0, 0), (None, 30), (45, 45)]
)
def test_get_message(visibility_timeout, deliverable_second):
    queue = Queue("test-queue")
    now = datetime(2020, 5, 1, 0, 0, 0)
    with mock.patch("uuid.uuid4", return_value="1111"), mock.patch("datetime.datetime") as dt:
        dt.now.return_value = now
        message = queue.add_message("takerun")

        assert queue.get_message(visibility_timeout) == message \
            and message.message_deliverable_time == datetime(2020, 5, 1, 0, 0, deliverable_second)


def test_get_message_exist_uncallable_message():
    queue = Queue("test-queue")

    with mock.patch("faws.sqs.message.Message.is_callable", return_value=False):
        message = queue.add_message("takerun")
        assert queue.get_message() is None
