from datetime import datetime
from faws.sqs.queue import Queue, Message
from unittest import mock


def test_equal():
    now = datetime(2020, 5, 28, 0, 0, 0)
    with mock.patch("datetime.datetime") as dt:
        dt.now.return_value = now
        queue = Queue("test_queue")
        same = Queue("test_queue")
        other = Queue("other_queue")
        assert queue == same
        assert queue != other


def test_add_message():
    queue = Queue("test-queue")
    message = queue.add_message(
        "takerun"
    )
    assert message.message_id in queue.messages


def test_message_set_delay():
    message = Message(
        message_body="hoge",
        message_id="1111",
        message_inserted_at=datetime(2020, 5, 28, 0, 0, 0),
        delay_seconds=10
    )

    assert message.message_deliverable_time == datetime(2020, 5, 28, 0, 0, 10)
