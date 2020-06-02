from datetime import datetime
from faws.sqs.queue import Queue
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
