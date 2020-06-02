import datetime
from faws.sqs import Queue, Queues, QueuesStorageType
from unittest import mock
from pytest import fixture


@fixture
def added_queues():
    created_at = datetime.datetime(2020, 5, 28, 0, 0, 0)
    queues = Queues(QueuesStorageType.IN_MEMORY)
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = created_at
        queues.create_queue("test_queue")
    return queues


def create_free_created_at_queue(queue_name: str, created_at: datetime.datetime) -> Queue:
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = created_at
        queue = Queue(
            queue_name=queue_name
        )

        return queue


def test_create_queue():
    queues = Queues(QueuesStorageType.IN_MEMORY)
    queue_name = "test_queue"
    now = datetime.datetime(2020, 5, 28, 0, 0, 0)
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = now
        actual = queues.create_queue(queue_name)
        expected = create_free_created_at_queue(queue_name, now)
    assert actual == expected


def test_created_queue_is_stored(added_queues):
    queue_name = "test_queue"
    expected = {
        queue_name: create_free_created_at_queue(
            queue_name,
            datetime.datetime(2020, 5, 28, 0, 0, 0)
        )
    }
    assert added_queues.queues == expected


def test_create_already_exist_queue(added_queues):
    now_collect = datetime.datetime(2020, 5, 28, 0, 0, 0)
    now_incollect = datetime.datetime(2020, 5, 30, 0, 0, 0)
    queue_name = "test_queue"
    expected = {
        queue_name: create_free_created_at_queue(queue_name, now_collect)
    }
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = now_incollect
        added_queues.create_queue("test_queue")
        assert added_queues.queues == expected


def test_get_queues(added_queues):
    queue_name = "test_queue"
    expected = [create_free_created_at_queue(
        queue_name,
        datetime.datetime(2020, 5, 28, 0, 0, 0)
    )]

    assert added_queues.get_queues() == expected
