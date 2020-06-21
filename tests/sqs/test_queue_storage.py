import datetime

from pytest import fixture
from faws.sqs import Queue
from faws.sqs.queue_storage import InMemoryQueueStorage
from unittest import mock


class TestInMemoryQueuesStorage:
    @fixture
    def added_queues(self):
        created_at = datetime.datetime(2020, 5, 28, 0, 0, 0)
        queues_storage = InMemoryQueueStorage()
        with mock.patch("datetime.datetime") as mock_datetime:
            mock_datetime.now.return_value = created_at
            queues_storage.create_queue("test_queue")
        return queues_storage

    def create_free_created_at_queue(
        self, queue_name: str, created_at: datetime.datetime
    ) -> Queue:
        with mock.patch("datetime.datetime") as mock_datetime:
            mock_datetime.now.return_value = created_at
            queue = Queue(queue_name=queue_name)

            return queue

    def test_create_queue(self):
        queues_storage = InMemoryQueueStorage()
        queue_name = "test_queue"
        now = datetime.datetime(2020, 5, 28, 0, 0, 0)
        with mock.patch("datetime.datetime") as mock_datetime:
            mock_datetime.now.return_value = now
            actual = queues_storage.create_queue(queue_name)
            expected = self.create_free_created_at_queue(queue_name, now)
        assert actual == expected

    def test_created_queue_is_stored(self, added_queues):
        queue_name = "test_queue"
        expected = {
            queue_name: self.create_free_created_at_queue(
                queue_name, datetime.datetime(2020, 5, 28, 0, 0, 0)
            )
        }
        assert added_queues.queues == expected

    def test_create_already_exist_queue(self, added_queues):
        now_collect = datetime.datetime(2020, 5, 28, 0, 0, 0)
        now_incollect = datetime.datetime(2020, 5, 30, 0, 0, 0)
        queue_name = "test_queue"
        expected = {
            queue_name: self.create_free_created_at_queue(queue_name, now_collect)
        }
        with mock.patch("datetime.datetime") as mock_datetime:
            mock_datetime.now.return_value = now_incollect
            added_queues.create_queue("test_queue")
            assert added_queues.queues == expected

    def test_get_queues(self, added_queues):
        queue_name = "test_queue"
        expected = [
            self.create_free_created_at_queue(
                queue_name, datetime.datetime(2020, 5, 28, 0, 0, 0)
            )
        ]

        assert added_queues.get_queues() == expected
