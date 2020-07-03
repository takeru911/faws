from datetime import datetime
from faws.sqs.queue import Queue, name_from_url, Tag
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
    assert message.message_deliverable_time == expected_deliverable_time


@pytest.mark.parametrize(
    "visibility_timeout,deliverable_second,num_of_message",
    [(0, 0, 1), (None, 30, 5), (45, 45, 100)],
)
def test_get_message(visibility_timeout, deliverable_second, num_of_message):
    queue = Queue("test-queue")
    now = datetime(2020, 5, 1, 0, 0, 0)
    with mock.patch("datetime.datetime") as dt:
        dt.now.return_value = now
        added_message = []
        for i in range(num_of_message):
            message = queue.add_message("takerun")
            added_message.append(message)
        received_message = queue.get_message(
            visibility_timeout, max_number_of_messages=num_of_message
        )

        assert (
            received_message == added_message
            and len(
                # deliverable timeが正常な値を持たないmessageを抽出し、0件であることをチェック
                [
                    message
                    for message in added_message
                    if message.message_deliverable_time
                    != datetime(2020, 5, 1, 0, 0, deliverable_second)
                ]
            )
            == 0
        )


def test_get_message_exist_uncallable_message():
    queue = Queue("test-queue")

    with mock.patch("faws.sqs.message.Message.is_callable", return_value=False):
        queue.add_message("takerun")
        assert queue.get_message() == []


@pytest.mark.parametrize(
    "input_, expected",
    [
        ("http://localhost:5000/queues/test_queue", "test_queue"),
        ("http://localhost:5000/test_queue_1", "test_queue_1"),
        ("http:///test_queue", "test_queue"),
        ("https:///test_queue_1", "test_queue_1"),
        ("https://ap-northeast-1.queue.amaz/test-queue", "test-queue"),
        ("https://ap-northeast-1.queue.amaz/", ""),
    ],
)
def test_queue_name_from_queue_url(input_, expected):
    actual = name_from_url(input_)
    assert actual == expected


@pytest.mark.parametrize("invalid_url", ["hoge://hugahuga/queue", "http://hoge"])
def test_queue_name_from_queue_url_invalid_url(invalid_url):
    with pytest.raises(ValueError):
        name_from_url(invalid_url)


def test_purge_message():
    queue = Queue("test-queue")
    queue.add_message("hoge")
    queue.purge_message()
    assert queue.get_message() == []


def test_set_tag():
    queue = Queue("test-queue")
    tag = Tag("key", "value")
    queue.set_tag(tag)

    assert queue._tags == {tag.name: tag}


def test_list_tags():
    queue = Queue("test-queue")
    tag = Tag("key", "value")
    queue.set_tag(tag)

    assert queue.list_tags() == [tag]
