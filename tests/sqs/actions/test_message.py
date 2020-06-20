from faws.sqs.actions import message
import pytest


@pytest.mark.parametrize(
    "input_, expected",
    [
        ("http://localhost:5000/queues/test_queue", "test_queue"),
        ("http://localhost:5000/test_queue_1", "test_queue_1"),
        ("http:///test_queue", "test_queue"),
        ("https:///test_queue_1", "test_queue_1"),
        ("https://ap-northeast-1.queue.amaz/test-queue", "test-queue"),
    ],
)
def test_queue_name_from_queue_url(input_, expected):
    actual = message.queue_name_from_queue_url(input_)
    assert actual == expected


def test_queue_name_from_queue_url_invalid_url():
    invalid_url = "hoge://hugahuga/queue"
    with pytest.raises(ValueError):
        message.queue_name_from_queue_url(invalid_url)
