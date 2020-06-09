import pytest
from unittest import mock
from faws.sqs import server


@pytest.fixture
def created_queue_server():
    server.create_queue("test_queue_1")

    return server


def test_parse_request_data():
    actual = server.parse_request_data("Action=ListQueue&version=2015-02-01")
    expected = {"Action": "ListQueue", "version": "2015-02-01"}

    assert actual == expected


def test_do_list_queues(created_queue_server):
    created_queue_server.create_queue("test_queue_2")
    assert server.do_operation({
        "Action": "ListQueues"
    }) == {
               "ListQueuesResponse": {
                   "ListQueuesResult": [
                       "https://localhost:5000/queues/test_queue_1",
                       "https://localhost:5000/queues/test_queue_2"
                    ],
                   "ResponseMetadata": {
                       "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                   }
               }
           }


def test_do_create_queue():
    assert server.do_operation({
        "Action": "CreateQueue",
        "QueueName": "test-queue"
    }) == {
               "CreateQueueResponse": {
                   "CreateQueueResult": {
                       "QueueUrl": f"https://localhost:5000/queues/test-queue"
                   },
                   "ResponseMetadata": {
                       "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                   }
               }
           }


def test_do_get_queue_url(created_queue_server):
    assert created_queue_server.do_operation({
        "Action": "GetQueueUrl",
        "QueueName": "test_queue_1"
    }) == {
               "GetQueueUrlResponse": {
                   "GetQueueUrlResult": {
                       "QueueUrl": f"https://localhost:5000/queues/test_queue_1"
                   },
                   "ResponseMetadata": {
                       "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                   }
               }
           }


def test_do_send_message(created_queue_server):
    with mock.patch("faws.sqs.queue.generate_uuid", return_value="1111"):
        assert created_queue_server.do_operation(
            {
                "Action": "SendMessage",
                "QueueUrl": "http://localhost:5000/queues/test_queue_1",
                "MessageBody": "taker"
            }
        ) == {
            "SendMessageResponse": {
                "SendMessageResult": {
                    "MD5OfMessageBody": "hogehoge",
                    "MD5OfMessageAttributes": "hugahuga",
                    "MessageId": "1111"
                },
                "ResponseMetadata": {
                    "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                }
            }
        }


def test_determine_operation_raises_when_non_exist_operation():
    with pytest.raises(NotImplementedError):
        server.do_operation({"Action": "NonImplementedOperation"})


@pytest.mark.parametrize("input_, expected", [
    ("http://localhost:5000/queues/test_queue", "test_queue"),
    ("http://localhost:5000/test_queue_1", "test_queue_1"),
    ("http:///test_queue", "test_queue"),
    ("https:///test_queue_1", "test_queue_1"),
    ("https://ap-northeast-1.queue.amaz/test-queue", "test-queue")
])
def test_queue_name_from_queue_url(input_, expected):
    actual = server.queue_name_from_queue_url(input_)
    assert actual == expected


def test_queue_name_from_queue_url_invalid_url():
    invalid_url = "hoge://hugahuga/queue"
    with pytest.raises(ValueError):
        server.queue_name_from_queue_url(invalid_url)


def test_parse_message_attribute():
    raw_message_attribute = {
        'MessageAttribute.1.Name': 'City', 'MessageAttribute.1.Value.DataType': 'String','MessageAttribute.1.Value.StringValue': 'Any+City',
        'MessageAttribute.2.Name': 'Greeting', 'MessageAttribute.2.Value.DataType': 'Binary', 'MessageAttribute.2.Value.BinaryValue': 'SGVsbG8sIFdvcmxkIQ%3D%3D',
        'MessageAttribute.3.Name': 'Population', 'MessageAttribute.3.Value.DataType': 'Number', 'MessageAttribute.3.Value.StringValue': '1250800',
    }
    print(raw_message_attribute.items())

    actual = server.parse_message_attribute(raw_message_attribute)
    expected = {
        "City": {"StringValue": "Any+City", "DataType": "String"},
        "Greeting": {"BinaryValue": "SGVsbG8sIFdvcmxkIQ%3D%3D", "DataType": "Binary"},
        "Population": {"StringValue": "1250800", "DataType": "Number"},
    }
    assert actual == expected
