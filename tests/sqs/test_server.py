import pytest
from dict2xml import dict2xml
from unittest import mock
from faws.sqs import server
from faws.sqs.queues import QueuesStorageType


@pytest.fixture
def client():
    app_config = {"QueuesStorageType": QueuesStorageType.IN_MEMORY, "TESTING": True}
    app = server.create_app(app_config)
    with app.test_client() as client:
        with app.app_context():
            server.init_queues()
        yield client


def dict2xml_bytes(d):
    return bytes(dict2xml(d), encoding="utf-8")


def create_queue(client, queue_name):
    return client.post("/", data=f"Action=CreateQueue&QueueName={queue_name}")


def list_queues(client):
    return client.post("/", data=f"Action=ListQueues")


def get_queue_url(client, queue_name):
    return client.post("/", data=f"Action=GetQueueUrl&QueueName={queue_name}")


def send_message(client, queue_url, message, message_attributes=None):
    data = f"Action=SendMessage&QueueUrl={queue_url}&MessageBody={message}"
    if message_attributes is None:
        return client.post("/", data=data)
    message_attributes_data = "&".join(
        [f"{k}={v}" for k, v in message_attributes.items()]
    )
    return client.post("/", data=data + "&" + message_attributes_data)


def receive_message(client, queue_url, message_attribute_names=None):
    data = f"Action=ReceiveMessage&QueueUrl={queue_url}"
    if message_attribute_names is None:
        return client.post("/", data=data)
    message_attribute_names_data = "&".join(
        [f"{k}={v}" for k, v in message_attribute_names.items()]
    )
    return client.post("/", data=data + "&" + message_attribute_names_data)


def test_parse_request_data():
    actual = server.parse_request_data("Action=ListQueue&version=2015-02-01")
    expected = {"Action": "ListQueue", "version": "2015-02-01"}

    assert actual == expected


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_do_list_queues(uuid, client):
    create_queue(client, "test_queue_1")
    create_queue(client, "test_queue_2")
    assert list_queues(client).data == dict2xml_bytes(
        {
            "ListQueuesResponse": {
                "ListQueuesResult": {
                    "QueueUrl": [
                        "https://localhost:5000/queues/test_queue_1",
                        "https://localhost:5000/queues/test_queue_2",
                    ]
                },
                "ResponseMetadata": {
                    "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                },
            }
        }
    )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_do_create_queue(uuid, client):
    response = create_queue(client, "test-queue")

    assert response.data == dict2xml_bytes(
        {
            "CreateQueueResponse": {
                "CreateQueueResult": {
                    "QueueUrl": f"https://localhost:5000/queues/test-queue"
                },
                "ResponseMetadata": {
                    "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                },
            }
        }
    )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_do_get_queue_url(uuid, client):
    create_queue(client, "test_queue_1")

    assert get_queue_url(client, "test_queue_1").data == dict2xml_bytes(
        {
            "GetQueueUrlResponse": {
                "GetQueueUrlResult": {
                    "QueueUrl": f"https://localhost:5000/queues/test_queue_1"
                },
                "ResponseMetadata": {
                    "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                },
            }
        }
    )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_do_send_message(uuid, client):
    create_queue(client, "test_queue_1")
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"):
        assert send_message(
            client, queue_url="http://localhost/quueus/test_queue_1", message="taker"
        ).data == dict2xml_bytes(
            {
                "SendMessageResponse": {
                    "SendMessageResult": {
                        "MD5OfMessageBody": "hogehoge",
                        "MD5OfMessageAttributes": "hugahuga",
                        "MessageId": "1111",
                    },
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_send_message_with_attribute(uuid, client):
    create_queue(client, "test_queue_1")
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"):
        assert send_message(
            client,
            "http://localhost:5000/queues/test_queue_1",
            message="taker",
            message_attributes={
                "MessageAttribute.1.Name": "v1",
                "MessageAttribute.1.Value.DataType": "String",
                "MessageAttribute.1.Value.StringValue": "hoge",
            },
        ).data == dict2xml_bytes(
            {
                "SendMessageResponse": {
                    "SendMessageResult": {
                        "MD5OfMessageBody": "hogehoge",
                        "MD5OfMessageAttributes": "hugahuga",
                        "MessageId": "1111",
                    },
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_do_receive_message_non_message(uuid, client):
    create_queue(client, "test_receive_queue")
    assert receive_message(
        client, "http://localhost:5000/queues/test_receive_queue"
    ).data == dict2xml_bytes(
        {
            "ReceiveMessageResponse": {
                "ReceiveMessageResult": {},
                "ResponseMetadata": {
                    "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                },
            }
        }
    )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_receive_message_non_attribute(uuid, client):
    queue_name = "test_receive_queue"
    create_queue(client, queue_name)
    queue_url = f"http://localhost/queues/{queue_name}"
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"):
        send_message(client, queue_url, "hogehoge")
        assert receive_message(client, queue_url).data == dict2xml_bytes(
            {
                "ReceiveMessageResponse": {
                    "ReceiveMessageResult": {
                        "Message": {
                            "MessageId": "1111",
                            "ReceiptHandle": "barbar",
                            "MD5OFBody": "hogehoge",
                            "Body": "hogehoge",
                        }
                    },
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
@pytest.mark.parametrize(
    "attribute_names_pattern",
    [
        {"MessageAttribute.1.Name": "v1", "MessageAttribute.2.Name": "v2"},
        {"MessageAttribute.1.Name": "All",},
    ],
)
def test_receive_message_with_attribute(uuid, client, attribute_names_pattern):
    queue_name = "test_receive_queue_attr"
    queue_url = f"http://localhost/queues/{queue_name}"
    create_queue(client, queue_name)
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"):
        send_message(
            client,
            queue_url,
            message="hogehoge",
            message_attributes={
                "MessageAttribute.1.Name": "v1",
                "MessageAttribute.1.Value.DataType": "String",
                "MessageAttribute.1.Value.StringValue": "hoge",
                "MessageAttribute.2.Name": "v2",
                "MessageAttribute.2.Value.DataType": "Number",
                "MessageAttribute.2.Value.StringValue": "123",
            },
        )
        assert receive_message(
            client, queue_url, message_attribute_names=attribute_names_pattern
        ).data == dict2xml_bytes(
            {
                "ReceiveMessageResponse": {
                    "ReceiveMessageResult": {
                        "Message": {
                            "MessageId": "1111",
                            "ReceiptHandle": "barbar",
                            "MD5OFBody": "hogehoge",
                            "Body": "hogehoge",
                            "MessageAttribute": [
                                {
                                    "Name": "v1",
                                    "Value": {
                                        "DataType": "String",
                                        "StringValue": "hoge",
                                    },
                                },
                                {
                                    "Name": "v2",
                                    "Value": {
                                        "DataType": "Number",
                                        "StringValue": "123",
                                    },
                                },
                            ],
                        }
                    },
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


def test_determine_operation_raises_when_non_exist_operation():
    with pytest.raises(NotImplementedError):
        server.do_operation({"Action": "NonImplementedOperation"})


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
    actual = server.queue_name_from_queue_url(input_)
    assert actual == expected


def test_queue_name_from_queue_url_invalid_url():
    invalid_url = "hoge://hugahuga/queue"
    with pytest.raises(ValueError):
        server.queue_name_from_queue_url(invalid_url)
