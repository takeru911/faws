import datetime
import pytest
from unittest import mock
from dict2xml import dict2xml
from faws.sqs import server
from faws.sqs.queue_storage import QueuesStorageType
from faws.sqs.server import Result, ErrorResult
from faws.sqs.error import NonExistentQueue


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


def delete_queue(client, queue_url):
    return client.post("/", data=f"Action=DeleteQueue&QueueUrl={queue_url}")


def send_message(client, queue_url, message, message_attributes=None, delay_seconds=0):
    data = f"Action=SendMessage&QueueUrl={queue_url}&MessageBody={message}&DelaySeconds={delay_seconds}"
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
    message_attribute_response_data = "&".join(
        [f"{k}={v}" for k, v in message_attribute_names.items()]
    )
    return client.post("/", data=data + "&" + message_attribute_response_data)


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
def test_do_list_queues_non_exists(uuid, client):
    assert list_queues(client).data == dict2xml_bytes(
        {
            "ListQueuesResponse": {
                "ListQueuesResult": {},
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
def test_do_get_queue_url_non_exist(uuid, client):
    create_queue(client, "test_queue_1")
    response = get_queue_url(client, "test_queue")
    assert (
        response.data
        == dict2xml_bytes(
            {
                "ErrorResponse": {
                    "Error": {
                        "Type": "Sender",
                        "Code": "AWS.SimpleQueueService.NonExistentQueue",
                        "Message": "The specified queue does not exist for this wsdl version.",
                        "Detail": {},
                    },
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )
        and response.status_code == 400
    )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_do_delete_queue(uuid, client):
    create_queue(client, "test_queue")
    assert delete_queue(
        client, "http://localhost:5000/queues/test_queue"
    ).data == dict2xml_bytes(
        {
            "DeleteQueueResponse": {
                "DeleteQueueResult": {},
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


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_send_set_delay_message(uuid, client):
    now = datetime.datetime(2020, 5, 1, 0, 0, 0)
    deliverable_time = datetime.datetime(2020, 5, 1, 0, 0, 40)
    queue_name = "test_send_set_delay_message"
    queue_url = f"http://localhost/queues/{queue_name}"
    create_queue(client, queue_name)
    with mock.patch("datetime.datetime") as dt:
        dt.now.return_value = now
        send_message(client, queue_url=queue_url, message="test", delay_seconds=30)
        response = receive_message(client, queue_url=queue_url)
        # 配信遅延させたので結果はない
        assert response.data == dict2xml_bytes(
            {
                "ReceiveMessageResponse": {
                    "ReceiveMessageResult": {},
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )
        # 配信可能な時間に変更し、結果が返るかを確認
        dt.now.return_value = deliverable_time
        response = receive_message(client, queue_url=queue_url)
        assert response.data == dict2xml_bytes(
            {
                "ReceiveMessageResponse": {
                    "ReceiveMessageResult": {
                        "Message": {
                            "MessageId": "725275ae-0b9b-4762-b238-436d7c65a1ac",
                            "ReceiptHandle": "barbar",
                            "MD5OFBody": "hogehoge",
                            "Body": "test",
                        },
                    },
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


@mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac")
def test_visibility_after_receiving(uuid, client):
    now = datetime.datetime(2020, 5, 1, 0, 0, 0)
    deliverable_time = datetime.datetime(2020, 5, 1, 0, 0, 40)
    queue_name = "test_send_set_delay_message"
    queue_url = f"http://localhost/queues/{queue_name}"
    create_queue(client, queue_name)
    with mock.patch("datetime.datetime") as dt:
        dt.now.return_value = now
        send_message(client, queue_url=queue_url, message="test")
        receive_message(client, queue_url=queue_url)
        # 2回目の受信は不可視状態なので受信できない
        response = receive_message(client, queue_url=queue_url)
        # 配信遅延させたので結果はない
        assert response.data == dict2xml_bytes(
            {
                "ReceiveMessageResponse": {
                    "ReceiveMessageResult": {},
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )
        # 配信可能な時間に変更し、結果が返るかを確認
        dt.now.return_value = deliverable_time
        response = receive_message(client, queue_url=queue_url)
        assert response.data == dict2xml_bytes(
            {
                "ReceiveMessageResponse": {
                    "ReceiveMessageResult": {
                        "Message": {
                            "MessageId": "725275ae-0b9b-4762-b238-436d7c65a1ac",
                            "ReceiptHandle": "barbar",
                            "MD5OFBody": "hogehoge",
                            "Body": "test",
                        },
                    },
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


def test_determine_operation_raises_when_non_exist_operation(client):
    with pytest.raises(NotImplementedError):
        client.post("/", data="Action=NotImplementedAction")


def test_result():
    result = Result(
        operation_name="test", result_data={"test_result": "hoge"}, request_id="111"
    )

    assert result.generate_response() == dict2xml(
        {
            "testResponse": {
                "testResult": {"test_result": "hoge"},
                "ResponseMetadata": {"RequestId": "111"},
            }
        }
    )


def test_error_result():
    result = ErrorResult(NonExistentQueue(), request_id="111")

    assert result.generate_response() == dict2xml(
        {
            "ErrorResponse": {
                "Error": {
                    "Type": "Sender",
                    "Code": "AWS.SimpleQueueService.NonExistentQueue",
                    "Message": "The specified queue does not exist for this wsdl version.",
                    "Detail": {},
                },
                "ResponseMetadata": {"RequestId": "111"},
            }
        }
    )
