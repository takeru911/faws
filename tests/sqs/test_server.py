import datetime
import pytest
from unittest import mock
from typing import Dict, List
from dict2xml import dict2xml
from faws.sqs import server
from faws.sqs.queue_storage import QueuesStorageType


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


def purge_queue(client, queue_url):
    return client.post("/", data=f"Action=PurgeQueue&QueueUrl={queue_url}")


def tag_queue(client, queue_url, tags: Dict):
    data = f"Action=TagQueue&QueueUrl={queue_url}"
    if tags is None:
        return client.post("/", data=data)
    tag_data = "&".join([f"{k}={v}" for k, v in tags.items()])
    return client.post("/", data=data + "&" + tag_data)


def list_queue_tags(client, queue_url):
    return client.post("/", data=f"Action=ListQueueTags&QueueUrl={queue_url}")


def untag_queue(client, queue_url: str, tag_names: List[str]):
    data = f"Action=UntagQueue&QueueUrl={queue_url}"
    untag_data = "&".join([f"TagKey.{i}={v}" for i, v in enumerate(tag_names, 1)])
    return client.post("/", data=data + "&" + untag_data)


def send_message(client, queue_url, message, message_attributes=None, delay_seconds=0):
    data = f"Action=SendMessage&QueueUrl={queue_url}&MessageBody={message}&DelaySeconds={delay_seconds}"
    if message_attributes is None:
        return client.post("/", data=data)
    message_attributes_data = "&".join(
        [f"{k}={v}" for k, v in message_attributes.items()]
    )
    return client.post("/", data=data + "&" + message_attributes_data)


def receive_message(
    client, queue_url, message_attribute_names=None, num_of_message=None
):
    data = f"Action=ReceiveMessage&QueueUrl={queue_url}"
    if message_attribute_names is not None:
        message_attribute_response_data = "&".join(
            [f"{k}={v}" for k, v in message_attribute_names.items()]
        )
        data = data + "&" + message_attribute_response_data
    if num_of_message is not None:
        data = data + "&" + f"MaxNumberOfMessages={num_of_message}"
    return client.post("/", data=data)


def test_parse_request_data():
    actual = server.parse_request_data("Action=ListQueue&version=2015-02-01")
    expected = {"Action": "ListQueue", "version": "2015-02-01"}

    assert actual == expected


def test_do_list_queues(client):

    create_queue(client, "test_queue_1")
    create_queue(client, "test_queue_2")
    with mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"):
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


def test_do_list_queues_non_exists(client):
    with mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"):
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


def test_do_create_queue(client):
    with mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"):
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


def test_do_get_queue_url(client):
    create_queue(client, "test_queue_1")
    with mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"):
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


def test_do_get_queue_url_non_exist(client):
    with mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"):
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


def test_do_delete_queue(client):
    create_queue(client, "test_queue")
    with mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"):
        assert delete_queue(
            client, "http://localhost:5000/queues/test_queue"
        ).data == dict2xml_bytes(
            {
                "DeleteQueueResponse": {
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


def test_do_purge_queue(client):
    create_queue(client, "test_queue")
    with mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"):
        assert purge_queue(
            client, "http://localhost:5000/queues/test_queue"
        ).data == dict2xml_bytes(
            {
                "PurgeQueueResponse": {
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


def test_do_tag_queue(client):
    create_queue(client, "test_queue_1")
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"), mock.patch(
        "uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"
    ):
        assert tag_queue(
            client,
            queue_url="http://localhost/quueus/test_queue_1",
            tags={
                "Tag.1.Key": "tag_name",
                "Tag.1.Value": "tag_value",
                "Tag.2.Key": "tag_name_2",
                "Tag.2.Value": "tag_value_2",
            },
        ).data == dict2xml_bytes(
            {
                "TagQueueResponse": {
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


@pytest.mark.parametrize(
    "request_tags,response_tags",
    [
        (
            {
                "Tag.1.Key": "tag_name",
                "Tag.1.Value": "tag_value",
                "Tag.2.Key": "tag_name_2",
                "Tag.2.Value": "tag_value_2",
            },
            [
                {"Key": "tag_name", "Value": "tag_value"},
                {"Key": "tag_name_2", "Value": "tag_value_2"},
            ],
        ),
        (None, {}),
    ],
)
def test_do_list_queue(client, request_tags, response_tags):
    create_queue(client, "test_queue_1")
    queue_url = "http://localhost/quueus/test_queue_1"
    tag_queue(
        client, queue_url, tags=request_tags,
    )
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"), mock.patch(
        "uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"
    ):
        expected_response = {
            "ListQueueTagsResponse": {
                "ListQueueTagsResult": {"Tag": response_tags}
                if response_tags != {}
                else {},
                "ResponseMetadata": {
                    "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                },
            }
        }
        assert list_queue_tags(client, queue_url).data == dict2xml_bytes(
            expected_response
        )


@pytest.mark.parametrize(
    "untags, exist_tags",
    [
        (["tag_name"], [{"Key": "tag_name_2", "Value": "tag_value_2"},]),
        (["tag_name", "tag_name_2"], {}),
        (
            ["tag_name_3"],
            [
                {"Key": "tag_name", "Value": "tag_value"},
                {"Key": "tag_name_2", "Value": "tag_value_2"},
            ],
        ),
    ],
)
def test_do_untag_queue(client, untags, exist_tags):
    create_queue(client, "test_queue_1")
    queue_url = "http://localhost/quueus/test_queue_1"
    tag_queue(
        client,
        queue_url,
        tags={
            "Tag.1.Key": "tag_name",
            "Tag.1.Value": "tag_value",
            "Tag.2.Key": "tag_name_2",
            "Tag.2.Value": "tag_value_2",
        },
    )
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"), mock.patch(
        "uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"
    ):
        assert untag_queue(client, queue_url, untags).data == dict2xml_bytes(
            {
                "UntagQueueResponse": {
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )
        expected_response = {
            "ListQueueTagsResponse": {
                "ListQueueTagsResult": {"Tag": exist_tags} if exist_tags != {} else {},
                "ResponseMetadata": {
                    "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                },
            }
        }

        assert list_queue_tags(client, queue_url).data == dict2xml_bytes(
            expected_response
        )


def test_do_send_message(client):
    create_queue(client, "test_queue_1")
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"), mock.patch(
        "uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"
    ):
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


def test_send_message_with_attribute(client):
    create_queue(client, "test_queue_1")
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"), mock.patch(
        "uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"
    ):
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


@pytest.mark.parametrize("num_of_message", [2, 10])
def test_do_receive_message(client, num_of_message):
    create_queue(client, "test_receive_queue")
    with mock.patch(
        "uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"
    ), mock.patch("faws.sqs.message.generate_uuid") as generate_uuid:
        queue_url = "http://localhost:5000/queues/test_receive_queue"
        for i in range(num_of_message):
            generate_uuid.return_value = str(i)
            send_message(client, queue_url, "test")
        assert receive_message(
            client,
            "http://localhost:5000/queues/test_receive_queue",
            num_of_message=num_of_message,
        ).data == dict2xml_bytes(
            {
                "ReceiveMessageResponse": {
                    "ReceiveMessageResult": {
                        "Message": [
                            {
                                "MessageId": f"{i}",
                                "ReceiptHandle": "barbar",
                                "MD5OFBody": "hogehoge",
                                "Body": "test",
                            }
                            for i in range(num_of_message)
                        ]
                    },
                    "ResponseMetadata": {
                        "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                    },
                }
            }
        )


def test_do_receive_message_non_message(client):
    create_queue(client, "test_receive_queue")
    with mock.patch("uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"):
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


def test_receive_message_non_attribute(client):
    queue_name = "test_receive_queue"
    create_queue(client, queue_name)
    queue_url = f"http://localhost/queues/{queue_name}"
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"), mock.patch(
        "uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"
    ):
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


@pytest.mark.parametrize(
    "attribute_names_pattern",
    [
        {"MessageAttribute.1.Name": "v1", "MessageAttribute.2.Name": "v2"},
        {"MessageAttribute.1.Name": "All"},
    ],
)
def test_receive_message_with_attribute(client, attribute_names_pattern):
    queue_name = "test_receive_queue_attr"
    queue_url = f"http://localhost/queues/{queue_name}"
    create_queue(client, queue_name)
    with mock.patch("faws.sqs.message.generate_uuid", return_value="1111"), mock.patch(
        "uuid.uuid4", return_value="725275ae-0b9b-4762-b238-436d7c65a1ac"
    ):
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
