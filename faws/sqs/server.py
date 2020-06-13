from flask import Flask, request, Response, g, current_app
from typing import Dict, List
from faws.sqs.message import MessageAttribute
from faws.sqs.queues import Queues, QueuesStorageType
from dict2xml import dict2xml
import re
import urllib


def init_queues():
    queues = get_queues()
    queues.init_queues()


def get_queues():
    if "queues" not in g:
        g.queues = Queues(
            current_app.config["QueuesStorageType"],
            **current_app.config.get("QueuesStorageTypeConfig", {})
        )
        return g.queues
    return g.queues


def parse_request_data(request_data: str):
    parsed_data = {}
    for param in request_data.split("&"):
        key, value = param.split("=")
        parsed_data[key] = urllib.parse.unquote(value)

    return parsed_data


def create_queue(QueueName: str, **kwargs):
    queue = get_queues().create_queue(QueueName)
    return {
        "CreateQueueResponse": {
            "CreateQueueResult": {
                "QueueUrl": queue.queue_url
            },
            "ResponseMetadata": {
                "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
            }
        }
    }


def get_list_queues(**kwargs):
    queue_urls = [queue.queue_url for queue in get_queues().get_queues()]

    return {
        "ListQueuesResponse": {
            "ListQueuesResult": {
                "QueueUrl": queue_urls
            },
            "ResponseMetadata": {
                "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
            }
        }
    }


def get_queue_url(QueueName: str, **kwargs):
    queue = get_queues().get_queue(QueueName)
    return {
        "GetQueueUrlResponse": {
            "GetQueueUrlResult": {
                "QueueUrl": queue.queue_url
            },
            "ResponseMetadata": {
                "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
            }
        }
    }


def send_message(QueueUrl: str, MessageBody: str, **kwargs):
    queue_name = queue_name_from_queue_url(QueueUrl)
    queue = get_queues().get_queue(queue_name)
    # message_attributeは
    # MessageAttribute.1.Name': 'City', 'MessageAttribute.1.Value.DataType': 'String'
    # のようなフォーマットで来るのでMessageAttributeを持つkwargsのsetを取得する
    message_attributes = {k: v for k, v in kwargs.items() if "MessageAttribute" in k}
    message = queue.add_message(
        MessageBody,
        message_attributes=message_attributes,
        delay_seconds=int(kwargs.get("DelaySeconds", "0")),
    )

    return {
        "SendMessageResponse": {
            "SendMessageResult": {
                "MD5OfMessageBody": "hogehoge",
                "MD5OfMessageAttributes": "hugahuga",
                "MessageId": message.message_id
            },
            "ResponseMetadata": {
                "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
            }
        }
    }


def receive_message(QueueUrl: str, **kwargs):
    queue_name = queue_name_from_queue_url(QueueUrl)
    message_attribute_names = {k: v for k, v in kwargs.items() if "MessageAttribute" in k}
    queue = get_queues().get_queue(queue_name)
    response_data = {
        "ReceiveMessageResponse": {
            "ResponseMetadata": {
                "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
            }
        }
    }
    message = queue.get_message()
    if message is None:
        return response_data

    response_data["ReceiveMessageResponse"]["ReceiveMessageResult"] = {
        "Message": {
            "MessageId": message.message_id,
            "ReceiptHandle": "barbar",
            "MD5OFBody": "hogehoge",
            "Body": message.message_body,
        }
    }
    if len(message_attribute_names) == 0:
        return response_data
    message_attributes = select_message_attribute(message.message_attributes, list(message_attribute_names.values()))
    response_data["ReceiveMessageResponse"]["ReceiveMessageResult"]["Message"]["MessageAttribute"] = [
        {"Name": k, "Value": v.to_dict()} for k, v in message_attributes.items()
    ]
    return response_data


def select_message_attribute(
        message_attributes: Dict[str, MessageAttribute], message_attribute_names: List[str]
) -> Dict[str, MessageAttribute]:
    if "All" in message_attribute_names:
        return message_attributes
    return {
        attribute_name: attribute
        for attribute_name, attribute in message_attributes.items()
        if attribute_name in message_attribute_names
    }


def queue_name_from_queue_url(queue_url: str) -> str:
    if "http" not in queue_url and "https" not in queue_url:
        raise ValueError(f"The address {queue_url} is not valid for this endpoint.")
    m = re.match(r"https*:\/\/.*\/(.*)", queue_url)

    if len(m.groups()) != 1:
        raise ValueError(f"The address {queue_url} is not valid for this endpoint.")
    return m.groups()[0]


def do_operation(request_data: Dict):
    action = request_data["Action"]
    if action == "ListQueues":
        return get_list_queues(**request_data)
    if action == "GetQueueUrl":
        return get_queue_url(**request_data)
    if action == "CreateQueue":
        return create_queue(**request_data)
    if action == "SendMessage":
        return send_message(**request_data)
    if action == "ReceiveMessage":
        return receive_message(**request_data)
    raise NotImplementedError()


def create_app(app_config: Dict = None):
    app = Flask(__name__, instance_relative_config=True)
    app.config["QueuesStorageType"] = QueuesStorageType.IN_MEMORY
    if app_config is not None:
        app.config.update(app_config)

    @app.route('/', methods=["POST"])
    def index():
        request_data = parse_request_data(
            request.get_data().decode(encoding="utf-8")
        )

        result = do_operation(request_data)
        response_data = dict2xml(result)
        return Response(
            response_data,
            mimetype="text/xml"
        )
    return app
