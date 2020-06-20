from flask import Flask, request, Response, g, current_app
from typing import Dict, List
from faws.sqs.message import MessageAttribute
from faws.sqs.queues import Queues, QueuesStorageType
from dict2xml import dict2xml
import dataclasses
import re
import urllib
import uuid


def init_queues():
    queues = get_queues()
    queues.init_queues()


def get_queues():
    if "queues" not in g:
        g.queues = Queues(
            current_app.config["QueuesStorageType"],
            **current_app.config.get("QueuesStorageTypeConfig", {}),
        )
        return g.queues
    return g.queues


def parse_request_data(request_data: str):
    parsed_data = {}
    for param in request_data.split("&"):
        key, value = param.split("=")
        parsed_data[key] = urllib.parse.unquote(value)

    return parsed_data


@dataclasses.dataclass()
class Result:
    operation_name: str
    result_data: Dict


def create_queue(QueueName: str, **kwargs) -> Dict:
    queue = get_queues().create_queue(QueueName)
    return {"QueueUrl": queue.queue_url}


def get_list_queues(**kwargs) -> Dict:
    queue_urls = [queue.queue_url for queue in get_queues().get_queues()]

    return {"QueueUrl": queue_urls}


def get_queue_url(QueueName: str, **kwargs) -> Dict:
    queue = get_queues().get_queue(QueueName)
    return {"QueueUrl": queue.queue_url}


def send_message(QueueUrl: str, MessageBody: str,  DelaySeconds: str = 0, **kwargs) -> Dict:
    queue_name = queue_name_from_queue_url(QueueUrl)
    queue = get_queues().get_queue(queue_name)
    # message_attributeは
    # MessageAttribute.1.Name': 'City', 'MessageAttribute.1.Value.DataType': 'String'
    # のようなフォーマットで来るのでMessageAttributeを持つkwargsのsetを取得する
    message_attributes = {k: v for k, v in kwargs.items() if "MessageAttribute" in k}
    message = queue.add_message(
        MessageBody,
        message_attributes=message_attributes,
        delay_seconds=int(DelaySeconds),
    )

    return {
        "MD5OfMessageBody": "hogehoge",
        "MD5OfMessageAttributes": "hugahuga",
        "MessageId": message.message_id,
    }


def receive_message(QueueUrl: str, **kwargs) -> Dict:
    queue_name = queue_name_from_queue_url(QueueUrl)
    message_attribute_names = {
        k: v for k, v in kwargs.items() if "MessageAttribute" in k
    }
    queue = get_queues().get_queue(queue_name)

    message = queue.get_message()
    if message is None:
        return {}
    result_data = {
        "Message": {
            "MessageId": message.message_id,
            "ReceiptHandle": "barbar",
            "MD5OFBody": "hogehoge",
            "Body": message.message_body,
        }
    }
    if len(message_attribute_names) == 0:
        message.update_deliverable_time(queue.default_visibility_timeout)
        return result_data
    message_attributes = select_message_attribute(
        message.message_attributes, list(message_attribute_names.values())
    )
    result_data["Message"]["MessageAttribute"] = [
        {"Name": k, "Value": v.to_dict()} for k, v in message_attributes.items()
    ]
    # メッセージの配信可能時間を現時刻 + Queueのデフォルト可視性タイムアウトで更新する
    message.update_deliverable_time(queue.default_visibility_timeout)
    return result_data


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


def do_operation(request_data: Dict) -> Result:
    action = request_data["Action"]
    if action == "ListQueues":
        return Result(action, get_list_queues(**request_data))
    if action == "GetQueueUrl":
        return Result(action, get_queue_url(**request_data))
    if action == "CreateQueue":
        return Result(action, create_queue(**request_data))
    if action == "SendMessage":
        return Result(action, send_message(**request_data))
    if action == "ReceiveMessage":
        return Result(action, receive_message(**request_data))
    raise NotImplementedError()


def run_request_to_index(request):
    request_id = uuid.uuid4()
    request_data = parse_request_data(request.get_data().decode(encoding="utf-8"))

    result = do_operation(request_data)
    response_data = dict2xml(
        {
            f"{result.operation_name}Response": {
                f"{result.operation_name}Result": result.result_data,
                "ResponseMetadata": {"RequestId": request_id},
            }
        }
    )
    return response_data


def create_app(app_config: Dict = None):
    app = Flask(__name__, instance_relative_config=True)
    app.config["QueuesStorageType"] = QueuesStorageType.IN_MEMORY
    if app_config is not None:
        app.config.update(app_config)

    @app.route("/", methods=["POST"])
    def index():
        response_data = run_request_to_index(request)
        return Response(response_data, mimetype="text/xml")

    return app
