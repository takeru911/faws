from flask import Flask, request, Response
from typing import Dict, Optional
from faws.sqs.queues import Queues
from faws.sqs.queues_storage import QueuesStorageType
import dicttoxml
import re
import urllib

app = Flask(__name__)
queues = Queues(QueuesStorageType.IN_MEMORY)


def parse_request_data(request_data: str):
    parsed_data = {}
    for param in request_data.split("&"):
        key, value = param.split("=")
        parsed_data[key] = urllib.parse.unquote(value)

    return parsed_data


def create_queue(QueueName: str, **kwargs):
    queue = queues.create_queue(QueueName)
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
    queue_urls = [queue.queue_url for queue in queues.get_queues()]
    return {
        "ListQueuesResponse": {
            "ListQueuesResult": queue_urls,
            "ResponseMetadata": {
                "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
            }
        }
    }


def get_queue_url(QueueName: str, **kwargs):
    queue = queues.get_queue(QueueName)
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
    # message_attributeは
    # MessageAttribute.1.Name': 'City', 'MessageAttribute.1.Value.DataType': 'String'
    # のようなフォーマットで来るのでMessageAttributeを持つkwargsのsetを取得する
    message_attributes = parse_message_attribute({k: v for k, v in kwargs.items() if "MessageAttribute" in k})
    queue = queues.get_queue(queue_name)
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


def parse_message_attribute(raw_message_attributes: Dict) -> Optional[Dict]:
    if len(raw_message_attributes) == 0:
        return None

    num_attributes = int(len(raw_message_attributes) / 3)
    message_attributes = {}
    for i in range(1, num_attributes + 1):
        attribute_name = raw_message_attributes[f"MessageAttribute.{i}.Name"]
        data_type = raw_message_attributes[f"MessageAttribute.{i}.Value.DataType"]
        value_info = {
            re.match(fr"MessageAttribute\.{i}\.Value\.(.*Value)", key).groups()[0]: value
            for key, value in raw_message_attributes.items()
            if re.match(fr"MessageAttribute\.{i}\.Value\.(.*Value)", key)
        }
        value_key = list(value_info.keys())[0]
        message_attributes[attribute_name] = {
            "DataType": data_type,
            value_key: value_info[value_key]
        }

    return message_attributes


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
    raise NotImplementedError()


@app.route('/', methods=["POST"])
def index():
    request_data = parse_request_data(
        request.get_data().decode(encoding="utf-8")
    )

    result = do_operation(request_data)
    response_data = dicttoxml.dicttoxml(result, root=False, item_func=lambda x: "QueueUrl")
    return Response(
        response_data,
        mimetype="text/xml"
    )


if __name__ == "__main__":
    app.run(debug=True)
