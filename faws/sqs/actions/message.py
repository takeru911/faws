import re
from faws.sqs.queues import Queues
from faws.sqs.message import MessageAttribute
from typing import Dict, List


def send_message(
    queues: Queues, QueueUrl: str, MessageBody: str, DelaySeconds: str = 0, **kwargs
) -> Dict:
    queue_name = queue_name_from_queue_url(QueueUrl)
    queue = queues.get_queue(queue_name)
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


def receive_message(
    queues: Queues, QueueUrl: str, VisibilityTimeout: str = None, **kwargs
) -> Dict:
    queue_name = queue_name_from_queue_url(QueueUrl)
    message_attribute_names = {
        k: v for k, v in kwargs.items() if "MessageAttribute" in k
    }
    queue = queues.get_queue(queue_name)

    message = queue.get_message(
        int(VisibilityTimeout) if VisibilityTimeout is not None else None
    )
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
        return result_data

    message_attributes = select_message_attribute(
        message.message_attributes, list(message_attribute_names.values())
    )
    result_data["Message"]["MessageAttribute"] = [
        {"Name": k, "Value": v.to_dict()} for k, v in message_attributes.items()
    ]

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
