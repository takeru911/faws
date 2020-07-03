from typing import Dict, List
from faws.sqs.message import MessageAttribute
from faws.sqs.queue import name_from_url
from faws.sqs.queue_storage import QueueStorage


def send_message(
    queues: QueueStorage,
    QueueUrl: str,
    MessageBody: str,
    DelaySeconds: str = 0,
    **kwargs
) -> Dict:
    queue_name = name_from_url(QueueUrl)
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
    queues: QueueStorage, QueueUrl: str, VisibilityTimeout: str = None, MaxNumberOfMessages: str = None,**kwargs
) -> Dict:
    queue_name = name_from_url(QueueUrl)
    queue = queues.get_queue(queue_name)
    message_attribute_names = {
        k: v for k, v in kwargs.items() if "MessageAttribute" in k
    }
    messages = queue.get_message(
        visibility_timeout=int(VisibilityTimeout) if VisibilityTimeout is not None else None,
        max_number_of_messages=int(MaxNumberOfMessages) if MaxNumberOfMessages is not None else 1
    )
    if not messages:
        return {}

    message_data_list = []
    for message in messages:
        message_data = {
            "MessageId": message.message_id,
            "ReceiptHandle": "barbar",
            "MD5OFBody": "hogehoge",
            "Body": message.message_body,
        }

        if len(message_attribute_names) == 0:
            message_data_list.append(message_data)
            continue
        message_data["MessageAttribute"] = _select_message_attribute(
            message.message_attributes, list(message_attribute_names.values())
        )
        message_data_list.append(message_data)

    return {
        "Message": message_data_list
    }


def _select_message_attribute(
    message_attributes: Dict[str, MessageAttribute], message_attribute_names: List[str]
) -> List[Dict]:
    if "All" in message_attribute_names:
        return [
            {"Name": attribute_name, "Value": attribute.to_dict()}
            for attribute_name, attribute in message_attributes.items()
        ]

    return [
        {"Name": attribute_name, "Value": attribute.to_dict()}
        for attribute_name, attribute in message_attributes.items()
        if attribute_name in message_attribute_names
    ]
