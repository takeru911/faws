from typing import Dict, List, Optional
from faws.sqs.queue import name_from_url, Tag
from faws.sqs.queue_storage import QueueStorage


def create_queue(queues: QueueStorage, QueueName: str, **kwargs) -> Dict:
    queue = queues.create_queue(QueueName)
    return {"QueueUrl": queue.queue_url}


def get_list_queues(queues: QueueStorage, **kwargs) -> Dict:
    queue_urls = [queue.queue_url for queue in queues.get_queues()]
    if len(queue_urls) == 0:
        return {}
    return {"QueueUrl": queue_urls}


def get_queue_url(queues: QueueStorage, QueueName: str, **kwargs) -> Dict:
    queue = queues.get_queue(QueueName)
    return {"QueueUrl": queue.queue_url}


def delete_queue(queues: QueueStorage, QueueUrl: str, **kwargs):
    queue_name = name_from_url(queue_url=QueueUrl)
    queues.delete_queue(queue_name)


def purge_queue(queues: QueueStorage, QueueUrl: str, **kwargs):
    queue_name = name_from_url(queue_url=QueueUrl)
    queue = queues.get_queue(queue_name)
    queue.purge_message()


def tag_queue(queues: QueueStorage, QueueUrl: str, **kwargs):
    queue_name = name_from_url(queue_url=QueueUrl)
    queue = queues.get_queue(queue_name)
    # tagのrequest dataはTag.1.Key=key_name, Tag.1.Value=value
    # そのため、まずはkwargsにあるTag.*をkvのdictにする
    # {"Tag.1.Key": "key_name", "Tag.1.Value": "value"...}
    tags = {k: v for k, v in kwargs.items() if "Tag" in k}
    parsed_tags = _parse_tag_request_data(tags)
    for tag in parsed_tags:
        queue.set_tag(tag)


def list_queue_tags(queues: QueueStorage, QueueUrl: str, **kwargs) -> Dict:
    queue_name = name_from_url(queue_url=QueueUrl)
    queue = queues.get_queue(queue_name)
    tags = queue.list_tags()
    if len(tags) == 0:
        return {}
    return {"Tag": [{"Key": tag.name, "Value": tag.value} for tag in tags]}


def untag_queue(queues: QueueStorage, QueueUrl: str, **kwargs):
    queue_name = name_from_url(queue_url=QueueUrl)
    queue = queues.get_queue(queue_name)
    tag_names = [v for k, v in kwargs.items() if "Tag" in k]
    for tag_name in tag_names:
        queue.un_tag(tag_name)


def _parse_tag_request_data(request_tags: Dict) -> List[Tag]:
    tags = []
    # tagのrequest dataはTag.1.Key: "key_name", Tag.1.Value: "value"
    # そのため、要素数を2で割りtagの数を取り出す
    for i in range(1, int(len(request_tags) / 2) + 1):
        tag_name = request_tags[f"Tag.{i}.Key"]
        tag_value = request_tags[f"Tag.{i}.Value"]
        tags.append(Tag(tag_name, tag_value))
    return tags
