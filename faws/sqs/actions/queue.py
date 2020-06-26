from typing import Dict
from faws.sqs.queue import name_from_url
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
    return queues.delete_queue(queue_name)
