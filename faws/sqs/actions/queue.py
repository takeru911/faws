from faws.sqs.queues import Queues
from typing import Dict


def create_queue(queues: Queues, QueueName: str, **kwargs) -> Dict:
    queue = queues.create_queue(QueueName)
    return {"QueueUrl": queue.queue_url}


def get_list_queues(queues: Queues, **kwargs) -> Dict:
    queue_urls = [queue.queue_url for queue in queues.get_queues()]

    return {"QueueUrl": queue_urls}


def get_queue_url(queues: Queues, QueueName: str, **kwargs) -> Dict:
    queue = queues.get_queue(QueueName)
    return {"QueueUrl": queue.queue_url}
