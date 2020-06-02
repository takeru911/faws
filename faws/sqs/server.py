from flask import Flask, request, Response
from typing import Dict
from faws.sqs.queues import Queues
from faws.sqs.queues_storage import QueuesStorageType
import dicttoxml

app = Flask(__name__)
queues = Queues(QueuesStorageType.IN_MEMORY)


def parse_request_data(request_data: str):
    parsed_data = {}
    for param in request_data.split("&"):
        key, value = param.split("=")
        parsed_data[key] = value

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


def do_operation(request_data: Dict):
    action = request_data["Action"]
    if action == "ListQueues":
        return get_list_queues(**request_data)
    if action == "GetQueueUrl":
        return get_queue_url(**request_data)
    if action == "CreateQueue":
        return create_queue(**request_data)
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
