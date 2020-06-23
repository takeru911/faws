from flask import Flask, request, Response, g, current_app
from faws.sqs.actions.message import send_message, receive_message
from faws.sqs.actions.queue import (
    create_queue,
    get_queue_url,
    get_list_queues,
    delete_queue,
)
from typing import Dict
from faws.sqs.queue_storage import build_queues_storage, QueuesStorageType
from dict2xml import dict2xml
import dataclasses
import urllib
import uuid


@dataclasses.dataclass()
class Result:
    operation_name: str
    result_data: Dict


def init_queues():
    queues = get_queues()
    queues.init_storage()


def get_queues():
    if "queues" not in g:
        g.queues = build_queues_storage(
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


def do_operation(request_data: Dict) -> Result:
    action = request_data["Action"]
    queues = get_queues()
    if action == "ListQueues":
        return Result(action, get_list_queues(queues, **request_data))
    if action == "GetQueueUrl":
        return Result(action, get_queue_url(queues, **request_data))
    if action == "CreateQueue":
        return Result(action, create_queue(queues, **request_data))
    if action == "DeleteQueue":
        return Result(action, delete_queue(queues, **request_data))
    if action == "SendMessage":
        return Result(action, send_message(queues, **request_data))
    if action == "ReceiveMessage":
        return Result(action, receive_message(queues, **request_data))

    raise NotImplementedError()


def run_request_to_index(request):
    request_id = uuid.uuid4()
    request_data = parse_request_data(request.get_data().decode(encoding="utf-8"))

    result = do_operation(request_data)

    if result.result_data is None:
        return dict2xml(
            {
                f"{result.operation_name}Response": {
                    "ResponseMetadata": {"RequestId": request_id},
                }
            }
        )
    return dict2xml(
        {
            f"{result.operation_name}Response": {
                f"{result.operation_name}Result": result.result_data,
                "ResponseMetadata": {"RequestId": request_id},
            }
        }
    )


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
