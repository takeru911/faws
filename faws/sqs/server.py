import urllib
import uuid
from flask import Flask, request, Response, g, current_app
from typing import Dict
from faws.sqs.actions.message import send_message, receive_message
from faws.sqs.actions.queue import (
    create_queue,
    get_queue_url,
    get_list_queues,
    delete_queue,
    purge_queue,
)
from faws.sqs.error import SQSError
from faws.sqs.queue_storage import build_queues_storage, QueuesStorageType
from faws.sqs.result import Result, ErrorResult, SuccessResult


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


def do_operation(request_data: Dict, request_id: str) -> Result:
    action = request_data["Action"]
    queues = get_queues()
    try:
        if action == "ListQueues":
            return SuccessResult(
                action, get_list_queues(queues, **request_data), request_id
            )
        if action == "GetQueueUrl":
            return SuccessResult(
                action, get_queue_url(queues, **request_data), request_id
            )
        if action == "CreateQueue":
            return SuccessResult(
                action, create_queue(queues, **request_data), request_id
            )
        if action == "DeleteQueue":
            return SuccessResult(
                action, delete_queue(queues, **request_data), request_id
            )
        if action == "PurgeQueue":
            return SuccessResult(
                action, purge_queue(queues, **request_data), request_id
            )
        if action == "SendMessage":
            return SuccessResult(
                action, send_message(queues, **request_data), request_id
            )
        if action == "ReceiveMessage":
            return SuccessResult(
                action, receive_message(queues, **request_data), request_id
            )
    except SQSError as e:
        return ErrorResult(e, request_id)

    raise NotImplementedError()


def run_request_to_index(request_):
    request_id = str(uuid.uuid4())
    request_data = parse_request_data(request_.get_data().decode(encoding="utf-8"))

    result = do_operation(request_data, request_id)

    return result


def create_app(app_config: Dict = None):
    app = Flask(__name__, instance_relative_config=True)
    app.config["QueuesStorageType"] = QueuesStorageType.IN_MEMORY
    if app_config is not None:
        app.config.update(app_config)

    @app.route("/", methods=["POST"])
    def index():
        response_data = run_request_to_index(request)
        return Response(
            response_data.generate_response(),
            mimetype="text/xml",
            status=response_data.response_code,
        )

    return app
