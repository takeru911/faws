import pytest


def test_parse_request_data():
    from faws.sqs import server
    actual = server.parse_request_data("Action=ListQueue&version=2015-02-01")
    expected = {"Action": "ListQueue", "version": "2015-02-01"}

    assert actual == expected


def test_do_list_queues():
    from faws.sqs import server
    server.create_queue("test_queue_1")
    server.create_queue("test_queue_2")
    assert server.do_operation({
        "Action": "ListQueues"
    }) == {
               "ListQueuesResponse": {
                   "ListQueuesResult": ["https://sqs-test_queue_1", "https://sqs-test_queue_2"],
                   "ResponseMetadata": {
                       "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                   }
               }
           }


def test_do_create_queue():
    from faws.sqs import server
    assert server.do_operation({
        "Action": "CreateQueue",
        "QueueName": "test-queue"
    }) == {
               "CreateQueueResponse": {
                   "CreateQueueResult": {
                       "QueueUrl": f"https://sqs-test-queue"
                   },
                   "ResponseMetadata": {
                       "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                   }
               }
           }


def test_do_get_queue_url():
    from faws.sqs import server
    server.create_queue("test-queue_1")
    assert server.do_operation({
        "Action": "GetQueueUrl",
        "QueueName": "test-queue_1"
    }) == {
               "GetQueueUrlResponse": {
                   "GetQueueUrlResult": {
                       "QueueUrl": f"https://sqs-test-queue_1"
                   },
                   "ResponseMetadata": {
                       "RequestId": "725275ae-0b9b-4762-b238-436d7c65a1ac"
                   }
               }
           }


def test_determine_operation_raises_when_non_exist_operation():
    from faws.sqs import server
    with pytest.raises(NotImplementedError):
        server.do_operation({"Action": "NonImplementedOperation"})
