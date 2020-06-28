import pytest
from dict2xml import dict2xml
from faws.sqs.error import NonExistentQueue
from faws.sqs.result import SuccessResult, ErrorResult


@pytest.mark.parametrize(
    "result,response",
    [
        (
            SuccessResult(
                operation_name="test",
                result_data={"test_result": "hoge"},
                request_id="111",
            ),
            {
                "testResponse": {
                    "testResult": {"test_result": "hoge"},
                    "ResponseMetadata": {"RequestId": "111"},
                }
            },
        ),
        (
            SuccessResult(operation_name="test", result_data=None, request_id="111"),
            {"testResponse": {"ResponseMetadata": {"RequestId": "111"}}},
        ),
    ],
)
def test_result(result, response):
    assert result.generate_response() == dict2xml(response)


def test_error_result():
    result = ErrorResult(NonExistentQueue(), request_id="111")

    assert result.generate_response() == dict2xml(
        {
            "ErrorResponse": {
                "Error": {
                    "Type": "Sender",
                    "Code": "AWS.SimpleQueueService.NonExistentQueue",
                    "Message": "The specified queue does not exist for this wsdl version.",
                    "Detail": {},
                },
                "ResponseMetadata": {"RequestId": "111"},
            }
        }
    )
