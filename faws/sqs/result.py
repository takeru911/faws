import dataclasses
from dict2xml import dict2xml
from typing import Dict, Optional
from faws.sqs.error import SQSError
from abc import abstractmethod


class Result:
    @property
    def response_code(self):
        raise NotImplementedError

    @abstractmethod
    def generate_response(self) -> str:
        raise NotImplementedError


@dataclasses.dataclass()
class SuccessResult(Result):
    operation_name: str
    result_data: Optional[Dict]
    request_id: str
    response_code: int = 200

    def generate_response(self) -> str:
        if self.result_data is not None:
            return dict2xml(
                {
                    f"{self.operation_name}Response": {
                        f"{self.operation_name}Result": self.result_data,
                        "ResponseMetadata": {"RequestId": self.request_id},
                    }
                }
            )
        return dict2xml(
            {
                f"{self.operation_name}Response": {
                    "ResponseMetadata": {"RequestId": self.request_id},
                }
            }
        )


@dataclasses.dataclass()
class ErrorResult(Result):
    error: SQSError
    request_id: str
    response_code: int = 400

    def generate_response(self) -> str:
        return dict2xml(
            {
                f"ErrorResponse": {
                    "Error": {
                        "Type": "Sender",
                        "Code": f"AWS.SimpleQueueService.{self.error.__class__.__name__}",
                        "Message": self.error.message,
                        "Detail": {},
                    },
                    "ResponseMetadata": {"RequestId": self.request_id},
                },
            }
        )
