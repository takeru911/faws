from __future__ import annotations
import dataclasses
import datetime
import enum
import uuid
from typing import Dict, Optional


def generate_uuid() -> str:
    return str(uuid.uuid4())


class MessageAttributeType(enum.Enum):
    BINARY = "Binary"
    STRING = "String"
    NUMBER = "Number"


@dataclasses.dataclass
class MessageAttribute:
    data_type: MessageAttributeType
    value: str

    @classmethod
    def from_request_data(
        cls, request_message_attribute_data: Dict
    ) -> Optional[Dict[str, MessageAttribute]]:
        if (
            not request_message_attribute_data
            or len(request_message_attribute_data) == 0
        ):
            return None

        num_attributes = int(len(request_message_attribute_data) / 3)
        message_attributes = {}
        for i in range(1, num_attributes + 1):
            attribute_name = request_message_attribute_data[
                f"MessageAttribute.{i}.Name"
            ]
            try:
                attribute_data_type = MessageAttributeType(
                    request_message_attribute_data[
                        f"MessageAttribute.{i}.Value.DataType"
                    ]
                )
                if attribute_data_type == MessageAttributeType.STRING:
                    attribute_value = request_message_attribute_data[
                        f"MessageAttribute.{i}.Value.StringValue"
                    ]
                if attribute_data_type == MessageAttributeType.NUMBER:
                    attribute_value = request_message_attribute_data[
                        f"MessageAttribute.{i}.Value.StringValue"
                    ]
                if attribute_data_type == MessageAttributeType.BINARY:
                    attribute_value = request_message_attribute_data[
                        f"MessageAttribute.{i}.Value.BinaryValue"
                    ]
            except ValueError:
                raise ValueError(
                    f"The type of message(user) attribute '{attribute_name}' is invalid. "
                    f"You must use only the following supported type prefixes: Binary, Number, String"
                )
            message_attributes[attribute_name] = MessageAttribute(
                attribute_data_type, attribute_value
            )

        return message_attributes

    def to_dict(self) -> Dict[str, Dict]:
        return {
            # String/Numberの場合はStringValue, Binaryの場合はBinaryValueとする
            "StringValue"
            if self.data_type != MessageAttributeType.BINARY
            else "BinaryValue": self.value,
            "DataType": self.data_type.value,
        }


class Message:
    def __init__(self,
                 message_body: str,
                 message_attributes: Optional[Dict] = None,
                 delay_seconds: int = 0,
                 visibility_timeout: int = 30
                 ):
        self._message_body = message_body
        self._message_attributes = MessageAttribute.from_request_data(
            message_attributes
        )
        self._delay_seconds = delay_seconds
        self._message_id = generate_uuid()
        self._message_inserted_at = datetime.datetime.now()
        self._message_deliverable_time = self._message_inserted_at + datetime.timedelta(
            seconds=self._delay_seconds
        )
        self._visibility_timeout = visibility_timeout

    @property
    def message_body(self) -> str:
        return self._message_body

    @property
    def message_attributes(self) -> Dict[str, MessageAttribute]:
        return self._message_attributes

    @property
    def message_id(self) -> str:
        return self._message_id

    @property
    def message_deliverable_time(self) -> datetime:
        return self._message_deliverable_time

    def update_deliverable_time(self, visibility_timeout: int):
        self._message_deliverable_time = datetime.datetime.now() + datetime.timedelta(
            seconds=visibility_timeout
        )

    def is_callable(self) -> bool:
        if self._message_deliverable_time <= datetime.datetime.now():
            return True
        return False
