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


@dataclasses.dataclass
class Message:
    message_body: str
    message_attributes: Optional[Dict[str, MessageAttribute]] = None
    delay_seconds: int = 0
    message_system_attributes: Optional[Dict] = None
    message_deduplication_id: Optional[str] = None
    message_group_id: Optional[str] = None
    message_inserted_at: datetime = datetime.datetime.now()
    message_id: str = dataclasses.field(init=False)
    message_deliverable_time: datetime = dataclasses.field(init=False)

    def __post_init__(self):
        self.message_attributes = MessageAttribute.from_request_data(
            self.message_attributes
        )
        self.message_id = generate_uuid()
        self.message_deliverable_time = self.message_inserted_at + datetime.timedelta(
            seconds=self.delay_seconds
        )

    def is_callable(self):
        return True
