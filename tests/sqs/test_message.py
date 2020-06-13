import pytest
from faws.sqs.message import MessageAttribute, MessageAttributeType


def test_from_request_data():
    request_data = {
        "MessageAttribute.1.Name": "City",
        "MessageAttribute.1.Value.DataType": "String",
        "MessageAttribute.1.Value.StringValue": "Any+City",
        "MessageAttribute.2.Name": "Greeting",
        "MessageAttribute.2.Value.DataType": "Binary",
        "MessageAttribute.2.Value.BinaryValue": "SGVsbG8sIFdvcmxkIQ%3D%3D",
        "MessageAttribute.3.Name": "Population",
        "MessageAttribute.3.Value.DataType": "Number",
        "MessageAttribute.3.Value.StringValue": "1250800",
    }

    actual = MessageAttribute.from_request_data(request_data)
    expected = {
        "City": MessageAttribute(MessageAttributeType.STRING, "Any+City"),
        "Greeting": MessageAttribute(
            MessageAttributeType.BINARY, "SGVsbG8sIFdvcmxkIQ%3D%3D"
        ),
        "Population": MessageAttribute(MessageAttributeType.NUMBER, "1250800"),
    }
    assert actual == expected


@pytest.mark.parametrize(
    "message_attribute,expected",
    [
        (
            MessageAttribute(MessageAttributeType.BINARY, "hogehuga"),
            {"BinaryValue": "hogehuga", "DataType": "Binary"},
        ),
        (
            MessageAttribute(MessageAttributeType.NUMBER, "123"),
            {"StringValue": "123", "DataType": "Number"},
        ),
        (
            MessageAttribute(MessageAttributeType.STRING, "tenteketen"),
            {"StringValue": "tenteketen", "DataType": "String"},
        ),
    ],
)
def test_message_attribute_to_dict(message_attribute, expected):
    actual = message_attribute.to_dict()
    assert actual == expected
