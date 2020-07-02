import pytest
from faws.sqs.actions import queue
from faws.sqs.queue import Tag


@pytest.mark.parametrize(
    "tag_request_data,tags",
    [
        (
            {"Tag.1.Key": "tag_name", "Tag.1.Value": "tag_value"},
            [Tag("tag_name", "tag_value")],
        ),
        (
            {
                "Tag.1.Key": "tag_name",
                "Tag.1.Value": "tag_value",
                "Tag.2.Key": "tag_name_2",
                "Tag.2.Value": "tag_value_2",
            },
            [Tag("tag_name", "tag_value"), Tag("tag_name_2", "tag_value_2"),],
        ),
    ],
)
def test_parse_tag_request_data(tag_request_data, tags):
    assert queue._parse_tag_request_data(tag_request_data) == tags
