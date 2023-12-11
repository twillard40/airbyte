#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import ParentStreamConfig
from airbyte_cdk.sources.streams import Stream
from requests import Response
from source_monday.components import IncrementalSingleSlice, IncrementalSubstreamSlicer
from source_monday.extractor import MondayIncrementalItemsExtractor


def _create_response(content: Any) -> Response:
    response = Response()
    response._content = json.dumps(content).encode("utf-8")
    return response


def test_slicer():
    date_time_dict = {"updated_at": 1662459010}
    slicer = IncrementalSingleSlice(config={}, parameters={}, cursor_field="updated_at")
    slicer.close_slice(date_time_dict, date_time_dict)
    assert slicer.get_stream_state() == date_time_dict
    assert slicer.get_request_headers() == {}
    assert slicer.get_request_body_data() == {}
    assert slicer.get_request_body_json() == {}


@pytest.mark.parametrize(
    "last_record, expected, records",
    [
        (
            {"first_stream_cursor": 1662459010},
            {"parent_stream_name": {"parent_cursor_field": 1662459010}, "first_stream_cursor": 1662459010},
            [{"first_stream_cursor": 1662459010}],
        ),
        (None, {}, []),
    ],
)
def test_sub_slicer(last_record, expected, records):
    parent_stream = Mock(spec=Stream)
    parent_stream.name = "parent_stream_name"
    parent_stream.cursor_field = "parent_cursor_field"
    parent_stream.stream_slices.return_value = [{"a slice": "value"}]
    parent_stream.read_records = MagicMock(return_value=records)

    parent_config = ParentStreamConfig(
        stream=parent_stream,
        parent_key="id",
        partition_field="first_stream_id",
        parameters={},
        config={},
    )

    slicer = IncrementalSubstreamSlicer(
        config={}, parameters={}, cursor_field="first_stream_cursor", parent_stream_configs=[parent_config], nested_items_per_page=10
    )
    stream_slice = next(slicer.stream_slices()) if records else {}
    slicer.close_slice(stream_slice, last_record)
    assert slicer.get_stream_state() == expected


def test_null_records(caplog):
    extractor = MondayIncrementalItemsExtractor(
        field_path=["data", "boards", "*"],
        config={},
        parameters={},
    )
    content = {
        "data": {
            "boards": [
                {"board_kind": "private", "id": "1234561", "updated_at": "2023-08-15T10:30:49Z"},
                {"board_kind": "private", "id": "1234562", "updated_at": "2023-08-15T10:30:50Z"},
                {"board_kind": "private", "id": "1234563", "updated_at": "2023-08-15T10:30:51Z"},
                {"board_kind": "private", "id": "1234564", "updated_at": "2023-08-15T10:30:52Z"},
                {"board_kind": "private", "id": "1234565", "updated_at": "2023-08-15T10:30:43Z"},
                {"board_kind": "private", "id": "1234566", "updated_at": "2023-08-15T10:30:54Z"},
                None,
                None,
            ]
        },
        "errors": [{"message": "Cannot return null for non-nullable field Board.creator"}],
        "account_id": 123456,
    }
    response = _create_response(content)
    records = extractor.extract_records(response)
    warning_message = "Record with null value received; errors: [{'message': 'Cannot return null for non-nullable field Board.creator'}]"
    assert warning_message in caplog.messages
    expected_records = [
        {"board_kind": "private", "id": "1234561", "updated_at": "2023-08-15T10:30:49Z", "updated_at_int": 1692095449},
        {"board_kind": "private", "id": "1234562", "updated_at": "2023-08-15T10:30:50Z", "updated_at_int": 1692095450},
        {"board_kind": "private", "id": "1234563", "updated_at": "2023-08-15T10:30:51Z", "updated_at_int": 1692095451},
        {"board_kind": "private", "id": "1234564", "updated_at": "2023-08-15T10:30:52Z", "updated_at_int": 1692095452},
        {"board_kind": "private", "id": "1234565", "updated_at": "2023-08-15T10:30:43Z", "updated_at_int": 1692095443},
        {"board_kind": "private", "id": "1234566", "updated_at": "2023-08-15T10:30:54Z", "updated_at_int": 1692095454},
    ]
    assert records == expected_records


def mock_stream_slices(*args, **kwargs):
    return iter([{"ids": [123, 456]}])

@pytest.fixture
def mock_parent_stream():
    mock_stream = MagicMock(spec=Stream)
    mock_stream.primary_key = "id"  # Example primary key
    mock_stream.stream_slices = mock_stream_slices
    mock_stream.read_records = MagicMock(return_value=iter([
        {
            "id": 456,
            "name": "Sample Project 2",
            "updated_at": "2023-01-03T00:00:00Z",
            "status": "In Progress",
            "due_date": "2023-07-15",
            "assignee": {
                "id": 67890,
                "name": "John Doe"
            }
        },
        {
            "id": 123,
            "name": "Sample Project",
            "updated_at": "2023-01-01T00:00:00Z",
            "status": "In Progress",
            "due_date": "2023-07-15",
            "assignee": {
                "id": 67890,
                "name": "John Doe"
            }
        }]))
    return mock_stream

@pytest.mark.parametrize("stream_state, expected_slices", [
    ({}, [{}]),  # Empty state
    ({"updated_at": "2022-01-01T00:00:00Z"}, []),  # Non-empty state
])
def test_read_parent_stream(mock_parent_stream, stream_state, expected_slices):

    slicer = IncrementalSubstreamSlicer(
        config={},
        parameters={},
        cursor_field="updated_at",
        parent_stream_configs=[MagicMock()],
        nested_items_per_page=10
    )

    slicer.parent_cursor_field = "updated_at"

    print("Calling read_parent_stream...")
    slices = list(slicer.read_parent_stream(
        sync_mode=SyncMode.full_refresh,
        cursor_field="updated_at",
        stream_state=stream_state
    ))

    print(f"Slices received: {slices}")
    print(f"read_records called: {mock_parent_stream.read_records.call_count} times")
    print(f"read_records call args: {mock_parent_stream.read_records.call_args}")

    # Assertions
    assert slices == expected_slices
