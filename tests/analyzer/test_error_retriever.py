import os
from datetime import datetime, timedelta

import boto3
import pytest
from moto import mock_logs

from aws_glue_workflow_analyzer.analyzer.error_retriever import ErrorContextRetriever
from aws_glue_workflow_analyzer.exceptions import APIRequestError
from aws_glue_workflow_analyzer.logger import logger


@pytest.fixture(autouse=True)
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(autouse=True)
def mock_logging(monkeypatch):
    monkeypatch.setattr(logger, "info", lambda msg: None)
    monkeypatch.setattr(logger, "error", lambda msg: None)
    monkeypatch.setattr(logger, "debug", lambda msg: None)


@pytest.fixture
def cloudwatch_logs_client():
    with mock_logs():
        client = boto3.client("logs", region_name="us-east-1")

        client.create_log_group(logGroupName="test_log_group")
        client.create_log_stream(
            logGroupName="test_log_group", logStreamName="test_log_stream"
        )

        yield client


@pytest.fixture
def error_context_retriever(cloudwatch_logs_client):
    return ErrorContextRetriever(cloudwatch_logs_client)


def test_get_error_context_success(error_context_retriever, cloudwatch_logs_client):
    # Use recent timestamps (e.g., current time minus a few seconds)
    now = datetime.now()
    timestamp_1 = int((now - timedelta(seconds=30)).timestamp() * 1000)
    timestamp_2 = int((now - timedelta(seconds=20)).timestamp() * 1000)
    timestamp_3 = int((now - timedelta(seconds=10)).timestamp() * 1000)

    response = cloudwatch_logs_client.put_log_events(
        logGroupName="test_log_group",
        logStreamName="test_log_stream",
        logEvents=[
            {"timestamp": timestamp_1, "message": "This is an informational message"},
            {"timestamp": timestamp_2, "message": "An error occurred: error code 123"},
            {"timestamp": timestamp_3, "message": "Process completed successfully"},
        ],
    )

    assert "rejectedLogEventsInfo" not in response, "Log events were rejected"

    fetched_events = cloudwatch_logs_client.get_log_events(
        logGroupName="test_log_group",
        logStreamName="test_log_stream",
        startTime=timestamp_1,
        endTime=timestamp_3 + 1000,
        limit=1000,
    )

    assert (
        len(fetched_events["events"]) == 3
    ), "The number of fetched log events is incorrect"

    context = error_context_retriever.get_error_context(
        log_group_name="test_log_group",
        log_stream_name="test_log_stream",
        start_time=timestamp_1,
        end_time=timestamp_3 + 1000,
    )

    assert (
        "An error occurred: error code 123" in context
    ), "The expected error message was not found in the context"


def test_get_error_context_no_relevant_context(
    error_context_retriever, cloudwatch_logs_client
):
    now = datetime.now()
    timestamp_1 = int((now - timedelta(seconds=30)).timestamp() * 1000)
    timestamp_2 = int((now - timedelta(seconds=20)).timestamp() * 1000)
    timestamp_3 = int((now - timedelta(seconds=10)).timestamp() * 1000)

    cloudwatch_logs_client.put_log_events(
        logGroupName="test_log_group",
        logStreamName="test_log_stream",
        logEvents=[
            {"timestamp": timestamp_1, "message": "This is an informational message"},
            {"timestamp": timestamp_2, "message": "All systems operational"},
            {"timestamp": timestamp_3, "message": "Process completed successfully"},
        ],
    )

    context = error_context_retriever.get_error_context(
        log_group_name="test_log_group",
        log_stream_name="test_log_stream",
        start_time=timestamp_1,
        end_time=timestamp_3 + 1000,
    )

    assert context == "No relevant error context found."


def test_get_error_context_client_error(
    error_context_retriever, cloudwatch_logs_client
):
    cloudwatch_logs_client.delete_log_group(logGroupName="test_log_group")

    with pytest.raises(APIRequestError):
        error_context_retriever.get_error_context(
            log_group_name="test_log_group",
            log_stream_name="test_log_stream",
            start_time=1622553000000,
            end_time=1622556600000,
        )
