import os
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from aws_glue_workflow_analyzer.analyzer.details_collector import StepDetailsCollector
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


@pytest.fixture
def error_context_retriever_mock():
    return MagicMock()


@pytest.fixture
def table_analyzer_mock():
    return MagicMock()


@pytest.fixture
def step_details_collector(error_context_retriever_mock, table_analyzer_mock):
    return StepDetailsCollector(
        error_context_retriever=error_context_retriever_mock,
        table_analyzer=table_analyzer_mock,
    )


def test_get_step_execution_details_success(
    step_details_collector, error_context_retriever_mock, table_analyzer_mock
):
    workflow_name = "test_workflow"
    workflow_run = {
        "RunId": "test_run_id",
        "StartedOn": datetime(2021, 6, 1, 12, 0, 0),
        "CompletedOn": datetime(2021, 6, 1, 13, 0, 0),
        "LogGroup": "test_log_group",
        "LogStream": "test_log_stream",
        "Graph": {},
        "Arguments": {"param1": "value1"},
    }
    node = {
        "Id": "test_node_id",
        "Type": "Job",
        "Name": "Test Node",
        "Status": "SUCCEEDED",
    }

    error_context_retriever_mock.get_error_context.return_value = (
        "This is an error message"
    )
    table_analyzer_mock.get_affected_tables.return_value = ["table1", "table2"]

    step_details = step_details_collector.get_step_execution_details(
        workflow_name, workflow_run, node
    )

    assert step_details["execution_id"] == "test_run_id"
    assert step_details["workflow_name"] == workflow_name
    assert step_details["node_id"] == "test_node_id"
    assert step_details["node_type"] == "Job"
    assert step_details["execution_status"] == "SUCCEEDED"
    assert step_details["execution_duration"] == 3600  # 1 hour in seconds


def test_get_step_execution_details_no_logs(
    step_details_collector, error_context_retriever_mock, table_analyzer_mock
):
    workflow_name = "test_workflow"
    workflow_run = {
        "RunId": "test_run_id",
        "StartedOn": MagicMock(timestamp=lambda: 1622553000),
        "CompletedOn": MagicMock(timestamp=lambda: 1622556600),
        "Graph": {},
        "Arguments": {"param1": "value1"},
    }
    node = {
        "Id": "test_node_id",
        "Type": "Job",
        "Name": "Test Node",
        "Status": "SUCCEEDED",
    }

    error_context_retriever_mock.get_error_context.return_value = None
    table_analyzer_mock.get_affected_tables.return_value = ["table1", "table2"]

    step_details = step_details_collector.get_step_execution_details(
        workflow_name, workflow_run, node
    )

    assert step_details["error_message"] is None
    assert step_details["log_group_name"] == ""
    assert step_details["log_stream_name"] == ""


def test_get_step_execution_details_failure(
    step_details_collector, error_context_retriever_mock, table_analyzer_mock
):
    workflow_name = "test_workflow"
    workflow_run = {
        "RunId": "test_run_id",
        "StartedOn": MagicMock(timestamp=lambda: 1622553000),
        "CompletedOn": MagicMock(timestamp=lambda: 1622556600),
        "LogGroup": "test_log_group",
        "LogStream": "test_log_stream",
        "Graph": {},
        "Arguments": {"param1": "value1"},
    }
    node = {
        "Id": "test_node_id",
        "Type": "Job",
        "Name": "Test Node",
        "Status": "FAILED",
    }

    error_context_retriever_mock.get_error_context.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Log not found"}},
        "GetLogEvents",
    )

    with pytest.raises(APIRequestError):
        step_details_collector.get_step_execution_details(
            workflow_name, workflow_run, node
        )


def test_get_step_execution_details_duration(step_details_collector):
    workflow_name = "test_workflow"
    workflow_run = {
        "RunId": "test_run_id",
        "StartedOn": datetime(2021, 6, 1, 12, 0, 0),
        "CompletedOn": datetime(2021, 6, 1, 13, 0, 0),  # 1 hour later
        "LogGroup": "test_log_group",
        "LogStream": "test_log_stream",
        "Graph": {},
        "Arguments": {"param1": "value1"},
    }
    node = {
        "Id": "test_node_id",
        "Type": "Job",
        "Name": "Test Node",
        "Status": "SUCCEEDED",
    }

    step_details = step_details_collector.get_step_execution_details(
        workflow_name, workflow_run, node
    )
    assert step_details["execution_duration"] == 3600  # 1 hour in seconds


def test_get_step_execution_details_no_end_time(step_details_collector):
    workflow_name = "test_workflow"
    workflow_run = {
        "RunId": "test_run_id",
        "StartedOn": datetime(2021, 6, 1, 12, 0, 0),
        "CompletedOn": None,  # No end time
        "LogGroup": "test_log_group",
        "LogStream": "test_log_stream",
        "Graph": {},
        "Arguments": {"param1": "value1"},
    }
    node = {
        "Id": "test_node_id",
        "Type": "Job",
        "Name": "Test Node",
        "Status": "RUNNING",
    }

    step_details = step_details_collector.get_step_execution_details(
        workflow_name, workflow_run, node
    )
    assert step_details["execution_duration"] is None
