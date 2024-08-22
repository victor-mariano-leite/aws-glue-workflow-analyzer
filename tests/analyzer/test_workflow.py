import os
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from moto import mock_glue, mock_logs

from aws_glue_workflow_analyzer.analyzer.workflow import GlueWorkflowAnalyzer
from aws_glue_workflow_analyzer.exceptions import APIRequestError


@pytest.fixture(autouse=True)
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def glue_analyzer():
    """Fixture for initializing GlueWorkflowAnalyzer."""
    with mock_glue():
        with mock_logs():
            analyzer = GlueWorkflowAnalyzer()
            yield analyzer


def test_analyze_workflows_success(glue_analyzer):
    """Test successful analysis of workflows."""
    # Mock data for get_workflow_runs
    workflow_runs = [
        {
            "Graph": {
                "Nodes": [
                    {"Id": "node1", "Type": "Job"},
                    {"Id": "node2", "Type": "Crawler"},
                ]
            },
            "RunId": "run1",
        }
    ]

    # Mock the methods to return expected data
    with patch.object(
        glue_analyzer.run_retriever, "get_workflow_runs", return_value=workflow_runs
    ), patch.object(
        glue_analyzer.step_details_collector,
        "get_step_execution_details",
        return_value={"step": "details"},
    ):
        # Call the method
        result = glue_analyzer.analyze_workflows(["test-workflow"], days=30)

        # Assertions
        assert len(result) == 2
        assert result[0] == {"step": "details"}
        assert result[1] == {"step": "details"}


def test_analyze_workflows_client_error(glue_analyzer):
    """Test handling of ClientError in analyze_workflows."""
    with patch.object(
        glue_analyzer.run_retriever,
        "get_workflow_runs",
        side_effect=ClientError({"Error": {}}, "GetWorkflowRuns"),
    ):
        with pytest.raises(APIRequestError):
            glue_analyzer.analyze_workflows(["test-workflow"], days=30)


def test_analyze_workflows_api_request_error(glue_analyzer):
    """Test handling of APIRequestError in analyze_workflows."""
    with patch.object(
        glue_analyzer.run_retriever,
        "get_workflow_runs",
        side_effect=APIRequestError("Failed request"),
    ):
        with pytest.raises(APIRequestError):
            glue_analyzer.analyze_workflows(["test-workflow"], days=30)


def test_analyze_workflows_no_runs(glue_analyzer):
    """Test handling of workflows with no runs."""
    # Mock the methods to return no workflow runs
    with patch.object(
        glue_analyzer.run_retriever, "get_workflow_runs", return_value=[]
    ):
        # Call the method
        result = glue_analyzer.analyze_workflows(["test-workflow"], days=30)

        # Assertions
        assert result == []


def test_analyze_workflows_partial_failure(glue_analyzer):
    """Test handling of partial failure where some steps fail."""
    # Mock data for get_workflow_runs
    workflow_runs = [
        {
            "Graph": {
                "Nodes": [
                    {"Id": "node1", "Type": "Job"},
                    {"Id": "node2", "Type": "Crawler"},
                ]
            },
            "RunId": "run1",
        }
    ]

    # Mock the methods to simulate a failure on the second step
    with patch.object(
        glue_analyzer.run_retriever, "get_workflow_runs", return_value=workflow_runs
    ), patch.object(
        glue_analyzer.step_details_collector,
        "get_step_execution_details",
        side_effect=[{"step": "details"}, APIRequestError("Failed step")],
    ):
        # Call the method and catch the exception
        with pytest.raises(APIRequestError):
            glue_analyzer.analyze_workflows(["test-workflow"], days=30)


def test_analyze_workflows_empty_workflows(glue_analyzer):
    """Test handling of empty workflow list."""
    # Call the method with an empty workflow list
    result = glue_analyzer.analyze_workflows([], days=30)

    # Assertions
    assert result == []
