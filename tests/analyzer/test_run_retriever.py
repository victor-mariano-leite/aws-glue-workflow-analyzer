import datetime
import os
from collections import defaultdict

import pytest
from botocore.exceptions import ClientError

from aws_glue_workflow_analyzer.analyzer.run_retriever import WorkflowRunRetriever
from aws_glue_workflow_analyzer.exceptions import APIRequestError
from aws_glue_workflow_analyzer.logger import logger


class MockGlueWorkflow:
    def __init__(self):
        self.workflows = {}
        self.workflow_runs = defaultdict(list)

    def create_workflow(self, Name):
        if Name in self.workflows:
            raise ValueError(f"Workflow {Name} already exists")
        self.workflows[Name] = []

    def start_workflow_run(self, Name):
        if Name not in self.workflows:
            raise ValueError(f"Workflow {Name} does not exist")

        run_id = f"run-{len(self.workflow_runs[Name]) + 1}"
        started_on = datetime.datetime.now()
        run = {
            "RunId": run_id,
            "StartedOn": started_on,
            "Status": "RUNNING",
        }
        self.workflow_runs[Name].append(run)
        return run_id

    def get_workflow_runs(self, Name, IncludeGraph=False, MaxResults=None):
        if Name not in self.workflows:
            raise ClientError(
                {
                    "Error": {
                        "Code": "EntityNotFoundException",
                        "Message": f"Workflow {Name} does not exist",
                    }
                },
                "GetWorkflowRuns",
            )

        runs = self.workflow_runs[Name]

        if not IncludeGraph:
            for run in runs:
                run.pop("Graph", None)

        if MaxResults:
            runs = runs[:MaxResults]

        return {"Runs": runs}

    def delete_workflow(self, Name):
        if Name in self.workflows:
            del self.workflows[Name]
            self.workflow_runs.pop(Name, None)
        else:
            raise ValueError(f"Workflow {Name} does not exist")


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
def glue_client():
    return MockGlueWorkflow()


@pytest.fixture
def workflow_run_retriever(glue_client):
    return WorkflowRunRetriever(glue_client)


def test_get_workflow_runs_success(workflow_run_retriever, glue_client):
    glue_client.create_workflow(Name="test_workflow")

    glue_client.start_workflow_run(Name="test_workflow")
    glue_client.start_workflow_run(Name="test_workflow")

    workflow_runs = workflow_run_retriever.get_workflow_runs("test_workflow", days=30)

    assert len(workflow_runs) == 2, "The number of retrieved workflow runs should be 2"
    assert (
        "StartedOn" in workflow_runs[0]
    ), "The workflow run should have a 'StartedOn' field"


def test_get_workflow_runs_filtered_by_date(workflow_run_retriever, glue_client):
    glue_client.create_workflow(Name="test_workflow")

    old_date = datetime.datetime.now() - datetime.timedelta(days=40)
    recent_date = datetime.datetime.now() - datetime.timedelta(days=10)

    glue_client.start_workflow_run(Name="test_workflow")
    glue_client.start_workflow_run(Name="test_workflow")

    # Manually set the start dates
    workflow_runs = glue_client.get_workflow_runs(Name="test_workflow")["Runs"]
    workflow_runs[0]["StartedOn"] = old_date
    workflow_runs[1]["StartedOn"] = recent_date

    # Ensure the mock handles date filtering as expected
    filtered_runs = workflow_run_retriever.get_workflow_runs("test_workflow", days=30)

    assert (
        len(filtered_runs) == 1
    ), "There should be only 1 workflow run within the last 30 days"
    assert (
        filtered_runs[0]["StartedOn"] >= recent_date
    ), "The returned run should be within the last 30 days"


def test_get_workflow_runs_api_request_error(workflow_run_retriever, glue_client):
    glue_client.create_workflow(Name="test_workflow")

    glue_client.delete_workflow(Name="test_workflow")

    with pytest.raises(APIRequestError):
        workflow_run_retriever.get_workflow_runs("test_workflow", days=30)


def test_get_workflow_runs_no_runs(workflow_run_retriever, glue_client):
    glue_client.create_workflow(Name="test_workflow")

    workflow_runs = workflow_run_retriever.get_workflow_runs("test_workflow", days=30)

    assert len(workflow_runs) == 0, "The workflow should have no runs"
