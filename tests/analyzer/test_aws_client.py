import os

import pytest
from botocore.client import BaseClient
from botocore.exceptions import (
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
)
from moto import mock_glue, mock_logs

from aws_glue_workflow_analyzer.analyzer.aws_client import AWSClientManager
from aws_glue_workflow_analyzer.exceptions import CredentialsNotFoundError
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


@mock_glue
@mock_logs
def test_aws_client_manager_initialization_success():
    """
    Test that AWSClientManager successfully initializes with valid credentials.
    """
    client_manager = AWSClientManager()
    assert isinstance(client_manager.glue_client, BaseClient)
    assert isinstance(client_manager.cloudwatch_logs_client, BaseClient)


def test_aws_client_manager_missing_credentials(mocker):
    """
    Test that AWSClientManager raises CredentialsNotFoundError when credentials are missing.
    """
    mocker.patch("boto3.client", side_effect=NoCredentialsError())
    with pytest.raises(CredentialsNotFoundError):
        AWSClientManager()


def test_aws_client_manager_partial_credentials(mocker):
    """
    Test that AWSClientManager raises CredentialsNotFoundError when credentials are partial.
    """
    mocker.patch(
        "boto3.client",
        side_effect=PartialCredentialsError(
            provider="aws", cred_var="AWS_ACCESS_KEY_ID"
        ),
    )
    with pytest.raises(CredentialsNotFoundError):
        AWSClientManager()


def test_aws_client_manager_endpoint_connection_error(mocker):
    """
    Test that AWSClientManager raises ConnectionError when endpoint connection fails.
    """
    mocker.patch(
        "boto3.client",
        side_effect=EndpointConnectionError(endpoint_url="https://example.com"),
    )
    with pytest.raises(ConnectionError):
        AWSClientManager()
