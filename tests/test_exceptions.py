import pytest

from aws_glue_workflow_analyzer.exceptions import (
    APIRequestError,
    CredentialsNotFoundError,
    WorkflowAnalyzerError,
    WorkflowConnectionError,
)


def test_workflow_analyzer_error():
    """Test that WorkflowAnalyzerError can be raised and caught."""
    with pytest.raises(WorkflowAnalyzerError) as exc_info:
        raise WorkflowAnalyzerError("An error occurred in the workflow analyzer.")

    assert str(exc_info.value) == "An error occurred in the workflow analyzer."


def test_credentials_not_found_error_default_message():
    """Test CredentialsNotFoundError with the default message."""
    with pytest.raises(CredentialsNotFoundError) as exc_info:
        raise CredentialsNotFoundError()

    assert (
        str(exc_info.value)
        == "AWS credentials not found. Please configure your AWS credentials."
    )


def test_credentials_not_found_error_custom_message():
    """Test CredentialsNotFoundError with a custom message."""
    with pytest.raises(CredentialsNotFoundError) as exc_info:
        raise CredentialsNotFoundError("Custom message for credentials error.")

    assert str(exc_info.value) == "Custom message for credentials error."


def test_api_request_error_default_message():
    """Test APIRequestError with the default message."""
    with pytest.raises(APIRequestError) as exc_info:
        raise APIRequestError()

    assert (
        str(exc_info.value)
        == "An error occurred while making an API request to AWS services."
    )


def test_api_request_error_custom_message():
    """Test APIRequestError with a custom message."""
    with pytest.raises(APIRequestError) as exc_info:
        raise APIRequestError("Custom message for API request error.")

    assert str(exc_info.value) == "Custom message for API request error."


def test_connection_error_default_message():
    """Test WorkflowConnectionError with the default message."""
    with pytest.raises(WorkflowConnectionError) as exc_info:
        raise WorkflowConnectionError()

    assert (
        str(exc_info.value)
        == "Could not connect to AWS services. Please check your network connection and AWS endpoint configurations."
    )


def test_connection_error_custom_message():
    """Test WorkflowConnectionError with a custom message."""
    with pytest.raises(WorkflowConnectionError) as exc_info:
        raise WorkflowConnectionError("Custom message for connection error.")

    assert str(exc_info.value) == "Custom message for connection error."
