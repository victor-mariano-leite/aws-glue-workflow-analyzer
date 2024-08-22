from unittest.mock import Mock, call

from aws_glue_workflow_analyzer.paginator import paginate_boto3


def test_paginate_boto3_single_page():
    """Test paginate_boto3 when the API call returns a single page."""
    # Mock the Boto3 client method
    mock_callable = Mock()
    mock_callable.return_value = {
        "Items": [{"Id": "item1"}, {"Id": "item2"}],
    }

    # Call the paginate_boto3 function
    result = paginate_boto3(mock_callable, dict_key="Items")

    # Check that the result matches the expected output
    assert result == [{"Id": "item1"}, {"Id": "item2"}]
    mock_callable.assert_called_once_with()


def test_paginate_boto3_multiple_pages():
    """Test paginate_boto3 when the API call returns multiple pages."""
    # Mock the Boto3 client method with two pages
    mock_callable = Mock()
    mock_callable.side_effect = [
        {
            "Items": [{"Id": "item1"}, {"Id": "item2"}],
            "NextToken": "token1",
        },
        {
            "Items": [{"Id": "item3"}, {"Id": "item4"}],
        },
    ]

    # Call the paginate_boto3 function
    result = paginate_boto3(mock_callable, dict_key="Items")

    # Check that the result matches the expected output
    assert result == [
        {"Id": "item1"},
        {"Id": "item2"},
        {"Id": "item3"},
        {"Id": "item4"},
    ]

    # Validate the calls to the mock
    mock_callable.assert_has_calls([call(), call(NextToken="token1")])


def test_paginate_boto3_empty_response():
    """Test paginate_boto3 when the API call returns an empty response."""
    # Mock the Boto3 client method returning an empty response
    mock_callable = Mock()
    mock_callable.return_value = {
        "Items": [],
    }

    # Call the paginate_boto3 function
    result = paginate_boto3(mock_callable, dict_key="Items")

    # Check that the result is an empty list
    assert result == []
    mock_callable.assert_called_once_with()


def test_paginate_boto3_no_items_key():
    """Test paginate_boto3 when the API call response does not contain the items key."""
    # Mock the Boto3 client method returning a response without the expected key
    mock_callable = Mock()
    mock_callable.return_value = {}

    # Call the paginate_boto3 function
    result = paginate_boto3(mock_callable, dict_key="Items")

    # Check that the result is an empty list since no items key was found
    assert result == []
    mock_callable.assert_called_once_with()


def test_paginate_boto3_with_additional_params():
    """Test paginate_boto3 with additional parameters passed to the API call."""
    # Mock the Boto3 client method
    mock_callable = Mock()
    mock_callable.return_value = {
        "Items": [{"Id": "item1"}],
    }

    # Call the paginate_boto3 function with additional parameters
    result = paginate_boto3(
        mock_callable, dict_key="Items", Param1="value1", Param2="value2"
    )

    # Check that the result matches the expected output
    assert result == [{"Id": "item1"}]
    mock_callable.assert_called_once_with(Param1="value1", Param2="value2")
