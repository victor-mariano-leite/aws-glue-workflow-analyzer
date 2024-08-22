from unittest.mock import mock_open, patch

import pytest

from aws_glue_workflow_analyzer.output import save_to_csv, save_to_json


@pytest.fixture
def sample_data():
    """Fixture to provide sample data for testing."""
    return [
        {"Id": "item1", "Name": "Test Item 1", "Value": 123},
        {"Id": "item2", "Name": "Test Item 2", "Value": 456},
    ]


@patch("aws_glue_workflow_analyzer.output.open", new_callable=mock_open)
@patch("aws_glue_workflow_analyzer.output.logger")
def test_save_to_json(mock_logger, mock_file, sample_data):
    """Test saving data to a JSON file."""
    file_path = "test_output.json"

    save_to_json(sample_data, file_path)

    mock_file.assert_called_once_with(file_path, "w", encoding="utf-8")

    mock_file().write.assert_called()

    mock_logger.info.assert_called_once_with(f"Analysis results saved to {file_path}")


@patch("aws_glue_workflow_analyzer.output.open", new_callable=mock_open)
@patch("aws_glue_workflow_analyzer.output.logger")
def test_save_to_json_io_error(mock_logger, mock_file, sample_data):
    """Test saving data to a JSON file when an IOError occurs."""
    mock_file.side_effect = IOError("Failed to write to file")
    file_path = "test_output.json"

    save_to_json(sample_data, file_path)

    mock_logger.error.assert_called_once_with(
        "Failed to save analysis results to JSON: Failed to write to file"
    )


@patch("aws_glue_workflow_analyzer.output.open", new_callable=mock_open)
@patch("aws_glue_workflow_analyzer.output.logger")
def test_save_to_csv(mock_logger, mock_file, sample_data):
    """Test saving data to a CSV file."""
    file_path = "test_output.csv"

    save_to_csv(sample_data, file_path)

    mock_file.assert_called_once_with(file_path, "w", newline="", encoding="utf-8")

    handle = mock_file()

    handle.write.assert_any_call(",".join(sample_data[0].keys()) + "\r\n")
    for item in sample_data:
        handle.write.assert_any_call(
            ",".join(str(value) for value in item.values()) + "\r\n"
        )

    mock_logger.info.assert_called_once_with(f"Analysis results saved to {file_path}")


@patch("aws_glue_workflow_analyzer.output.open", new_callable=mock_open)
@patch("aws_glue_workflow_analyzer.output.logger")
def test_save_to_csv_empty_data(mock_logger, mock_file):
    """Test saving empty data to a CSV file."""
    file_path = "test_output.csv"
    empty_data = []

    save_to_csv(empty_data, file_path)

    mock_file.assert_not_called()

    mock_logger.warning.assert_called_once_with("No data to save to CSV.")


@patch("aws_glue_workflow_analyzer.output.open", new_callable=mock_open)
@patch("aws_glue_workflow_analyzer.output.logger")
def test_save_to_csv_io_error(mock_logger, mock_file, sample_data):
    """Test saving data to a CSV file when an IOError occurs."""
    mock_file.side_effect = IOError("Failed to write to file")
    file_path = "test_output.csv"

    save_to_csv(sample_data, file_path)

    mock_logger.error.assert_called_once_with(
        "Failed to save analysis results to CSV: Failed to write to file"
    )
