from unittest.mock import MagicMock, patch

from aws_glue_workflow_analyzer.__main__ import main
from aws_glue_workflow_analyzer.exceptions import WorkflowAnalyzerError


@patch("aws_glue_workflow_analyzer.__main__.parse_args")
@patch("aws_glue_workflow_analyzer.__main__.GlueWorkflowAnalyzer")
@patch("aws_glue_workflow_analyzer.__main__.save_to_json")
@patch("aws_glue_workflow_analyzer.__main__.console")
def test_main_json_output(
    mock_console, mock_save_to_json, mock_analyzer, mock_parse_args
):
    mock_parse_args.return_value = MagicMock(
        workflows=["workflow1"], days=30, output="output.json", format="json"
    )
    mock_analyzer_instance = MagicMock()
    mock_analyzer.return_value = mock_analyzer_instance
    mock_analyzer_instance.analyze_workflows.return_value = [{"key": "value"}]

    main()

    mock_analyzer_instance.analyze_workflows.assert_called_once_with(["workflow1"], 30)
    mock_save_to_json.assert_called_once_with([{"key": "value"}], "output.json")
    mock_console.print_json.assert_not_called()


@patch("aws_glue_workflow_analyzer.__main__.parse_args")
@patch("aws_glue_workflow_analyzer.__main__.GlueWorkflowAnalyzer")
@patch("aws_glue_workflow_analyzer.__main__.save_to_csv")
@patch("aws_glue_workflow_analyzer.__main__.console")
def test_main_csv_output(
    mock_console, mock_save_to_csv, mock_analyzer, mock_parse_args
):
    mock_parse_args.return_value = MagicMock(
        workflows=["workflow1"], days=30, output="output.csv", format="csv"
    )
    mock_analyzer_instance = MagicMock()
    mock_analyzer.return_value = mock_analyzer_instance
    mock_analyzer_instance.analyze_workflows.return_value = [{"key": "value"}]

    main()

    mock_analyzer_instance.analyze_workflows.assert_called_once_with(["workflow1"], 30)
    mock_save_to_csv.assert_called_once_with([{"key": "value"}], "output.csv")
    mock_console.print_json.assert_not_called()


@patch("aws_glue_workflow_analyzer.__main__.parse_args")
@patch("aws_glue_workflow_analyzer.__main__.GlueWorkflowAnalyzer")
@patch("aws_glue_workflow_analyzer.__main__.console")
def test_main_console_output(mock_console, mock_analyzer, mock_parse_args):
    mock_parse_args.return_value = MagicMock(
        workflows=["workflow1"], days=30, output=None, format="json"
    )
    mock_analyzer_instance = MagicMock()
    mock_analyzer.return_value = mock_analyzer_instance
    mock_analyzer_instance.analyze_workflows.return_value = [{"key": "value"}]

    main()

    mock_analyzer_instance.analyze_workflows.assert_called_once_with(["workflow1"], 30)
    mock_console.print_json.assert_called_once_with(data={"key": "value"})


@patch("aws_glue_workflow_analyzer.__main__.parse_args")
@patch("aws_glue_workflow_analyzer.__main__.GlueWorkflowAnalyzer")
@patch("aws_glue_workflow_analyzer.__main__.logger")
def test_main_workflow_analyzer_error(mock_logger, mock_analyzer, mock_parse_args):
    mock_parse_args.return_value = MagicMock(
        workflows=["workflow1"], days=30, output=None, format="json"
    )
    mock_analyzer_instance = MagicMock()
    mock_analyzer.return_value = mock_analyzer_instance
    mock_analyzer_instance.analyze_workflows.side_effect = WorkflowAnalyzerError(
        "Test error"
    )

    main()

    mock_logger.error.assert_called_once_with(
        "An error occurred during workflow analysis: Test error"
    )
