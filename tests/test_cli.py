import sys

import pytest

from aws_glue_workflow_analyzer.cli import parse_args


def test_parse_args_workflows_required():
    """Test that the workflows argument is required."""
    test_args = []
    sys.argv = ["gwfa"] + test_args
    with pytest.raises(SystemExit):
        parse_args()


def test_parse_args_with_workflows():
    """Test parsing with required workflows argument."""
    test_args = ["-w", "workflow1", "workflow2"]
    sys.argv = ["gwfa"] + test_args
    args = parse_args()
    assert args.workflows == ["workflow1", "workflow2"]


def test_parse_args_with_days():
    """Test parsing with optional days argument."""
    test_args = ["-w", "workflow1", "--days", "15"]
    sys.argv = ["gwfa"] + test_args
    args = parse_args()
    assert args.workflows == ["workflow1"]
    assert args.days == 15


def test_parse_args_with_output():
    """Test parsing with optional output argument."""
    test_args = ["-w", "workflow1", "--output", "output.json"]
    sys.argv = ["gwfa"] + test_args
    args = parse_args()
    assert args.workflows == ["workflow1"]
    assert args.output == "output.json"


def test_parse_args_with_format():
    """Test parsing with optional format argument."""
    test_args = ["-w", "workflow1", "--format", "csv"]
    sys.argv = ["gwfa"] + test_args
    args = parse_args()
    assert args.workflows == ["workflow1"]
    assert args.format == "csv"


def test_parse_args_defaults():
    """Test parsing with default values."""
    test_args = ["-w", "workflow1"]
    sys.argv = ["gwfa"] + test_args
    args = parse_args()
    assert args.workflows == ["workflow1"]
    assert args.days == 30
    assert args.output == "."
    assert args.format == "json"


def test_parse_args_with_all_options():
    """Test parsing with all options provided."""
    test_args = [
        "-w",
        "workflow1",
        "workflow2",
        "-d",
        "7",
        "-o",
        "results.json",
        "-f",
        "csv",
    ]
    sys.argv = ["gwfa"] + test_args
    args = parse_args()
    assert args.workflows == ["workflow1", "workflow2"]
    assert args.days == 7
    assert args.output == "results.json"
    assert args.format == "csv"
