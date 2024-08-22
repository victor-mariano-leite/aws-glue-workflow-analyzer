import os
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_glue

from aws_glue_workflow_analyzer.analyzer.table_analyzer import TableAnalyzer
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
def glue_client():
    with mock_glue():
        client = boto3.client("glue", region_name="us-east-1")
        yield client


@pytest.fixture
def table_analyzer(glue_client):
    return TableAnalyzer(glue_client)


def test_get_affected_tables_from_crawler(table_analyzer, glue_client):
    with patch.object(
        glue_client,
        "get_crawler",
        return_value={
            "Crawler": {
                "Name": "test_crawler",
                "Targets": {"S3Targets": [{"Path": "s3://bucket/path/to/table1"}]},
            }
        },
    ):
        graph = {
            "Nodes": [{"Id": "node1", "Type": "Crawler", "Name": "test_crawler"}],
            "Edges": [],
        }

        affected_tables = table_analyzer.get_affected_tables(graph, "node1")

        assert affected_tables == [
            "table1"
        ], f"Expected ['table1'], but got {affected_tables}"


def test_get_affected_tables_from_job(table_analyzer, glue_client):
    with patch.object(
        glue_client,
        "get_job",
        return_value={
            "Job": {
                "Name": "test_job",
                "OutputDataConfig": {
                    "S3Outputs": [{"S3Uri": "s3://bucket/path/to/table2"}]
                },
            }
        },
    ):
        graph = {
            "Nodes": [{"Id": "node2", "Type": "Job", "Name": "test_job"}],
            "Edges": [],
        }

        affected_tables = table_analyzer.get_affected_tables(graph, "node2")

        assert affected_tables == [
            "table2"
        ], f"Expected ['table2'], but got {affected_tables}"


def test_get_affected_tables_with_edges(table_analyzer, glue_client):
    with patch.object(
        glue_client,
        "get_crawler",
        return_value={
            "Crawler": {
                "Name": "test_crawler",
                "Targets": {"S3Targets": [{"Path": "s3://bucket/path/to/table1"}]},
            }
        },
    ), patch.object(
        glue_client,
        "get_job",
        return_value={
            "Job": {
                "Name": "test_job",
                "OutputDataConfig": {
                    "S3Outputs": [{"S3Uri": "s3://bucket/path/to/table2"}]
                },
            }
        },
    ):
        graph = {
            "Nodes": [
                {"Id": "node1", "Type": "Crawler", "Name": "test_crawler"},
                {"Id": "node2", "Type": "Job", "Name": "test_job"},
            ],
            "Edges": [{"SourceId": "node1", "DestinationId": "node2"}],
        }

        affected_tables = table_analyzer.get_affected_tables(graph, "node1")

        assert set(affected_tables) == {
            "table1",
            "table2",
        }, f"Expected {{'table1', 'table2'}}, but got {set(affected_tables)}"


def test_get_affected_tables_with_api_request_error(
    table_analyzer, glue_client, mocker
):
    mocker.patch.object(
        glue_client, "get_crawler", side_effect=ClientError({"Error": {}}, "GetCrawler")
    )

    graph = {
        "Nodes": [{"Id": "node1", "Type": "Crawler", "Name": "test_crawler"}],
        "Edges": [],
    }

    with pytest.raises(APIRequestError):
        table_analyzer.get_affected_tables(graph, "node1")


def test_get_affected_tables_scenario(table_analyzer, glue_client):
    def mock_get_crawler(Name):
        crawlers = {
            "crawler1": {
                "Crawler": {
                    "Name": "crawler1",
                    "Targets": {"S3Targets": [{"Path": "s3://bucket/path/to/table1"}]},
                }
            },
            "crawler2": {
                "Crawler": {
                    "Name": "crawler2",
                    "Targets": {"S3Targets": [{"Path": "s3://bucket/path/to/table2"}]},
                }
            },
            "crawler3": {
                "Crawler": {
                    "Name": "crawler3",
                    "Targets": {"S3Targets": [{"Path": "s3://bucket/path/to/table3"}]},
                }
            },
        }
        return crawlers[Name]

    def mock_get_job(Name):
        jobs = {
            "job1": {
                "Job": {
                    "Name": "job1",
                    "OutputDataConfig": {
                        "S3Outputs": [{"S3Uri": "s3://bucket/path/to/job_table1"}]
                    },
                }
            },
            "job2": {
                "Job": {
                    "Name": "job2",
                    "OutputDataConfig": {
                        "S3Outputs": [{"S3Uri": "s3://bucket/path/to/job_table2"}]
                    },
                }
            },
            "job3": {
                "Job": {
                    "Name": "job3",
                    "OutputDataConfig": {
                        "S3Outputs": [{"S3Uri": "s3://bucket/path/to/job_table3"}]
                    },
                }
            },
        }
        return jobs[Name]

    with patch.object(
        glue_client, "get_crawler", side_effect=mock_get_crawler
    ), patch.object(glue_client, "get_job", side_effect=mock_get_job):
        graph = {
            "Nodes": [
                {"Id": "trigger1", "Type": "Trigger"},
                {"Id": "job1", "Type": "Job", "Name": "job1"},
                {"Id": "trigger2", "Type": "Trigger"},
                {"Id": "crawler1", "Type": "Crawler", "Name": "crawler1"},
                {"Id": "trigger3", "Type": "Trigger"},
                {"Id": "job2", "Type": "Job", "Name": "job2"},
                {"Id": "trigger4", "Type": "Trigger"},
                {"Id": "crawler2", "Type": "Crawler", "Name": "crawler2"},
                {"Id": "trigger5", "Type": "Trigger"},
                {"Id": "job3", "Type": "Job", "Name": "job3"},
                {"Id": "crawler3", "Type": "Crawler", "Name": "crawler3"},
                {"Id": "trigger6", "Type": "Trigger"},
            ],
            "Edges": [
                {"SourceId": "trigger1", "DestinationId": "job1"},
                {"SourceId": "job1", "DestinationId": "trigger2"},
                {"SourceId": "trigger2", "DestinationId": "crawler1"},
                {"SourceId": "crawler1", "DestinationId": "trigger3"},
                {"SourceId": "trigger3", "DestinationId": "job2"},
                {"SourceId": "job2", "DestinationId": "trigger4"},
                {"SourceId": "trigger4", "DestinationId": "crawler2"},
                {"SourceId": "crawler2", "DestinationId": "trigger5"},
                {"SourceId": "trigger5", "DestinationId": "job3"},
                {"SourceId": "job3", "DestinationId": "trigger6"},
                {"SourceId": "trigger6", "DestinationId": "crawler3"},
            ],
        }

        affected_tables = table_analyzer.get_affected_tables(graph, "crawler2")
        expected_tables = {"table2", "table3", "job_table3"}
        assert (
            set(affected_tables) == expected_tables
        ), f"Expected {expected_tables}, but got {set(affected_tables)}"
