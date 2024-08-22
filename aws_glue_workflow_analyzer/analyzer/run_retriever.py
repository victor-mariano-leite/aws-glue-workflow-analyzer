import datetime
from typing import Any, Dict, List

from botocore.exceptions import ClientError

from aws_glue_workflow_analyzer.exceptions import APIRequestError
from aws_glue_workflow_analyzer.logger import logger
from aws_glue_workflow_analyzer.paginator import paginate_boto3


class WorkflowRunRetriever:
    """
    Handles the retrieval of workflow runs from AWS Glue.
    """

    def __init__(self, glue_client):
        """
        Parameters
        ----------
        glue_client : boto3.client
            An initialized Glue client.
        """
        self.glue_client = glue_client

    def get_workflow_runs(
        self, workflow_name: str, days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Retrieves all workflow runs within the last specified number of days.

        Parameters
        ----------
        workflow_name : str
            The name of the Glue workflow to analyze.
        days : int, optional
            The number of days to look back for workflow runs, by default 30.

        Returns
        -------
        List[Dict[str, Any]]
            A list of workflow runs within the specified time period.

        Raises
        ------
        APIRequestError
            If the API request to AWS Glue fails.
        """
        try:
            logger.info(
                f"Fetching workflow runs for '{workflow_name}' for the past {days} days."
            )
            start_from = datetime.datetime.now() - datetime.timedelta(days=days)

            workflow_runs = paginate_boto3(
                self.glue_client.get_workflow_runs,
                dict_key="Runs",
                Name=workflow_name,
                IncludeGraph=True,
                MaxResults=100,
            )

            filtered_runs = [
                run
                for run in workflow_runs
                if run.get("StartedOn") and run["StartedOn"] >= start_from
            ]

            logger.debug(
                f"Retrieved {len(filtered_runs)} filtered runs for workflow '{workflow_name}'."
            )
            return filtered_runs

        except ClientError as e:
            logger.error(f"Failed to retrieve workflow runs for {workflow_name}: {e}")
            raise APIRequestError(
                f"Failed to retrieve workflow runs for {workflow_name}: {e}"
            ) from e
