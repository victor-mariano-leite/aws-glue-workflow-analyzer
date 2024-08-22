from typing import Any, Dict, List

from botocore.exceptions import ClientError

from aws_glue_workflow_analyzer.analyzer.aws_client import AWSClientManager
from aws_glue_workflow_analyzer.analyzer.details_collector import StepDetailsCollector
from aws_glue_workflow_analyzer.analyzer.error_retriever import ErrorContextRetriever
from aws_glue_workflow_analyzer.analyzer.run_retriever import WorkflowRunRetriever
from aws_glue_workflow_analyzer.analyzer.table_analyzer import TableAnalyzer
from aws_glue_workflow_analyzer.exceptions import APIRequestError
from aws_glue_workflow_analyzer.logger import logger


class GlueWorkflowAnalyzer:
    """
    Coordinates the analysis of AWS Glue workflows, retrieving and processing details of each step.
    """

    def __init__(self):
        """
        Initializes the GlueWorkflowAnalyzer with AWS clients and auxiliary classes.
        """
        self.client_manager = AWSClientManager()
        self.run_retriever = WorkflowRunRetriever(self.client_manager.glue_client)
        self.error_context_retriever = ErrorContextRetriever(
            self.client_manager.cloudwatch_logs_client
        )
        self.table_analyzer = TableAnalyzer(self.client_manager.glue_client)
        self.step_details_collector = StepDetailsCollector(
            self.error_context_retriever, self.table_analyzer
        )

    def analyze_workflows(
        self, workflow_names: List[str], days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Analyzes multiple workflows, gathering step-level execution details for each.

        Parameters
        ----------
        workflow_names : List[str]
            A list of workflow names to analyze.
        days : int, optional
            The number of days to look back for workflow runs, by default 30.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries, each containing details of a step execution.

        Raises
        ------
        APIRequestError
            If the API request to AWS services fails.
        """
        try:
            logger.info(
                f"Analyzing workflows: {workflow_names} for the past {days} days."
            )
            all_step_data = []

            for workflow_name in workflow_names:
                workflow_runs = self.run_retriever.get_workflow_runs(
                    workflow_name, days
                )

                for workflow_run in workflow_runs:
                    for node in workflow_run["Graph"]["Nodes"]:
                        step_data = (
                            self.step_details_collector.get_step_execution_details(
                                workflow_name, workflow_run, node
                            )
                        )
                        all_step_data.append(step_data)

            logger.info("Workflow step analysis completed successfully.")
            return all_step_data
        except (ClientError, APIRequestError) as e:
            logger.error(f"Failed to analyze workflows: {e}")
            raise APIRequestError(f"Failed to analyze workflows: {e}") from e
