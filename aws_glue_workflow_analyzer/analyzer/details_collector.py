from typing import Any, Dict

from botocore.exceptions import ClientError

from aws_glue_workflow_analyzer.analyzer.error_retriever import ErrorContextRetriever
from aws_glue_workflow_analyzer.analyzer.table_analyzer import TableAnalyzer
from aws_glue_workflow_analyzer.exceptions import APIRequestError
from aws_glue_workflow_analyzer.logger import logger


class StepDetailsCollector:
    """
    Collects detailed information about each step in the workflow.
    """

    def __init__(
        self,
        error_context_retriever: ErrorContextRetriever,
        table_analyzer: TableAnalyzer,
    ):
        """
        Parameters
        ----------
        error_context_retriever : ErrorContextRetriever
            An instance of ErrorContextRetriever to retrieve error context from logs.
        table_analyzer : TableAnalyzer
            An instance of TableAnalyzer to analyze affected tables in the workflow graph.
        """
        self.error_context_retriever = error_context_retriever
        self.table_analyzer = table_analyzer

    def get_step_execution_details(
        self, workflow_name: str, workflow_run: Dict[str, Any], node: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Gathers detailed execution data for a specific step (Crawler, Job, Trigger) in a workflow run.

        Parameters
        ----------
        workflow_name : str
            The name of the workflow being analyzed.
        workflow_run : Dict[str, Any]
            The data of the workflow run.
        node : Dict[str, Any]
            The specific step node in the workflow graph.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing detailed information about the step execution.

        Raises
        ------
        APIRequestError
            If the API request to AWS services fails.
        """
        try:
            logger.info(
                f"Gathering step execution details for workflow: {workflow_name}, node ID: {node['Id']}, node type: {node['Type']}."
            )
            execution_id = workflow_run["RunId"]
            execution_status = node.get("Status", "UNKNOWN")
            execution_start_timestamp = workflow_run.get("StartedOn", "")
            execution_end_timestamp = workflow_run.get("CompletedOn", "")
            duration = (
                (execution_end_timestamp - execution_start_timestamp).total_seconds()
                if execution_start_timestamp and execution_end_timestamp
                else None
            )

            log_group_name = workflow_run.get("LogGroup", "")
            log_stream_name = workflow_run.get("LogStream", "")

            error_message = None
            if (
                log_group_name
                and log_stream_name
                and execution_start_timestamp
                and execution_end_timestamp
            ):
                error_message = self.error_context_retriever.get_error_context(
                    log_group_name,
                    log_stream_name,
                    int(execution_start_timestamp.timestamp() * 1000),
                    int(execution_end_timestamp.timestamp() * 1000),
                )

            affected_tables = self.table_analyzer.get_affected_tables(
                workflow_run["Graph"], node["Id"]
            )

            step_details = {
                "execution_id": execution_id,
                "workflow_name": workflow_name,
                "node_id": node["Id"],
                "node_type": node["Type"],
                "node_name": node["Name"],
                "execution_status": execution_status,
                "execution_start_timestamp": execution_start_timestamp,
                "execution_end_timestamp": execution_end_timestamp,
                "execution_duration": duration,
                "error_message": error_message,
                "affected_tables": affected_tables,
                "log_group_name": log_group_name,
                "log_stream_name": log_stream_name,
                "execution_parameters": workflow_run.get("Arguments", {}),
            }

            logger.debug(f"Step execution details: {step_details}")
            return step_details
        except (ClientError, APIRequestError) as e:
            logger.error(
                f"Failed to gather step execution details for {workflow_name}, node ID: {node['Id']}: {e}"
            )
            raise APIRequestError(
                f"Failed to gather step execution details for {workflow_name}, node ID: {node['Id']}: {e}"
            ) from e
