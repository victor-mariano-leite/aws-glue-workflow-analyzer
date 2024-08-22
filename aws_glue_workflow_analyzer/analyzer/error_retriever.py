from botocore.exceptions import ClientError

from aws_glue_workflow_analyzer.exceptions import APIRequestError
from aws_glue_workflow_analyzer.logger import logger
from aws_glue_workflow_analyzer.paginator import paginate_boto3


class ErrorContextRetriever:
    """
    Handles the retrieval of error context from AWS CloudWatch Logs.
    """

    def __init__(self, cloudwatch_logs_client):
        """
        Parameters
        ----------
        cloudwatch_logs_client : boto3.client
            An initialized CloudWatch Logs client.
        """
        self.cloudwatch_logs_client = cloudwatch_logs_client

    def get_error_context(
        self, log_group_name: str, log_stream_name: str, start_time: int, end_time: int
    ) -> str:
        """
        Retrieves error context from CloudWatch logs surrounding failure keywords.

        Parameters
        ----------
        log_group_name : str
            The name of the CloudWatch log group.
        log_stream_name : str
            The name of the CloudWatch log stream.
        start_time : int
            The start time for log retrieval, in milliseconds since epoch.
        end_time : int
            The end time for log retrieval, in milliseconds since epoch.

        Returns
        -------
        str
            The error context string, or a message if no relevant context is found.

        Raises
        ------
        APIRequestError
            If the API request to AWS CloudWatch Logs fails.
        """
        try:
            logger.info(
                f"Fetching error context from logs in group '{log_group_name}', stream '{log_stream_name}'."
            )
            logs = paginate_boto3(
                self.cloudwatch_logs_client.get_log_events,
                dict_key="events",
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                startTime=start_time,
                endTime=end_time,
                limit=1000,
            )

            full_log = ""
            for event in logs:
                full_log += event["message"] + "\n"

            for line in full_log.splitlines():
                if any(keyword in line for keyword in ["error", "exception", "failed"]):
                    context_start = max(0, full_log.index(line) - 100)
                    context_end = min(
                        len(full_log), full_log.index(line) + len(line) + 100
                    )
                    logger.debug(
                        f"Error context found: {full_log[context_start:context_end]}"
                    )
                    return full_log[context_start:context_end]

            logger.info("No relevant error context found in logs.")
            return "No relevant error context found."

        except ClientError as e:
            logger.error(f"Failed to retrieve log events from CloudWatch Logs: {e}")
            raise APIRequestError(
                f"Failed to retrieve log events from CloudWatch Logs: {e}"
            ) from e
