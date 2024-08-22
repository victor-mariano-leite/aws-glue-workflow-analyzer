import boto3
from botocore.exceptions import (
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
)

from aws_glue_workflow_analyzer.exceptions import CredentialsNotFoundError
from aws_glue_workflow_analyzer.logger import logger


class AWSClientManager:
    """
    Manages the initialization of AWS Glue and CloudWatch Logs clients.
    """

    def __init__(self):
        """
        Initializes the Glue and CloudWatch clients.

        Raises
        ------
        CredentialsNotFoundError
            If AWS credentials are missing or incomplete.
        ConnectionError
            If a connection to AWS services cannot be established.
        """
        try:
            self.glue_client = boto3.client("glue")
            self.cloudwatch_logs_client = boto3.client("logs")
            logger.info("AWS Glue and CloudWatch clients initialized successfully.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error("AWS credentials are missing or incomplete.")
            raise CredentialsNotFoundError() from e
        except EndpointConnectionError as e:
            logger.error("Could not connect to AWS services.")
            raise ConnectionError() from e
