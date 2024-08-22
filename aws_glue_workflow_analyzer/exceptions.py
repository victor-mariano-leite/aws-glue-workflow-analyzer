class WorkflowAnalyzerError(Exception):
    """
    Base class for exceptions in this module.
    """


class CredentialsNotFoundError(WorkflowAnalyzerError):
    """
    Raised when AWS credentials are missing or incomplete.
    """

    def __init__(
        self,
        message="AWS credentials not found. Please configure your AWS credentials.",
    ):
        self.message = message
        super().__init__(self.message)


class APIRequestError(WorkflowAnalyzerError):
    """
    Raised when an API request fails.
    """

    def __init__(
        self, message="An error occurred while making an API request to AWS services."
    ):
        self.message = message
        super().__init__(self.message)


class WorkflowConnectionError(WorkflowAnalyzerError):
    """
    Raised when a connection to AWS services cannot be established.
    """

    def __init__(
        self,
        message="Could not connect to AWS services. Please check your network connection and AWS endpoint configurations.",
    ):
        self.message = message
        super().__init__(self.message)
