# AWS Glue Workflow Analyzer

AWS Glue Workflow Analyzer is a Python tool designed to streamline the analysis and debugging of AWS Glue workflows. By leveraging AWS services such as Glue and CloudWatch Logs, this tool provides detailed insights into workflow executions, helping you identify and troubleshoot errors efficiently.

## Table of Contents

- [AWS Glue Workflow Analyzer](#aws-glue-workflow-analyzer)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Install from PyPI](#install-from-pypi)
    - [Install from Source](#install-from-source)
  - [Usage](#usage)
    - [Example](#example)
  - [Command-Line Interface](#command-line-interface)
    - [Options](#options)
    - [Help Command](#help-command)
  - [Configuration](#configuration)
    - [AWS Credentials](#aws-credentials)
    - [Logging Configuration](#logging-configuration)
  - [Logging](#logging)
  - [Error Handling](#error-handling)
  - [Testing and Pre-Commit Hooks](#testing-and-pre-commit-hooks)
    - [Unit Tests](#unit-tests)
    - [Pre-Commit Hooks](#pre-commit-hooks)
    - [Setting Up Pre-Commit Hooks](#setting-up-pre-commit-hooks)
  - [Contributing](#contributing)
  - [License](#license)
  - [Acknowledgments](#acknowledgments)

## Features

- **AWS Client Management**: Initialize and manage AWS Glue and CloudWatch Logs clients with robust error handling.
- **Step Details Collection**: Gather detailed execution data for each step in a workflow, including errors and affected tables.
- **Error Context Retrieval**: Retrieve relevant error logs from CloudWatch, pinpointing the root cause of failures.
- **Workflow Run Retrieval**: Fetch and filter workflow runs from AWS Glue within a specified time range.
- **Table Analysis**: Identify tables affected by workflow failures using a depth-first search (DFS) on the workflow graph.
- **Output Management**: Save analysis results in JSON or CSV format for easy sharing and review.
- **Rich Logging**: Enhanced logging with the Rich library for better readability and debugging.
- **Command-Line Interface (CLI)**: Easy-to-use CLI for analyzing workflows and generating reports.

## Installation

### Prerequisites

- Python 3.7 or higher
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)

### Install from PyPI

```bash
pip install aws-glue-workflow-analyzer
```

### Install from Source

Clone the repository and install the dependencies:

```bash
git clone https://github.com/victor-mariano-leite/aws_glue_workflow_analyzer.git
cd aws_glue_workflow_analyzer
pip install .
```

## Usage

The AWS Glue Workflow Analyzer can be used to analyze workflows, identify errors, and generate detailed reports. You can run the tool directly from the command line.

### Example

```bash
gwfa -w my-glue-workflow -d 7 -o output.json -f json
```

This command analyzes the `my-glue-workflow` for the past 7 days, saving the results in JSON format to `output.json`.

## Command-Line Interface

The CLI provides a simple interface to interact with the AWS Glue Workflow Analyzer.

### Options

- `-w`, `--workflows`: List of AWS Glue workflows to analyze (required).
- `-d`, `--days`: Number of days to look back for workflow runs (default: 30).
- `-o`, `--output`: File path to save the analysis results.
- `-f`, `--format`: Output format (`json` or `csv`, default: `json`).

### Help Command

For detailed help, use:

```bash
gwfa --help
```

## Configuration

### AWS Credentials

Ensure that your AWS credentials are properly configured. The tool will use these credentials to access AWS Glue and CloudWatch Logs.

Credentials can be set up via:

- **AWS CLI**: `aws configure`
- **Environment Variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- **IAM Roles**: If running on an EC2 instance or another AWS service with an assigned role.

### Logging Configuration

You can adjust the logging level using the `LOG_LEVEL` environment variable:

```bash
export LOG_LEVEL=DEBUG
```

By default, the log level is set to `INFO`.

## Logging

The tool uses the Rich library to enhance log readability. Logs include timestamps, paths, and local variables for better traceability. Logs are essential for understanding the tool's operations, especially in production environments with limited access to real-time data.

## Error Handling

The AWS Glue Workflow Analyzer includes robust error handling:

- **CredentialsNotFoundError**: Raised when AWS credentials are missing or incomplete.
- **APIRequestError**: Raised when an API request to AWS services fails.
- **WorkflowConnectionError**: Raised when a connection to AWS services cannot be established.

These custom exceptions ensure that errors are clearly communicated, making it easier to diagnose and fix issues.

## Testing and Pre-Commit Hooks

### Unit Tests

The project includes comprehensive unit tests to ensure reliability and correctness. Tests are located in the `tests` directory and are executed using `pytest`.

To run the tests:

```bash
make test
```

For coverage reports:

```bash
make test-cov
```

### Pre-Commit Hooks

To maintain code quality, the project uses several pre-commit hooks configured via `.pre-commit-config.yaml`. These hooks include:

- **isort**: Sorts imports according to PEP8 standards.
- **black**: Formats code to adhere to Python's Black style guide.
- **pylint**: Lints the code to catch potential errors and enforce coding standards.
- **flake8**: Checks the code for style issues and logical errors.
- **mypy**: Performs static type checking.
- **pytest**: Runs unit tests with coverage reports.

### Setting Up Pre-Commit Hooks

To set up the pre-commit hooks:

```bash
pip install pre-commit
pre-commit install
```

Now, every time you commit changes, these hooks will run automatically, ensuring that your code adheres to the project's standards.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m 'Add your feature'`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a pull request.

Please ensure your code adheres to the existing code style and includes appropriate tests. The pre-commit hooks and unit tests must pass before your pull request can be merged.

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/victor-mariano-leite/aws_glue_workflow_analyzer/blob/main/LICENSE) file for details.

## Acknowledgments

- **Boto3**: For providing an excellent Python interface to AWS services.
- **Rich**: For making logging beautiful and more informative.
- **The Open Source Community**: For their continuous contributions that make projects like this possible.