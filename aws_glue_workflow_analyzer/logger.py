import logging
import os

from rich.console import Console
from rich.logging import RichHandler


def set_logger():
    """
    Sets up the logger for the workflow analyzer.

    Returns
    -------
    logging.Logger
        The logger instance.
    rich.console.Console
        The console instance.
    """
    rich_console = Console()
    rich_logger = logging.getLogger("workflow_analyzer")
    rich_logger.propagate = False

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    rich_logger.setLevel(log_level)

    rich_handler = RichHandler(
        console=rich_console,
        show_time=True,
        show_path=True,
        tracebacks_show_locals=True,
    )
    rich_logger.addHandler(rich_handler)

    return rich_logger, rich_console


logger, console = set_logger()
