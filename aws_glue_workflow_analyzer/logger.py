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
    console = Console()
    logger = logging.getLogger("workflow_analyzer")
    logger.propagate = False

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(log_level)

    rich_handler = RichHandler(
        console=console, show_time=True, show_path=True, tracebacks_show_locals=True
    )
    logger.addHandler(rich_handler)

    return logger, console


logger, console = set_logger()
