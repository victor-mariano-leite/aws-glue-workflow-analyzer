import csv
import json
from typing import Any, Dict, List

from aws_glue_workflow_analyzer.logger import logger


def save_to_json(data: List[Dict[str, Any]], file_path: str):
    """
    Saves the analysis results to a JSON file.

    Parameters
    ----------
    data : List[Dict[str, Any]]
        The analysis results to save.
    file_path : str
        The file path where the results should be saved.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as outfile:
            json.dump(data, outfile, indent=4)
        logger.info(f"Analysis results saved to {file_path}")
    except IOError as e:
        logger.error(f"Failed to save analysis results to JSON: {e}")


def save_to_csv(data: List[Dict[str, Any]], file_path: str):
    """
    Saves the analysis results to a CSV file.

    Parameters
    ----------
    data : List[Dict[str, Any]]
        The analysis results to save.
    file_path : str
        The file path where the results should be saved.
    """
    try:
        if data:
            keys = data[0].keys()
            with open(file_path, "w", encoding="utf-8", newline="") as output_file:
                dict_writer = csv.DictWriter(output_file, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)
            logger.info(f"Analysis results saved to {file_path}")
        else:
            logger.warning("No data to save to CSV.")
    except IOError as e:
        logger.error(f"Failed to save analysis results to CSV: {e}")
