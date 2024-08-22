import argparse


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns
    -------
    argparse.Namespace
        The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        prog="gwfa",
        usage="%(prog)s [options] -w <workflow1> <workflow2> ...",
        description="Analyze AWS Glue Workflows for errors and generate detailed reports.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        prefix_chars="-",
        fromfile_prefix_chars="@",
        argument_default=argparse.SUPPRESS,
        conflict_handler="resolve",
        add_help=True,
        allow_abbrev=True,
    )
    parser.add_argument(
        "-w",
        "--workflows",
        nargs="+",
        required=True,
        help="List of AWS Glue workflows to analyze.",
    )
    parser.add_argument(
        "-d",
        "--days",
        type=int,
        default=30,
        help="Number of days to look back for workflow runs.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=".",
        help="File path to save the analysis results.",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format for the analysis results.",
    )
    return parser.parse_args()
