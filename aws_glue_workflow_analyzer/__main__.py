from aws_glue_workflow_analyzer.analyzer.workflow import GlueWorkflowAnalyzer
from aws_glue_workflow_analyzer.cli import parse_args
from aws_glue_workflow_analyzer.exceptions import WorkflowAnalyzerError
from aws_glue_workflow_analyzer.logger import console, logger
from aws_glue_workflow_analyzer.output import save_to_csv, save_to_json


def main():
    """
    Main function to run the GlueWorkflowAnalyzer.

    Raises
    ------
    WorkflowAnalyzerError
        If an error occurs during workflow analysis.
    """
    args = parse_args()
    try:
        analyzer = GlueWorkflowAnalyzer()
        analysis_results = analyzer.analyze_workflows(args.workflows, args.days)
        if args.output:
            if args.format == "json":
                save_to_json(analysis_results, args.output)
            elif args.format == "csv":
                save_to_csv(analysis_results, args.output)
        else:
            for result in analysis_results:
                console.print_json(data=result)
    except WorkflowAnalyzerError as e:
        logger.error(f"An error occurred during workflow analysis: {e}")


if __name__ == "__main__":
    main()
