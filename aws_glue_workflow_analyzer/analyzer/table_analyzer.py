from typing import Any, Dict, List

from botocore.exceptions import ClientError

from aws_glue_workflow_analyzer.exceptions import APIRequestError


class TableAnalyzer:
    """
    Analyzes the workflow graph to determine affected tables.
    """

    def __init__(self, glue_client):
        """
        Parameters
        ----------
        glue_client : boto3.client
            An initialized Glue client.
        """
        self.glue_client = glue_client

    def get_affected_tables(
        self, graph: Dict[str, Any], failure_node_id: str
    ) -> List[str]:
        """
        Identifies tables affected by a failure in the workflow by analyzing downstream nodes.

        Parameters
        ----------
        graph : Dict[str, Any]
            The workflow execution graph.
        failure_node_id : str
            The ID of the failure node in the graph.

        Returns
        -------
        List[str]
            A list of tables affected by the failure.

        Raises
        ------
        APIRequestError
            If the API request to AWS Glue fails.
        """
        try:
            failed_nodes = self._dfs_collect_failed_nodes(graph, failure_node_id)
            affected_tables = self._extract_tables_from_nodes(graph, failed_nodes)
            return list(affected_tables)
        except ClientError as e:
            raise APIRequestError(f"Failed to retrieve affected tables: {e}") from e

    def _dfs_collect_failed_nodes(
        self, graph: Dict[str, Any], failure_node_id: str
    ) -> set:
        """
        Performs DFS to collect all nodes affected by a failure, including the failure node itself.

        Parameters
        ----------
        graph : Dict[str, Any]
            The workflow execution graph.
        failure_node_id : str
            The ID of the failure node in the graph.

        Returns
        -------
        set
            A set of node IDs affected by the failure.
        """
        failed_nodes = set()
        stack = [failure_node_id]

        while stack:
            current_node_id = stack.pop()
            if current_node_id not in failed_nodes:
                failed_nodes.add(current_node_id)
                for edge in graph.get("Edges", []):
                    if edge["SourceId"] == current_node_id:
                        stack.append(edge["DestinationId"])
                        print(
                            f"Traversing from {current_node_id} to {edge['DestinationId']}"
                        )

        print(f"Failed nodes collected: {failed_nodes}")
        return failed_nodes

    def _extract_tables_from_nodes(self, graph: Dict[str, Any], node_ids: set) -> set:
        """
        Extracts tables from the set of node IDs.

        Parameters
        ----------
        graph : Dict[str, Any]
            The workflow execution graph.
        node_ids : set
            A set of node IDs from which to extract tables.

        Returns
        -------
        set
            A set of table names extracted from the nodes.
        """
        affected_tables = set()
        for node_id in node_ids:
            node = self._get_node_by_id(graph, node_id)
            if node:  # Ensure the node exists in the graph
                tables_from_node = self._get_tables_for_node(node)
                affected_tables.update(tables_from_node)
                print(f"Extracted tables from node {node_id}: {tables_from_node}")
        return affected_tables

    def _get_node_by_id(self, graph: Dict[str, Any], node_id: str) -> Dict[str, Any]:
        """
        Retrieves a node from the workflow graph by its ID.

        Parameters
        ----------
        graph : Dict[str, Any]
            The workflow execution graph.
        node_id : str
            The ID of the node to retrieve.

        Returns
        -------
        Dict[str, Any]
            The node data.
        """
        return next(node for node in graph["Nodes"] if node["Id"] == node_id)

    def _get_tables_for_node(self, node: Dict[str, Any]) -> set:
        """
        Retrieves the tables affected by a specific node in the workflow.

        Parameters
        ----------
        node : Dict[str, Any]
            The node from which to retrieve the tables.

        Returns
        -------
        set
            A set of table names affected by the node.
        """
        affected_tables = set()
        if node["Type"] == "Crawler":
            affected_tables.update(self._get_tables_from_crawler(node["Name"]))
        elif node["Type"] == "Job":
            affected_tables.update(self._get_tables_from_job(node["Name"]))
        print(f"Extracted tables from node {node['Id']}: {affected_tables}")
        return affected_tables

    def _get_tables_from_crawler(self, crawler_name: str) -> set:
        """
        Retrieves the tables affected by a specific Glue Crawler.

        Parameters
        ----------
        crawler_name : str
            The name of the crawler.

        Returns
        -------
        set
            A set of table names affected by the crawler.
        """
        tables = set()
        crawler = self.glue_client.get_crawler(Name=crawler_name)["Crawler"]
        if "Targets" in crawler:
            for target in crawler["Targets"]["S3Targets"]:
                path = target["Path"]
                table_name = path.split("/")[-1]
                tables.add(table_name)
        return tables

    def _get_tables_from_job(self, job_name: str) -> set:
        """
        Retrieves the tables affected by a specific Glue Job.

        Parameters
        ----------
        job_name : str
            The name of the job.

        Returns
        -------
        set
            A set of table names affected by the job.
        """
        tables = set()
        job = self.glue_client.get_job(Name=job_name)["Job"]
        if "OutputDataConfig" in job:
            for output in job["OutputDataConfig"]["S3Outputs"]:
                path = output["S3Uri"]
                table_name = path.split("/")[-1]
                tables.add(table_name)
        return tables
