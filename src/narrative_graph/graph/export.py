"""Graph export functionality."""

import json
from pathlib import Path
from typing import Any

from narrative_graph.graph.connection import Neo4jConnection, get_neo4j_connection
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class GraphExporter:
    """Export graph data from Neo4j."""

    def __init__(self, connection: Neo4jConnection | None = None):
        """Initialize exporter.

        Args:
            connection: Neo4j connection instance
        """
        self.conn = connection or get_neo4j_connection()

    def export_narrative_subgraph(
        self, narrative_id: str, output_path: str | Path | None = None
    ) -> dict[str, Any]:
        """Export subgraph for a specific narrative.

        Args:
            narrative_id: Narrative identifier
            output_path: Optional path to save JSON output

        Returns:
            Dictionary with nodes and edges
        """
        # Get all nodes related to the narrative
        query = """
        MATCH (n:Narrative {id: $narrative_id})
        OPTIONAL MATCH (p:Post)-[:BELONGS_TO]->(n)
        OPTIONAL MATCH (a:Author)-[:POSTED]->(p)
        OPTIONAL MATCH (p)-[:LINKS_TO]->(d:Domain)
        OPTIONAL MATCH (p)-[:TAGGED_WITH]->(h:Hashtag)
        OPTIONAL MATCH (p)-[:MENTIONS]->(e:Entity)
        WITH collect(DISTINCT {
            id: id(n), 
            labels: labels(n), 
            properties: properties(n)
        }) + collect(DISTINCT {
            id: id(p), 
            labels: labels(p), 
            properties: {id: p.id, platform: p.platform, timestamp: toString(p.timestamp)}
        }) + collect(DISTINCT {
            id: id(a), 
            labels: labels(a), 
            properties: properties(a)
        }) + collect(DISTINCT {
            id: id(d), 
            labels: labels(d), 
            properties: properties(d)
        }) + collect(DISTINCT {
            id: id(h), 
            labels: labels(h), 
            properties: properties(h)
        }) + collect(DISTINCT {
            id: id(e), 
            labels: labels(e), 
            properties: properties(e)
        }) as all_nodes
        UNWIND all_nodes as node
        WITH collect(DISTINCT node) as nodes
        RETURN nodes
        """

        nodes_result = self.conn.execute_read(query, {"narrative_id": narrative_id})
        nodes = nodes_result[0]["nodes"] if nodes_result else []

        # Filter out null nodes
        nodes = [n for n in nodes if n.get("id") is not None]

        # Get relationships
        edge_query = """
        MATCH (n:Narrative {id: $narrative_id})
        OPTIONAL MATCH (p:Post)-[r1:BELONGS_TO]->(n)
        OPTIONAL MATCH (a:Author)-[r2:POSTED]->(p)
        OPTIONAL MATCH (p)-[r3:LINKS_TO]->(d:Domain)
        OPTIONAL MATCH (p)-[r4:TAGGED_WITH]->(h:Hashtag)
        OPTIONAL MATCH (p)-[r5:MENTIONS]->(e:Entity)
        WITH collect(DISTINCT {source: id(p), target: id(n), type: 'BELONGS_TO'}) +
             collect(DISTINCT {source: id(a), target: id(p), type: 'POSTED'}) +
             collect(DISTINCT {source: id(p), target: id(d), type: 'LINKS_TO'}) +
             collect(DISTINCT {source: id(p), target: id(h), type: 'TAGGED_WITH'}) +
             collect(DISTINCT {source: id(p), target: id(e), type: 'MENTIONS'}) as all_edges
        UNWIND all_edges as edge
        WITH collect(DISTINCT edge) as edges
        RETURN edges
        """

        edges_result = self.conn.execute_read(edge_query, {"narrative_id": narrative_id})
        edges = edges_result[0]["edges"] if edges_result else []

        # Filter out edges with null nodes
        edges = [e for e in edges if e.get("source") is not None and e.get("target") is not None]

        graph_data = {
            "narrative_id": narrative_id,
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
        }

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(graph_data, f, indent=2, default=str)
            logger.info("subgraph_exported", path=str(output_path))

        return graph_data

    def export_full_graph(self, output_path: str | Path) -> dict[str, int]:
        """Export full graph to JSON.

        Args:
            output_path: Path to save JSON output

        Returns:
            Dictionary with export statistics
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Export nodes by type
        all_nodes = []
        node_counts = {}

        for label in ["Author", "Post", "Narrative", "Domain", "Hashtag", "Entity"]:
            query = f"""
            MATCH (n:{label})
            RETURN id(n) as id, labels(n) as labels, properties(n) as properties
            """
            result = self.conn.execute_read(query)
            nodes = [
                {
                    "id": r["id"],
                    "labels": r["labels"],
                    "properties": {
                        k: str(v) if not isinstance(v, (str, int, float, bool, list)) else v
                        for k, v in (r["properties"] or {}).items()
                    },
                }
                for r in result
            ]
            all_nodes.extend(nodes)
            node_counts[label] = len(nodes)

        # Export relationships
        all_edges = []
        edge_counts = {}

        for rel_type in [
            "POSTED",
            "BELONGS_TO",
            "LINKS_TO",
            "TAGGED_WITH",
            "MENTIONS",
            "COORDINATED_WITH",
        ]:
            query = f"""
            MATCH (a)-[r:{rel_type}]->(b)
            RETURN id(a) as source, id(b) as target, type(r) as type, properties(r) as properties
            """
            result = self.conn.execute_read(query)
            edges = [
                {
                    "source": r["source"],
                    "target": r["target"],
                    "type": r["type"],
                    "properties": r["properties"] or {},
                }
                for r in result
            ]
            all_edges.extend(edges)
            edge_counts[rel_type] = len(edges)

        graph_data = {
            "nodes": all_nodes,
            "edges": all_edges,
            "metadata": {
                "node_counts": node_counts,
                "edge_counts": edge_counts,
                "total_nodes": len(all_nodes),
                "total_edges": len(all_edges),
            },
        }

        with open(output_path, "w") as f:
            json.dump(graph_data, f, default=str)

        logger.info(
            "full_graph_exported",
            path=str(output_path),
            nodes=len(all_nodes),
            edges=len(all_edges),
        )

        return graph_data["metadata"]

    def export_to_graphml(self, output_path: str | Path) -> None:
        """Export graph to GraphML format.

        Args:
            output_path: Path to save GraphML file
        """
        import xml.etree.ElementTree as ET

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Create GraphML structure
        graphml = ET.Element("graphml")
        graphml.set("xmlns", "http://graphml.graphdrawing.org/xmlns")

        # Add key definitions
        keys = [
            ("label", "node", "string"),
            ("type", "edge", "string"),
            ("name", "node", "string"),
        ]

        for key_id, for_type, key_type in keys:
            key_elem = ET.SubElement(graphml, "key")
            key_elem.set("id", key_id)
            key_elem.set("for", for_type)
            key_elem.set("attr.name", key_id)
            key_elem.set("attr.type", key_type)

        graph = ET.SubElement(graphml, "graph")
        graph.set("id", "narrative_graph")
        graph.set("edgedefault", "directed")

        # Add nodes
        for label in ["Author", "Post", "Narrative", "Domain", "Hashtag"]:
            query = f"MATCH (n:{label}) RETURN id(n) as id, n.id as name, '{label}' as label"
            result = self.conn.execute_read(query)

            for record in result:
                node = ET.SubElement(graph, "node")
                node.set("id", str(record["id"]))

                label_data = ET.SubElement(node, "data")
                label_data.set("key", "label")
                label_data.text = record["label"]

                if record.get("name"):
                    name_data = ET.SubElement(node, "data")
                    name_data.set("key", "name")
                    name_data.text = str(record["name"])

        # Add edges
        edge_id = 0
        for rel_type in ["POSTED", "BELONGS_TO", "LINKS_TO", "TAGGED_WITH"]:
            query = f"MATCH (a)-[r:{rel_type}]->(b) RETURN id(a) as source, id(b) as target"
            result = self.conn.execute_read(query)

            for record in result:
                edge = ET.SubElement(graph, "edge")
                edge.set("id", f"e{edge_id}")
                edge.set("source", str(record["source"]))
                edge.set("target", str(record["target"]))

                type_data = ET.SubElement(edge, "data")
                type_data.set("key", "type")
                type_data.text = rel_type

                edge_id += 1

        # Write to file
        tree = ET.ElementTree(graphml)
        tree.write(output_path, encoding="utf-8", xml_declaration=True)

        logger.info("graphml_exported", path=str(output_path))
