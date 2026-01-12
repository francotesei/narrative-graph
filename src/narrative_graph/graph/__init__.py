"""Graph module for Neo4j operations."""

from narrative_graph.graph.connection import Neo4jConnection, get_neo4j_connection

__all__ = ["Neo4jConnection", "get_neo4j_connection"]
