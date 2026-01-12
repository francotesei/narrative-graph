"""Neo4j connection management with connection pooling."""

from contextlib import contextmanager
from typing import Any, Generator

from neo4j import GraphDatabase, Driver, Session, ManagedTransaction
from neo4j.exceptions import ServiceUnavailable, AuthError

from narrative_graph.config import get_settings
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class Neo4jConnection:
    """Neo4j database connection manager with connection pooling."""

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        max_connection_pool_size: int | None = None,
    ) -> None:
        """Initialize Neo4j connection.

        Args:
            uri: Neo4j URI (bolt://localhost:7687)
            user: Database user
            password: Database password
            database: Database name
            max_connection_pool_size: Maximum connections in pool
        """
        settings = get_settings()

        self.uri = uri or settings.neo4j.uri
        self.user = user or settings.neo4j.user
        self.password = password or settings.neo4j.password
        self.database = database or settings.neo4j.database
        self.max_connection_pool_size = (
            max_connection_pool_size or settings.neo4j.max_connection_pool_size
        )

        self._driver: Driver | None = None

    @property
    def driver(self) -> Driver:
        """Get or create the Neo4j driver."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                max_connection_pool_size=self.max_connection_pool_size,
            )
            logger.info("neo4j_driver_created", uri=self.uri, database=self.database)
        return self._driver

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.info("neo4j_driver_closed")

    def verify_connectivity(self) -> bool:
        """Verify database connectivity.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            self.driver.verify_connectivity()
            logger.info("neo4j_connectivity_verified", uri=self.uri)
            return True
        except ServiceUnavailable as e:
            logger.error("neo4j_service_unavailable", error=str(e))
            return False
        except AuthError as e:
            logger.error("neo4j_auth_error", error=str(e))
            return False

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Get a database session as context manager.

        Yields:
            Neo4j session
        """
        session = self.driver.session(database=self.database)
        try:
            yield session
        finally:
            session.close()

    def execute_read(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a read query.

        Args:
            query: Cypher query string
            parameters: Query parameters

        Returns:
            List of result records as dictionaries
        """
        def _read_tx(tx: ManagedTransaction) -> list[dict[str, Any]]:
            result = tx.run(query, parameters or {})
            return [record.data() for record in result]

        with self.session() as session:
            return session.execute_read(_read_tx)

    def execute_write(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a write query.

        Args:
            query: Cypher query string
            parameters: Query parameters

        Returns:
            List of result records as dictionaries
        """
        def _write_tx(tx: ManagedTransaction) -> list[dict[str, Any]]:
            result = tx.run(query, parameters or {})
            return [record.data() for record in result]

        with self.session() as session:
            return session.execute_write(_write_tx)

    def execute_batch_write(
        self, query: str, batch_data: list[dict[str, Any]], batch_key: str = "batch"
    ) -> int:
        """Execute a batch write query using UNWIND.

        Args:
            query: Cypher query with UNWIND $batch_key AS item
            batch_data: List of data items to process
            batch_key: Parameter name for batch data

        Returns:
            Number of items processed
        """
        def _batch_tx(tx: ManagedTransaction) -> int:
            result = tx.run(query, {batch_key: batch_data})
            summary = result.consume()
            return summary.counters.nodes_created + summary.counters.relationships_created

        with self.session() as session:
            return session.execute_write(_batch_tx)

    def init_schema(self) -> None:
        """Initialize database schema with constraints and indexes."""
        constraints = [
            # Unique constraints
            "CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT post_id IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT narrative_id IF NOT EXISTS FOR (n:Narrative) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT domain_name IF NOT EXISTS FOR (d:Domain) REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT hashtag_tag IF NOT EXISTS FOR (h:Hashtag) REQUIRE h.tag IS UNIQUE",
            "CREATE CONSTRAINT entity_name_type IF NOT EXISTS FOR (e:Entity) REQUIRE (e.name, e.type) IS UNIQUE",
        ]

        indexes = [
            # Performance indexes
            "CREATE INDEX post_timestamp IF NOT EXISTS FOR (p:Post) ON (p.timestamp)",
            "CREATE INDEX post_platform IF NOT EXISTS FOR (p:Post) ON (p.platform)",
            "CREATE INDEX narrative_risk_score IF NOT EXISTS FOR (n:Narrative) ON (n.risk_score)",
            "CREATE INDEX author_platform IF NOT EXISTS FOR (a:Author) ON (a.platform)",
        ]

        with self.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.debug("constraint_created", query=constraint[:50])
                except Exception as e:
                    logger.warning("constraint_creation_failed", query=constraint[:50], error=str(e))

            for index in indexes:
                try:
                    session.run(index)
                    logger.debug("index_created", query=index[:50])
                except Exception as e:
                    logger.warning("index_creation_failed", query=index[:50], error=str(e))

        logger.info("neo4j_schema_initialized")

    def clear_database(self) -> None:
        """Clear all data from the database. Use with caution!"""
        query = "MATCH (n) DETACH DELETE n"
        self.execute_write(query)
        logger.warning("neo4j_database_cleared")

    def get_stats(self) -> dict[str, int]:
        """Get database statistics.

        Returns:
            Dictionary with node and relationship counts by type
        """
        stats: dict[str, int] = {}

        # Count nodes by label
        node_query = """
        CALL db.labels() YIELD label
        CALL apoc.cypher.run('MATCH (n:`' + label + '`) RETURN count(n) as count', {}) YIELD value
        RETURN label, value.count as count
        """

        try:
            results = self.execute_read(node_query)
            for record in results:
                stats[f"nodes_{record['label']}"] = record["count"]
        except Exception:
            # Fallback without APOC
            for label in ["Author", "Post", "Narrative", "Domain", "Hashtag", "Entity"]:
                query = f"MATCH (n:{label}) RETURN count(n) as count"
                result = self.execute_read(query)
                if result:
                    stats[f"nodes_{label}"] = result[0]["count"]

        # Count relationships
        rel_query = """
        CALL db.relationshipTypes() YIELD relationshipType
        RETURN relationshipType, count(*) as count
        """

        try:
            results = self.execute_read(rel_query)
            for record in results:
                stats[f"rels_{record['relationshipType']}"] = record["count"]
        except Exception:
            pass

        return stats


# Global connection instance (lazy loaded)
_connection: Neo4jConnection | None = None


def get_neo4j_connection() -> Neo4jConnection:
    """Get the global Neo4j connection instance."""
    global _connection
    if _connection is None:
        _connection = Neo4jConnection()
    return _connection


def reset_neo4j_connection() -> None:
    """Reset the global Neo4j connection (useful for testing)."""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None
