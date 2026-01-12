"""SQLite database for run metadata and auxiliary storage."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from narrative_graph.config import get_settings
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class RunDatabase:
    """SQLite database for storing run metadata and pipeline state."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file. Defaults to outputs/runs.db
        """
        if db_path is None:
            settings = get_settings()
            db_path = Path(settings.paths.outputs_dir) / "runs.db"

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn: sqlite3.Connection | None = None
        self._init_schema()

    @property
    def conn(self) -> sqlite3.Connection:
        """Get database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _init_schema(self) -> None:
        """Initialize database schema."""
        schema = """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            config_hash TEXT,
            input_file TEXT,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS run_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            step_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            records_processed INTEGER DEFAULT 0,
            error_message TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS dead_letters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            error_type TEXT NOT NULL,
            error_message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            source_file TEXT,
            line_number INTEGER,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_run_steps_run_id ON run_steps(run_id);
        CREATE INDEX IF NOT EXISTS idx_dead_letters_run_id ON dead_letters(run_id);
        """

        self.conn.executescript(schema)
        self.conn.commit()
        logger.debug("database_schema_initialized", db_path=str(self.db_path))

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # =========================================================================
    # Run Management
    # =========================================================================

    def create_run(
        self,
        run_id: str,
        input_file: str | None = None,
        config_hash: str | None = None,
    ) -> None:
        """Create a new pipeline run record.

        Args:
            run_id: Unique run identifier
            input_file: Path to input data file
            config_hash: Hash of configuration for reproducibility
        """
        self.conn.execute(
            """
            INSERT INTO runs (run_id, started_at, status, config_hash, input_file)
            VALUES (?, ?, 'running', ?, ?)
            """,
            (run_id, datetime.utcnow().isoformat(), config_hash, input_file),
        )
        self.conn.commit()
        logger.info("run_created", run_id=run_id)

    def complete_run(self, run_id: str, status: str = "completed") -> None:
        """Mark a run as completed.

        Args:
            run_id: Run identifier
            status: Final status ('completed', 'failed')
        """
        self.conn.execute(
            """
            UPDATE runs SET completed_at = ?, status = ?
            WHERE run_id = ?
            """,
            (datetime.utcnow().isoformat(), status, run_id),
        )
        self.conn.commit()
        logger.info("run_completed", run_id=run_id, status=status)

    def fail_run(self, run_id: str, error_message: str) -> None:
        """Mark a run as failed.

        Args:
            run_id: Run identifier
            error_message: Error description
        """
        self.conn.execute(
            """
            UPDATE runs SET completed_at = ?, status = 'failed', error_message = ?
            WHERE run_id = ?
            """,
            (datetime.utcnow().isoformat(), error_message, run_id),
        )
        self.conn.commit()
        logger.error("run_failed", run_id=run_id, error=error_message)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Get run information.

        Args:
            run_id: Run identifier

        Returns:
            Run record as dictionary or None
        """
        cursor = self.conn.execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_latest_run(self) -> dict[str, Any] | None:
        """Get the most recent run.

        Returns:
            Latest run record or None
        """
        cursor = self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT 1"
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def list_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        """List recent runs.

        Args:
            limit: Maximum number of runs to return

        Returns:
            List of run records
        """
        cursor = self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # Step Tracking
    # =========================================================================

    def start_step(self, run_id: str, step_name: str) -> int:
        """Record the start of a pipeline step.

        Args:
            run_id: Run identifier
            step_name: Name of the step

        Returns:
            Step record ID
        """
        cursor = self.conn.execute(
            """
            INSERT INTO run_steps (run_id, step_name, started_at, status)
            VALUES (?, ?, ?, 'running')
            """,
            (run_id, step_name, datetime.utcnow().isoformat()),
        )
        self.conn.commit()
        logger.info("step_started", run_id=run_id, step=step_name)
        return cursor.lastrowid or 0

    def complete_step(
        self,
        step_id: int,
        records_processed: int = 0,
        status: str = "completed",
    ) -> None:
        """Mark a step as completed.

        Args:
            step_id: Step record ID
            records_processed: Number of records processed
            status: Final status
        """
        self.conn.execute(
            """
            UPDATE run_steps 
            SET completed_at = ?, status = ?, records_processed = ?
            WHERE id = ?
            """,
            (datetime.utcnow().isoformat(), status, records_processed, step_id),
        )
        self.conn.commit()

    def fail_step(self, step_id: int, error_message: str) -> None:
        """Mark a step as failed.

        Args:
            step_id: Step record ID
            error_message: Error description
        """
        self.conn.execute(
            """
            UPDATE run_steps 
            SET completed_at = ?, status = 'failed', error_message = ?
            WHERE id = ?
            """,
            (datetime.utcnow().isoformat(), error_message, step_id),
        )
        self.conn.commit()

    def get_run_steps(self, run_id: str) -> list[dict[str, Any]]:
        """Get all steps for a run.

        Args:
            run_id: Run identifier

        Returns:
            List of step records
        """
        cursor = self.conn.execute(
            "SELECT * FROM run_steps WHERE run_id = ? ORDER BY started_at",
            (run_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # Dead Letter Queue
    # =========================================================================

    def add_dead_letter(
        self,
        run_id: str,
        raw_payload: str,
        error_type: str,
        error_message: str,
        source_file: str | None = None,
        line_number: int | None = None,
    ) -> None:
        """Add a record to the dead letter queue.

        Args:
            run_id: Run identifier
            raw_payload: Original data as JSON string
            error_type: Type of error
            error_message: Error description
            source_file: Source file path
            line_number: Line number in source file
        """
        self.conn.execute(
            """
            INSERT INTO dead_letters 
            (run_id, raw_payload, error_type, error_message, created_at, source_file, line_number)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                raw_payload,
                error_type,
                error_message,
                datetime.utcnow().isoformat(),
                source_file,
                line_number,
            ),
        )
        self.conn.commit()

    def get_dead_letters(
        self, run_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Get dead letter records.

        Args:
            run_id: Optional run identifier to filter by
            limit: Maximum records to return

        Returns:
            List of dead letter records
        """
        if run_id:
            cursor = self.conn.execute(
                """
                SELECT * FROM dead_letters 
                WHERE run_id = ? 
                ORDER BY created_at DESC LIMIT ?
                """,
                (run_id, limit),
            )
        else:
            cursor = self.conn.execute(
                "SELECT * FROM dead_letters ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        return [dict(row) for row in cursor.fetchall()]

    def count_dead_letters(self, run_id: str) -> int:
        """Count dead letters for a run.

        Args:
            run_id: Run identifier

        Returns:
            Number of dead letter records
        """
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM dead_letters WHERE run_id = ?", (run_id,)
        )
        return cursor.fetchone()[0]
