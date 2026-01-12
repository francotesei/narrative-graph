"""Parquet storage for datasets."""

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from narrative_graph.config import get_settings
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


def _serialize_complex_fields(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert complex fields (dicts) to JSON strings for Parquet compatibility.

    Args:
        records: List of record dictionaries

    Returns:
        Records with complex fields serialized
    """
    if not records:
        return records

    serialized = []
    for record in records:
        new_record = {}
        for key, value in record.items():
            if isinstance(value, dict):
                # Convert dict to JSON string (Parquet can't handle empty structs)
                new_record[key] = json.dumps(value) if value else None
            else:
                new_record[key] = value
        serialized.append(new_record)
    return serialized


def _deserialize_complex_fields(records: list[dict[str, Any]], json_fields: list[str] | None = None) -> list[dict[str, Any]]:
    """Convert JSON string fields back to dicts.

    Args:
        records: List of record dictionaries
        json_fields: List of field names that should be deserialized

    Returns:
        Records with JSON fields deserialized
    """
    if not records:
        return records

    # Known fields that should be dicts (even if None)
    known_dict_fields = {"metadata", "facts_used", "components"}

    # Auto-detect JSON fields if not specified
    if json_fields is None:
        json_fields = list(known_dict_fields)
        if records:
            for key, value in records[0].items():
                if key not in json_fields and isinstance(value, str) and value.startswith("{"):
                    try:
                        json.loads(value)
                        json_fields.append(key)
                    except (json.JSONDecodeError, TypeError):
                        pass

    deserialized = []
    for record in records:
        new_record = dict(record)
        for field in json_fields:
            if field in new_record:
                value = new_record[field]
                if isinstance(value, str):
                    try:
                        new_record[field] = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        pass
                elif value is None:
                    new_record[field] = {}
        deserialized.append(new_record)
    return deserialized


class ParquetStorage:
    """Parquet file storage for datasets."""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        """Initialize Parquet storage.

        Args:
            base_dir: Base directory for parquet files. Defaults to outputs/
        """
        if base_dir is None:
            settings = get_settings()
            base_dir = settings.paths.outputs_dir

        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, name: str, run_id: str | None = None) -> Path:
        """Get full path for a dataset.

        Args:
            name: Dataset name
            run_id: Optional run ID for versioning

        Returns:
            Full path to parquet file
        """
        if run_id:
            return self.base_dir / run_id / f"{name}.parquet"
        return self.base_dir / f"{name}.parquet"

    def save_dataframe(
        self,
        df: pd.DataFrame,
        name: str,
        run_id: str | None = None,
        compression: str = "snappy",
    ) -> Path:
        """Save a DataFrame to parquet.

        Args:
            df: DataFrame to save
            name: Dataset name
            run_id: Optional run ID for versioning
            compression: Compression codec

        Returns:
            Path to saved file
        """
        path = self._get_path(name, run_id)
        path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(path, compression=compression, index=False)
        logger.info(
            "parquet_saved",
            path=str(path),
            rows=len(df),
            columns=list(df.columns),
        )
        return path

    def load_dataframe(
        self,
        name: str,
        run_id: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load a DataFrame from parquet.

        Args:
            name: Dataset name
            run_id: Optional run ID for versioning
            columns: Optional list of columns to load

        Returns:
            Loaded DataFrame
        """
        path = self._get_path(name, run_id)

        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        df = pd.read_parquet(path, columns=columns)
        logger.debug("parquet_loaded", path=str(path), rows=len(df))
        return df

    def exists(self, name: str, run_id: str | None = None) -> bool:
        """Check if a dataset exists.

        Args:
            name: Dataset name
            run_id: Optional run ID

        Returns:
            True if file exists
        """
        return self._get_path(name, run_id).exists()

    def save_records(
        self,
        records: list[dict[str, Any]],
        name: str,
        run_id: str | None = None,
        compression: str = "snappy",
    ) -> Path:
        """Save a list of records to parquet.

        Args:
            records: List of dictionaries
            name: Dataset name
            run_id: Optional run ID
            compression: Compression codec

        Returns:
            Path to saved file
        """
        # Serialize complex fields (dicts) to JSON strings
        serialized_records = _serialize_complex_fields(records)
        df = pd.DataFrame(serialized_records)
        return self.save_dataframe(df, name, run_id, compression)

    def load_records(
        self,
        name: str,
        run_id: str | None = None,
        json_fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Load records from parquet.

        Args:
            name: Dataset name
            run_id: Optional run ID
            json_fields: List of field names to deserialize from JSON

        Returns:
            List of record dictionaries
        """
        df = self.load_dataframe(name, run_id)
        records = df.to_dict(orient="records")
        # Deserialize JSON string fields back to dicts
        return _deserialize_complex_fields(records, json_fields)

    def append_records(
        self,
        records: list[dict[str, Any]],
        name: str,
        run_id: str | None = None,
    ) -> Path:
        """Append records to existing parquet file.

        Args:
            records: Records to append
            name: Dataset name
            run_id: Optional run ID

        Returns:
            Path to file
        """
        path = self._get_path(name, run_id)

        if path.exists():
            existing_df = pd.read_parquet(path)
            new_df = pd.DataFrame(records)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = pd.DataFrame(records)

        return self.save_dataframe(combined_df, name, run_id)

    def get_schema(self, name: str, run_id: str | None = None) -> pa.Schema:
        """Get the schema of a parquet file.

        Args:
            name: Dataset name
            run_id: Optional run ID

        Returns:
            PyArrow schema
        """
        path = self._get_path(name, run_id)
        return pq.read_schema(path)

    def get_row_count(self, name: str, run_id: str | None = None) -> int:
        """Get row count without loading entire file.

        Args:
            name: Dataset name
            run_id: Optional run ID

        Returns:
            Number of rows
        """
        path = self._get_path(name, run_id)
        parquet_file = pq.ParquetFile(path)
        return parquet_file.metadata.num_rows

    def list_datasets(self, run_id: str | None = None) -> list[str]:
        """List available datasets.

        Args:
            run_id: Optional run ID

        Returns:
            List of dataset names
        """
        if run_id:
            search_dir = self.base_dir / run_id
        else:
            search_dir = self.base_dir

        if not search_dir.exists():
            return []

        return [
            p.stem for p in search_dir.glob("*.parquet")
        ]

    def delete_dataset(self, name: str, run_id: str | None = None) -> bool:
        """Delete a dataset.

        Args:
            name: Dataset name
            run_id: Optional run ID

        Returns:
            True if deleted, False if not found
        """
        path = self._get_path(name, run_id)
        if path.exists():
            path.unlink()
            logger.info("parquet_deleted", path=str(path))
            return True
        return False
