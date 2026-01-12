"""Data loaders for JSONL and CSV files."""

import csv
import json
from pathlib import Path
from typing import Any, Generator

from narrative_graph.ingestion.schemas import RawPost
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


def load_jsonl(
    file_path: str | Path,
    skip_errors: bool = True,
) -> Generator[tuple[int, dict[str, Any] | None, str | None], None, None]:
    """Load records from a JSONL file.

    Args:
        file_path: Path to JSONL file
        skip_errors: If True, yield error info instead of raising

    Yields:
        Tuples of (line_number, record_dict or None, error_message or None)
    """
    file_path = Path(file_path)
    logger.info("loading_jsonl", path=str(file_path))

    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
                yield (line_num, record, None)
            except json.JSONDecodeError as e:
                if skip_errors:
                    yield (line_num, None, f"JSON decode error: {e}")
                else:
                    raise


def load_csv(
    file_path: str | Path,
    skip_errors: bool = True,
) -> Generator[tuple[int, dict[str, Any] | None, str | None], None, None]:
    """Load records from a CSV file.

    Args:
        file_path: Path to CSV file
        skip_errors: If True, yield error info instead of raising

    Yields:
        Tuples of (line_number, record_dict or None, error_message or None)
    """
    file_path = Path(file_path)
    logger.info("loading_csv", path=str(file_path))

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for line_num, row in enumerate(reader, start=2):  # Start at 2 (header is line 1)
            try:
                # Parse JSON fields if present
                record = dict(row)

                # Try to parse list fields
                for field in ["urls", "hashtags", "mentions"]:
                    if field in record and record[field]:
                        try:
                            if record[field].startswith("["):
                                record[field] = json.loads(record[field])
                            else:
                                # Comma-separated values
                                record[field] = [
                                    v.strip() for v in record[field].split(",") if v.strip()
                                ]
                        except (json.JSONDecodeError, AttributeError):
                            record[field] = []

                # Parse metadata if present
                if "metadata" in record and record["metadata"]:
                    try:
                        record["metadata"] = json.loads(record["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        record["metadata"] = {}

                yield (line_num, record, None)

            except Exception as e:
                if skip_errors:
                    yield (line_num, None, f"CSV parse error: {e}")
                else:
                    raise


def load_data(
    file_path: str | Path,
    skip_errors: bool = True,
) -> Generator[tuple[int, dict[str, Any] | None, str | None], None, None]:
    """Load records from a data file (auto-detect format).

    Args:
        file_path: Path to data file (JSONL or CSV)
        skip_errors: If True, yield error info instead of raising

    Yields:
        Tuples of (line_number, record_dict or None, error_message or None)
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    if suffix in [".jsonl", ".json", ".ndjson"]:
        yield from load_jsonl(file_path, skip_errors)
    elif suffix in [".csv"]:
        yield from load_csv(file_path, skip_errors)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def parse_raw_post(
    record: dict[str, Any],
    line_number: int | None = None,
    source_file: str | None = None,
) -> tuple[RawPost | None, str | None]:
    """Parse a record dictionary into a RawPost.

    Args:
        record: Record dictionary
        line_number: Source line number for error reporting
        source_file: Source file path for error reporting

    Returns:
        Tuple of (RawPost or None, error_message or None)
    """
    try:
        post = RawPost(**record)
        return (post, None)
    except Exception as e:
        error_msg = f"Validation error at line {line_number}: {e}"
        logger.warning(
            "post_validation_failed",
            line=line_number,
            source=source_file,
            error=str(e),
        )
        return (None, error_msg)


def count_records(file_path: str | Path) -> int:
    """Count records in a data file without loading all into memory.

    Args:
        file_path: Path to data file

    Returns:
        Number of records
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    count = 0
    if suffix in [".jsonl", ".json", ".ndjson"]:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    count += 1
    elif suffix in [".csv"]:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            count = sum(1 for _ in reader)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")

    return count
