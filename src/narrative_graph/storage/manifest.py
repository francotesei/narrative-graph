"""Run manifest for reproducibility tracking."""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from narrative_graph.config import get_settings
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class RunManifest(BaseModel):
    """Manifest tracking inputs, config, and outputs for a pipeline run."""

    run_id: str
    started_at: datetime
    completed_at: datetime | None = None
    status: str = "running"

    # Input tracking
    input_file: str | None = None
    input_hash: str | None = None
    input_record_count: int = 0

    # Config tracking
    config_hash: str | None = None
    config_snapshot: dict[str, Any] = Field(default_factory=dict)

    # Output tracking
    outputs: dict[str, str] = Field(default_factory=dict)  # name -> path
    output_counts: dict[str, int] = Field(default_factory=dict)  # name -> count

    # Step tracking
    steps_completed: list[str] = Field(default_factory=list)

    # Error tracking
    error_message: str | None = None
    dead_letter_count: int = 0


def compute_file_hash(file_path: str | Path) -> str:
    """Compute SHA256 hash of a file.

    Args:
        file_path: Path to file

    Returns:
        Hex digest of file hash
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()[:16]


def compute_config_hash(config: dict[str, Any]) -> str:
    """Compute hash of configuration.

    Args:
        config: Configuration dictionary

    Returns:
        Hex digest of config hash
    """
    config_str = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha256(config_str.encode()).hexdigest()[:16]


def create_manifest(
    run_id: str,
    input_file: str | None = None,
    config: dict[str, Any] | None = None,
) -> RunManifest:
    """Create a new run manifest.

    Args:
        run_id: Unique run identifier
        input_file: Path to input data file
        config: Configuration dictionary

    Returns:
        New RunManifest instance
    """
    manifest = RunManifest(
        run_id=run_id,
        started_at=datetime.utcnow(),
    )

    if input_file:
        manifest.input_file = input_file
        if Path(input_file).exists():
            manifest.input_hash = compute_file_hash(input_file)

    if config:
        manifest.config_hash = compute_config_hash(config)
        manifest.config_snapshot = config

    return manifest


def save_manifest(manifest: RunManifest, output_dir: str | Path | None = None) -> Path:
    """Save manifest to JSON file.

    Args:
        manifest: Manifest to save
        output_dir: Output directory. Defaults to outputs/{run_id}/

    Returns:
        Path to saved manifest
    """
    if output_dir is None:
        settings = get_settings()
        output_dir = Path(settings.paths.outputs_dir) / manifest.run_id

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest.model_dump(mode="json"), f, indent=2, default=str)

    logger.info("manifest_saved", path=str(manifest_path), run_id=manifest.run_id)
    return manifest_path


def load_manifest(run_id: str, output_dir: str | Path | None = None) -> RunManifest:
    """Load manifest from JSON file.

    Args:
        run_id: Run identifier
        output_dir: Output directory. Defaults to outputs/

    Returns:
        Loaded RunManifest

    Raises:
        FileNotFoundError: If manifest doesn't exist
    """
    if output_dir is None:
        settings = get_settings()
        output_dir = Path(settings.paths.outputs_dir)

    manifest_path = Path(output_dir) / run_id / "manifest.json"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    with open(manifest_path) as f:
        data = json.load(f)

    return RunManifest(**data)


def list_manifests(output_dir: str | Path | None = None) -> list[RunManifest]:
    """List all available manifests.

    Args:
        output_dir: Output directory to search

    Returns:
        List of RunManifest objects
    """
    if output_dir is None:
        settings = get_settings()
        output_dir = Path(settings.paths.outputs_dir)

    output_dir = Path(output_dir)
    manifests = []

    for manifest_path in output_dir.glob("*/manifest.json"):
        try:
            with open(manifest_path) as f:
                data = json.load(f)
            manifests.append(RunManifest(**data))
        except Exception as e:
            logger.warning("manifest_load_failed", path=str(manifest_path), error=str(e))

    return sorted(manifests, key=lambda m: m.started_at, reverse=True)
