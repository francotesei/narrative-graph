"""CLI commands for Narrative Graph Intelligence."""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from narrative_graph.config import get_settings, load_config
from narrative_graph.logging import setup_logging, get_logger, generate_run_id, set_run_id

app = typer.Typer(
    name="narrative-graph",
    help="Narrative Graph Intelligence - Analyze narratives and detect coordination",
)
console = Console()


def init_logging():
    """Initialize logging."""
    setup_logging()


@app.callback()
def main(
    config: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to config file"
    ),
):
    """Narrative Graph Intelligence CLI."""
    if config:
        load_config(config)
    init_logging()


@app.command()
def db_init(
    force: bool = typer.Option(False, "--force", "-f", help="Clear existing data"),
):
    """Initialize Neo4j database schema."""
    from narrative_graph.graph.connection import get_neo4j_connection

    logger = get_logger(__name__)
    console.print("[bold blue]Initializing Neo4j database...[/]")

    conn = get_neo4j_connection()

    if not conn.verify_connectivity():
        console.print("[bold red]Failed to connect to Neo4j![/]")
        console.print("Make sure Neo4j is running: docker-compose up -d")
        raise typer.Exit(1)

    if force:
        console.print("[yellow]Clearing existing data...[/]")
        conn.clear_database()

    conn.init_schema()
    console.print("[bold green]Database initialized successfully![/]")

    # Show stats
    stats = conn.get_stats()
    if stats:
        table = Table(title="Database Statistics")
        table.add_column("Item", style="cyan")
        table.add_column("Count", style="green")
        for key, value in stats.items():
            table.add_row(key, str(value))
        console.print(table)


@app.command()
def ingest(
    input_file: Path = typer.Argument(..., help="Input data file (JSONL or CSV)"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Run identifier"),
):
    """Ingest data from file."""
    from narrative_graph.ingestion import normalize_posts
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage
    from narrative_graph.storage.manifest import create_manifest, save_manifest

    logger = get_logger(__name__)

    if not input_file.exists():
        console.print(f"[bold red]File not found: {input_file}[/]")
        raise typer.Exit(1)

    run_id = run_id or generate_run_id()
    set_run_id(run_id)

    console.print(f"[bold blue]Starting ingestion[/] (run_id: {run_id})")

    db = RunDatabase()
    storage = ParquetStorage()
    settings = get_settings()

    # Create manifest
    manifest = create_manifest(run_id, str(input_file), settings.model_dump())
    db.create_run(run_id, str(input_file), manifest.config_hash)

    step_id = db.start_step(run_id, "ingest")

    try:
        posts, dead_letters = normalize_posts(input_file, run_id, db, storage)

        db.complete_step(step_id, records_processed=len(posts))
        manifest.input_record_count = len(posts)
        manifest.dead_letter_count = dead_letters
        manifest.steps_completed.append("ingest")

        save_manifest(manifest)

        console.print(f"[green]✓ Ingested {len(posts)} posts[/]")
        if dead_letters > 0:
            console.print(f"[yellow]⚠ {dead_letters} records failed (see dead letter queue)[/]")

    except Exception as e:
        db.fail_step(step_id, str(e))
        db.fail_run(run_id, str(e))
        console.print(f"[bold red]Ingestion failed: {e}[/]")
        raise typer.Exit(1)


@app.command()
def enrich(
    run_id: str = typer.Option(..., "--run-id", help="Run identifier"),
):
    """Extract features and entities from posts."""
    from narrative_graph.features import extract_features, extract_entities, get_entity_extractor
    from narrative_graph.ingestion.schemas import NormalizedPost
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    logger = get_logger(__name__)
    set_run_id(run_id)

    console.print(f"[bold blue]Starting enrichment[/] (run_id: {run_id})")

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "enrich")

    try:
        # Load posts
        records = storage.load_records("silver", run_id)
        posts = [NormalizedPost(**r) for r in records]

        console.print(f"Loaded {len(posts)} posts")

        # Extract features
        posts = extract_features(posts, run_id, storage)

        # Extract entities
        try:
            extractor = get_entity_extractor()
            entities = extract_entities(posts, extractor)
            storage.save_records(
                [e.model_dump(mode="json") for e in entities],
                "entities",
                run_id,
            )
            console.print(f"[green]✓ Extracted entities from {len(entities)} posts[/]")
        except Exception as e:
            console.print(f"[yellow]⚠ Entity extraction skipped: {e}[/]")

        db.complete_step(step_id, records_processed=len(posts))
        console.print(f"[green]✓ Enriched {len(posts)} posts[/]")

    except Exception as e:
        db.fail_step(step_id, str(e))
        console.print(f"[bold red]Enrichment failed: {e}[/]")
        raise typer.Exit(1)


@app.command()
def cluster(
    run_id: str = typer.Option(..., "--run-id", help="Run identifier"),
):
    """Cluster posts into narratives."""
    from narrative_graph.ingestion.schemas import NormalizedPost
    from narrative_graph.narratives import cluster_posts, assign_narratives, extract_narrative_keywords
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    logger = get_logger(__name__)
    set_run_id(run_id)

    console.print(f"[bold blue]Starting clustering[/] (run_id: {run_id})")

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "cluster")

    try:
        # Load posts
        records = storage.load_records("features", run_id)
        posts = [NormalizedPost(**r) for r in records]

        console.print(f"Loaded {len(posts)} posts")

        # Cluster
        embeddings, labels, similarities = cluster_posts(posts)

        # Assign narratives
        posts, narratives = assign_narratives(posts, labels, similarities, embeddings)

        # Extract keywords
        narratives = extract_narrative_keywords(posts, narratives)

        # Save results
        storage.save_records(
            [p.model_dump(mode="json") for p in posts],
            "clustered",
            run_id,
        )
        storage.save_records(
            [n.model_dump(mode="json") for n in narratives],
            "narratives",
            run_id,
        )

        db.complete_step(step_id, records_processed=len(narratives))

        console.print(f"[green]✓ Detected {len(narratives)} narratives[/]")

        # Show top narratives
        table = Table(title="Top Narratives")
        table.add_column("ID", style="cyan")
        table.add_column("Size", style="green")
        table.add_column("Keywords", style="yellow")

        for n in narratives[:5]:
            table.add_row(n.id, str(n.size), ", ".join(n.keywords[:3]))

        console.print(table)

    except Exception as e:
        db.fail_step(step_id, str(e))
        console.print(f"[bold red]Clustering failed: {e}[/]")
        raise typer.Exit(1)


@app.command()
def build_graph(
    run_id: str = typer.Option(..., "--run-id", help="Run identifier"),
):
    """Build Neo4j graph from clustered data."""
    from narrative_graph.graph.builder import GraphBuilder
    from narrative_graph.graph.connection import get_neo4j_connection
    from narrative_graph.ingestion.schemas import NormalizedPost, NarrativeMetadata, PostEntities
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    logger = get_logger(__name__)
    set_run_id(run_id)

    console.print(f"[bold blue]Building graph[/] (run_id: {run_id})")

    db = RunDatabase()
    storage = ParquetStorage()
    conn = get_neo4j_connection()

    if not conn.verify_connectivity():
        console.print("[bold red]Failed to connect to Neo4j![/]")
        raise typer.Exit(1)

    step_id = db.start_step(run_id, "build-graph")

    try:
        # Load data
        posts_records = storage.load_records("clustered", run_id)
        posts = [NormalizedPost(**r) for r in posts_records]

        narratives_records = storage.load_records("narratives", run_id)
        narratives = [NarrativeMetadata(**r) for r in narratives_records]

        # Load entities if available
        entities = None
        if storage.exists("entities", run_id):
            entities_records = storage.load_records("entities", run_id)
            entities = [PostEntities(**r) for r in entities_records]

        console.print(f"Building graph with {len(posts)} posts, {len(narratives)} narratives")

        # Build graph
        builder = GraphBuilder(conn)
        stats = builder.build_from_posts(posts, narratives, entities)

        db.complete_step(step_id, records_processed=sum(stats.values()))

        # Show stats
        table = Table(title="Graph Statistics")
        table.add_column("Item", style="cyan")
        table.add_column("Count", style="green")

        for key, value in stats.items():
            table.add_row(key, str(value))

        console.print(table)
        console.print("[green]✓ Graph built successfully[/]")

    except Exception as e:
        db.fail_step(step_id, str(e))
        console.print(f"[bold red]Graph building failed: {e}[/]")
        raise typer.Exit(1)


@app.command()
def detect_coordination(
    run_id: str = typer.Option(..., "--run-id", help="Run identifier"),
):
    """Detect coordinated behavior."""
    import numpy as np
    from narrative_graph.coordination import detect_coordination as detect_coord
    from narrative_graph.coordination.evidence import generate_evidence_summary
    from narrative_graph.ingestion.schemas import NormalizedPost
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    logger = get_logger(__name__)
    set_run_id(run_id)

    console.print(f"[bold blue]Detecting coordination[/] (run_id: {run_id})")

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "detect-coordination")

    try:
        # Load clustered posts
        records = storage.load_records("clustered", run_id)
        posts = [NormalizedPost(**r) for r in records]

        # Get embeddings if available
        embeddings = None
        if posts and posts[0].embedding:
            embeddings = np.array([p.embedding for p in posts])

        console.print(f"Analyzing {len(posts)} posts for coordination")

        # Detect coordination
        pairs, groups = detect_coord(posts, embeddings)

        # Save results
        storage.save_records(
            [p.model_dump(mode="json") for p in pairs],
            "coordination_pairs",
            run_id,
        )
        storage.save_records(
            [g.model_dump(mode="json") for g in groups],
            "coordination_groups",
            run_id,
        )

        # Generate summary
        summary = generate_evidence_summary(pairs, groups)

        db.complete_step(step_id, records_processed=len(pairs))

        console.print(f"[green]✓ Detected {len(pairs)} coordinated pairs[/]")
        console.print(f"[green]✓ Formed {len(groups)} coordination groups[/]")

        if groups:
            table = Table(title="Top Coordination Groups")
            table.add_column("ID", style="cyan")
            table.add_column("Size", style="green")
            table.add_column("Score", style="yellow")

            for g in groups[:5]:
                table.add_row(g.id, str(g.size), f"{g.score:.3f}")

            console.print(table)

    except Exception as e:
        db.fail_step(step_id, str(e))
        console.print(f"[bold red]Coordination detection failed: {e}[/]")
        raise typer.Exit(1)


@app.command()
def score_risk(
    run_id: str = typer.Option(..., "--run-id", help="Run identifier"),
):
    """Calculate risk scores for narratives."""
    from narrative_graph.ingestion.schemas import NormalizedPost, NarrativeMetadata, CoordinatedGroup
    from narrative_graph.risk import calculate_narrative_risk
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    logger = get_logger(__name__)
    set_run_id(run_id)

    console.print(f"[bold blue]Calculating risk scores[/] (run_id: {run_id})")

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "score-risk")

    try:
        # Load data
        posts_records = storage.load_records("clustered", run_id)
        posts = [NormalizedPost(**r) for r in posts_records]

        narratives_records = storage.load_records("narratives", run_id)
        narratives = [NarrativeMetadata(**r) for r in narratives_records]

        groups = []
        if storage.exists("coordination_groups", run_id):
            groups_records = storage.load_records("coordination_groups", run_id)
            groups = [CoordinatedGroup(**r) for r in groups_records]

        console.print(f"Scoring {len(narratives)} narratives")

        # Calculate risk
        risks = calculate_narrative_risk(posts, narratives, groups)

        # Save results
        storage.save_records(
            [r.model_dump(mode="json") for r in risks],
            "risks",
            run_id,
        )

        db.complete_step(step_id, records_processed=len(risks))

        # Show results
        table = Table(title="Risk Assessment")
        table.add_column("Narrative", style="cyan")
        table.add_column("Score", style="yellow")
        table.add_column("Level", style="bold")

        for r in risks[:10]:
            level_color = {"HIGH": "red", "MEDIUM": "yellow", "LOW": "green"}[r.risk_level.value]
            table.add_row(
                r.narrative_id,
                f"{r.risk_score:.3f}",
                f"[{level_color}]{r.risk_level.value}[/]",
            )

        console.print(table)

        high_risk = sum(1 for r in risks if r.risk_level.value == "HIGH")
        console.print(f"[green]✓ {high_risk} high-risk narratives identified[/]")

    except Exception as e:
        db.fail_step(step_id, str(e))
        console.print(f"[bold red]Risk scoring failed: {e}[/]")
        raise typer.Exit(1)


@app.command()
def explain(
    run_id: str = typer.Option(..., "--run-id", help="Run identifier"),
    use_llm: bool = typer.Option(False, "--llm", help="Use LLM for explanations"),
):
    """Generate explanations for narratives."""
    from narrative_graph.explain.llm import LLMExplainer
    from narrative_graph.explain.fallback import FallbackExplainer
    from narrative_graph.ingestion.schemas import NarrativeMetadata, NarrativeRisk
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    logger = get_logger(__name__)
    set_run_id(run_id)

    console.print(f"[bold blue]Generating explanations[/] (run_id: {run_id})")

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "explain")

    try:
        # Load data
        narratives_records = storage.load_records("narratives", run_id)
        narratives = [NarrativeMetadata(**r) for r in narratives_records]

        risks_records = storage.load_records("risks", run_id)
        risks = {r["narrative_id"]: NarrativeRisk(**r) for r in risks_records}

        # Choose explainer
        if use_llm:
            llm_explainer = LLMExplainer()
            if not llm_explainer.is_available():
                console.print("[yellow]LLM not available, using fallback[/]")
                explainer = FallbackExplainer()
            else:
                explainer = llm_explainer
                console.print("[blue]Using LLM for explanations[/]")
        else:
            explainer = FallbackExplainer()
            console.print("[blue]Using template-based explanations[/]")

        explanations = []
        for narrative in narratives:
            risk = risks.get(narrative.id)
            if not risk:
                continue

            explanation = explainer.explain_narrative(narrative, risk)
            explanations.append(explanation)

        # Save explanations
        storage.save_records(
            [e.model_dump(mode="json") for e in explanations],
            "explanations",
            run_id,
        )

        db.complete_step(step_id, records_processed=len(explanations))

        console.print(f"[green]✓ Generated {len(explanations)} explanations[/]")

        # Show sample
        if explanations:
            console.print("\n[bold]Sample explanation:[/]")
            console.print(explanations[0].explanation_text[:500] + "...")

    except Exception as e:
        db.fail_step(step_id, str(e))
        console.print(f"[bold red]Explanation generation failed: {e}[/]")
        raise typer.Exit(1)


def _run_ingest(input_file: Path, run_id: str):
    """Internal: run ingest step."""
    from narrative_graph.ingestion import normalize_posts
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage
    from narrative_graph.storage.manifest import create_manifest, save_manifest

    if not input_file.exists():
        console.print(f"[bold red]File not found: {input_file}[/]")
        raise typer.Exit(1)

    db = RunDatabase()
    storage = ParquetStorage()
    settings = get_settings()

    # Create manifest
    manifest = create_manifest(run_id, str(input_file), settings.model_dump())
    db.create_run(run_id, str(input_file), manifest.config_hash)

    step_id = db.start_step(run_id, "ingest")

    posts, dead_letters = normalize_posts(input_file, run_id, db, storage)

    db.complete_step(step_id, records_processed=len(posts))
    manifest.input_record_count = len(posts)
    manifest.dead_letter_count = dead_letters
    manifest.steps_completed.append("ingest")

    save_manifest(manifest)

    console.print(f"[green]✓ Ingested {len(posts)} posts[/]")
    if dead_letters > 0:
        console.print(f"[yellow]⚠ {dead_letters} records failed (see dead letter queue)[/]")


def _run_enrich(run_id: str):
    """Internal: run enrich step."""
    from narrative_graph.features import extract_features, extract_entities, get_entity_extractor
    from narrative_graph.ingestion.schemas import NormalizedPost
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "enrich")

    # Load posts
    records = storage.load_records("silver", run_id)
    posts = [NormalizedPost(**r) for r in records]

    console.print(f"Loaded {len(posts)} posts")

    # Extract features
    posts = extract_features(posts, run_id, storage)

    # Extract entities
    try:
        extractor = get_entity_extractor()
        entities = extract_entities(posts, extractor)
        storage.save_records(
            [e.model_dump(mode="json") for e in entities],
            "entities",
            run_id,
        )
        console.print(f"[green]✓ Extracted entities from {len(entities)} posts[/]")
    except Exception as e:
        console.print(f"[yellow]⚠ Entity extraction skipped: {e}[/]")

    db.complete_step(step_id, records_processed=len(posts))
    console.print(f"[green]✓ Enriched {len(posts)} posts[/]")


def _run_cluster(run_id: str):
    """Internal: run cluster step."""
    from narrative_graph.ingestion.schemas import NormalizedPost
    from narrative_graph.narratives import cluster_posts, assign_narratives, extract_narrative_keywords
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "cluster")

    # Load posts
    records = storage.load_records("features", run_id)
    posts = [NormalizedPost(**r) for r in records]

    console.print(f"Loaded {len(posts)} posts")

    # Cluster
    embeddings, labels, similarities = cluster_posts(posts)

    # Assign narratives
    posts, narratives = assign_narratives(posts, labels, similarities, embeddings)

    # Extract keywords
    narratives = extract_narrative_keywords(posts, narratives)

    # Save results
    storage.save_records(
        [p.model_dump(mode="json") for p in posts],
        "clustered",
        run_id,
    )
    storage.save_records(
        [n.model_dump(mode="json") for n in narratives],
        "narratives",
        run_id,
    )

    db.complete_step(step_id, records_processed=len(narratives))

    console.print(f"[green]✓ Detected {len(narratives)} narratives[/]")

    # Show top narratives
    table = Table(title="Top Narratives")
    table.add_column("ID", style="cyan")
    table.add_column("Size", style="green")
    table.add_column("Keywords", style="yellow")

    for n in narratives[:5]:
        table.add_row(n.id, str(n.size), ", ".join(n.keywords[:3]))

    console.print(table)


def _run_build_graph(run_id: str):
    """Internal: run build-graph step."""
    from narrative_graph.graph.builder import GraphBuilder
    from narrative_graph.graph.connection import get_neo4j_connection
    from narrative_graph.ingestion.schemas import NormalizedPost, NarrativeMetadata, PostEntities
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    db = RunDatabase()
    storage = ParquetStorage()
    conn = get_neo4j_connection()

    if not conn.verify_connectivity():
        console.print("[bold red]Failed to connect to Neo4j![/]")
        raise typer.Exit(1)

    step_id = db.start_step(run_id, "build-graph")

    # Load data
    posts_records = storage.load_records("clustered", run_id)
    posts = [NormalizedPost(**r) for r in posts_records]

    narratives_records = storage.load_records("narratives", run_id)
    narratives = [NarrativeMetadata(**r) for r in narratives_records]

    # Load entities if available
    entities = None
    if storage.exists("entities", run_id):
        entities_records = storage.load_records("entities", run_id)
        entities = [PostEntities(**r) for r in entities_records]

    console.print(f"Building graph with {len(posts)} posts, {len(narratives)} narratives")

    # Build graph
    builder = GraphBuilder(conn)
    stats = builder.build_from_posts(posts, narratives, entities)

    db.complete_step(step_id, records_processed=sum(stats.values()))

    # Show stats
    table = Table(title="Graph Statistics")
    table.add_column("Item", style="cyan")
    table.add_column("Count", style="green")

    for key, value in stats.items():
        table.add_row(key, str(value))

    console.print(table)
    console.print("[green]✓ Graph built successfully[/]")


def _run_detect_coordination(run_id: str):
    """Internal: run detect-coordination step."""
    import numpy as np
    from narrative_graph.coordination import detect_coordination as detect_coord
    from narrative_graph.coordination.evidence import generate_evidence_summary
    from narrative_graph.ingestion.schemas import NormalizedPost
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "detect-coordination")

    # Load clustered posts
    records = storage.load_records("clustered", run_id)
    posts = [NormalizedPost(**r) for r in records]

    # Get embeddings if available
    embeddings = None
    if posts and posts[0].embedding:
        embeddings = np.array([p.embedding for p in posts])

    console.print(f"Analyzing {len(posts)} posts for coordination")

    # Detect coordination
    pairs, groups = detect_coord(posts, embeddings)

    # Save results
    storage.save_records(
        [p.model_dump(mode="json") for p in pairs],
        "coordination_pairs",
        run_id,
    )
    storage.save_records(
        [g.model_dump(mode="json") for g in groups],
        "coordination_groups",
        run_id,
    )

    # Generate summary
    summary = generate_evidence_summary(pairs, groups)

    db.complete_step(step_id, records_processed=len(pairs))

    console.print(f"[green]✓ Detected {len(pairs)} coordinated pairs[/]")
    console.print(f"[green]✓ Formed {len(groups)} coordination groups[/]")

    if groups:
        table = Table(title="Top Coordination Groups")
        table.add_column("ID", style="cyan")
        table.add_column("Size", style="green")
        table.add_column("Score", style="yellow")

        for g in groups[:5]:
            table.add_row(g.id, str(g.size), f"{g.score:.3f}")

        console.print(table)


def _run_score_risk(run_id: str):
    """Internal: run score-risk step."""
    from narrative_graph.ingestion.schemas import NormalizedPost, NarrativeMetadata, CoordinatedGroup
    from narrative_graph.risk import calculate_narrative_risk
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "score-risk")

    # Load data
    posts_records = storage.load_records("clustered", run_id)
    posts = [NormalizedPost(**r) for r in posts_records]

    narratives_records = storage.load_records("narratives", run_id)
    narratives = [NarrativeMetadata(**r) for r in narratives_records]

    groups = []
    if storage.exists("coordination_groups", run_id):
        groups_records = storage.load_records("coordination_groups", run_id)
        groups = [CoordinatedGroup(**r) for r in groups_records]

    console.print(f"Scoring {len(narratives)} narratives")

    # Calculate risk
    risks = calculate_narrative_risk(posts, narratives, groups)

    # Save results
    storage.save_records(
        [r.model_dump(mode="json") for r in risks],
        "risks",
        run_id,
    )

    db.complete_step(step_id, records_processed=len(risks))

    # Show results
    table = Table(title="Risk Assessment")
    table.add_column("Narrative", style="cyan")
    table.add_column("Score", style="yellow")
    table.add_column("Level", style="bold")

    for r in risks[:10]:
        level_color = {"HIGH": "red", "MEDIUM": "yellow", "LOW": "green"}[r.risk_level.value]
        table.add_row(
            r.narrative_id,
            f"{r.risk_score:.3f}",
            f"[{level_color}]{r.risk_level.value}[/]",
        )

    console.print(table)

    high_risk = sum(1 for r in risks if r.risk_level.value == "HIGH")
    console.print(f"[green]✓ {high_risk} high-risk narratives identified[/]")


def _run_explain(run_id: str, use_llm: bool):
    """Internal: run explain step."""
    from narrative_graph.explain.llm import LLMExplainer
    from narrative_graph.explain.fallback import FallbackExplainer
    from narrative_graph.ingestion.schemas import NarrativeMetadata, NarrativeRisk
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.parquet import ParquetStorage

    db = RunDatabase()
    storage = ParquetStorage()

    step_id = db.start_step(run_id, "explain")

    # Load data
    narratives_records = storage.load_records("narratives", run_id)
    narratives = [NarrativeMetadata(**r) for r in narratives_records]

    risks_records = storage.load_records("risks", run_id)
    risks = {r["narrative_id"]: NarrativeRisk(**r) for r in risks_records}

    # Choose explainer
    if use_llm:
        llm_explainer = LLMExplainer()
        if not llm_explainer.is_available():
            console.print("[yellow]LLM not available, using fallback[/]")
            explainer = FallbackExplainer()
        else:
            explainer = llm_explainer
            console.print("[blue]Using LLM for explanations[/]")
    else:
        explainer = FallbackExplainer()
        console.print("[blue]Using template-based explanations[/]")

    explanations = []
    for narrative in narratives:
        risk = risks.get(narrative.id)
        if not risk:
            continue

        explanation = explainer.explain_narrative(narrative, risk)
        explanations.append(explanation)

    # Save explanations
    storage.save_records(
        [e.model_dump(mode="json") for e in explanations],
        "explanations",
        run_id,
    )

    db.complete_step(step_id, records_processed=len(explanations))

    console.print(f"[green]✓ Generated {len(explanations)} explanations[/]")

    # Show sample
    if explanations:
        console.print("\n[bold]Sample explanation:[/]")
        console.print(explanations[0].explanation_text[:500] + "...")


@app.command()
def run_all(
    input_file: Path = typer.Argument(..., help="Input data file"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Run identifier"),
    use_llm: bool = typer.Option(False, "--llm", help="Use LLM for explanations"),
):
    """Run complete pipeline."""
    from narrative_graph.storage.database import RunDatabase
    from narrative_graph.storage.manifest import load_manifest, save_manifest

    logger = get_logger(__name__)

    run_id = run_id or generate_run_id()
    set_run_id(run_id)

    console.print(f"[bold blue]Starting full pipeline[/] (run_id: {run_id})")
    console.print(f"Input: {input_file}")

    try:
        # 1. Ingest
        console.print("\n[bold]Step 1/7: Ingestion[/]")
        _run_ingest(input_file, run_id)

        # 2. Enrich
        console.print("\n[bold]Step 2/7: Enrichment[/]")
        _run_enrich(run_id)

        # 3. Cluster
        console.print("\n[bold]Step 3/7: Clustering[/]")
        _run_cluster(run_id)

        # 4. Build graph
        console.print("\n[bold]Step 4/7: Graph Building[/]")
        _run_build_graph(run_id)

        # 5. Detect coordination
        console.print("\n[bold]Step 5/7: Coordination Detection[/]")
        _run_detect_coordination(run_id)

        # 6. Score risk
        console.print("\n[bold]Step 6/7: Risk Scoring[/]")
        _run_score_risk(run_id)

        # 7. Generate explanations
        console.print("\n[bold]Step 7/7: Explanations[/]")
        _run_explain(run_id, use_llm)

        # Complete run
        db = RunDatabase()
        db.complete_run(run_id)

        # Update manifest
        manifest = load_manifest(run_id)
        manifest.status = "completed"
        save_manifest(manifest)

        console.print(f"\n[bold green]Pipeline completed successfully![/]")
        console.print(f"Run ID: {run_id}")
        console.print(f"View results: uv run streamlit run src/narrative_graph/ui/app.py")

    except typer.Exit:
        raise
    except Exception as e:
        db = RunDatabase()
        db.fail_run(run_id, str(e))
        console.print(f"\n[bold red]Pipeline failed: {e}[/]")
        raise typer.Exit(1)


@app.command()
def status(
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Run identifier"),
):
    """Show pipeline status."""
    from narrative_graph.storage.database import RunDatabase

    db = RunDatabase()

    if run_id:
        run = db.get_run(run_id)
        if not run:
            console.print(f"[red]Run not found: {run_id}[/]")
            raise typer.Exit(1)

        console.print(f"\n[bold]Run: {run_id}[/]")
        console.print(f"Status: {run['status']}")
        console.print(f"Started: {run['started_at']}")
        if run['completed_at']:
            console.print(f"Completed: {run['completed_at']}")

        steps = db.get_run_steps(run_id)
        if steps:
            table = Table(title="Steps")
            table.add_column("Step", style="cyan")
            table.add_column("Status", style="bold")
            table.add_column("Records", style="green")

            for step in steps:
                status_color = {"completed": "green", "failed": "red", "running": "yellow"}
                color = status_color.get(step["status"], "white")
                table.add_row(
                    step["step_name"],
                    f"[{color}]{step['status']}[/]",
                    str(step["records_processed"] or 0),
                )

            console.print(table)

        dead_letters = db.count_dead_letters(run_id)
        if dead_letters > 0:
            console.print(f"[yellow]Dead letters: {dead_letters}[/]")

    else:
        runs = db.list_runs(10)
        if not runs:
            console.print("[yellow]No runs found[/]")
            return

        table = Table(title="Recent Runs")
        table.add_column("Run ID", style="cyan")
        table.add_column("Status", style="bold")
        table.add_column("Started", style="dim")

        for run in runs:
            status_color = {"completed": "green", "failed": "red", "running": "yellow"}
            color = status_color.get(run["status"], "white")
            table.add_row(
                run["run_id"],
                f"[{color}]{run['status']}[/]",
                run["started_at"][:19],
            )

        console.print(table)


if __name__ == "__main__":
    app()
