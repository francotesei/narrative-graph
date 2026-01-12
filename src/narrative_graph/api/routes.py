"""API routes for Narrative Graph Intelligence."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from narrative_graph.graph.connection import get_neo4j_connection
from narrative_graph.graph import queries
from narrative_graph.storage.database import RunDatabase
from narrative_graph.storage.parquet import ParquetStorage
from narrative_graph.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


# Response models
class HealthResponse(BaseModel):
    status: str
    neo4j_connected: bool
    version: str


class NarrativeResponse(BaseModel):
    id: str
    size: int
    keywords: list[str]
    top_domains: list[str]
    top_hashtags: list[str]
    risk_score: float | None = None
    risk_level: str | None = None
    explanation: str | None = None
    author_count: int = 0


class NarrativeListResponse(BaseModel):
    narratives: list[NarrativeResponse]
    total: int


class CoordinationGroupResponse(BaseModel):
    id: str
    size: int
    score: float
    author_ids: list[str]
    narrative_ids: list[str]
    evidence_summary: str


class CoordinationListResponse(BaseModel):
    groups: list[CoordinationGroupResponse]
    total: int


class RunResponse(BaseModel):
    run_id: str
    status: str
    started_at: str
    completed_at: str | None = None
    input_file: str | None = None


class RunListResponse(BaseModel):
    runs: list[RunResponse]
    total: int


# Health check
@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Check API and database health."""
    conn = get_neo4j_connection()
    neo4j_ok = conn.verify_connectivity()

    return HealthResponse(
        status="healthy" if neo4j_ok else "degraded",
        neo4j_connected=neo4j_ok,
        version="0.1.0",
    )


# Runs
@router.get("/runs", response_model=RunListResponse, tags=["Runs"])
async def list_runs(limit: int = Query(default=10, le=100)):
    """List pipeline runs."""
    db = RunDatabase()
    runs = db.list_runs(limit)

    return RunListResponse(
        runs=[
            RunResponse(
                run_id=r["run_id"],
                status=r["status"],
                started_at=r["started_at"],
                completed_at=r.get("completed_at"),
                input_file=r.get("input_file"),
            )
            for r in runs
        ],
        total=len(runs),
    )


@router.get("/runs/{run_id}", response_model=RunResponse, tags=["Runs"])
async def get_run(run_id: str):
    """Get run details."""
    db = RunDatabase()
    run = db.get_run(run_id)

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    return RunResponse(
        run_id=run["run_id"],
        status=run["status"],
        started_at=run["started_at"],
        completed_at=run.get("completed_at"),
        input_file=run.get("input_file"),
    )


# Narratives
@router.get("/narratives", response_model=NarrativeListResponse, tags=["Narratives"])
async def list_narratives(
    run_id: str = Query(..., description="Run identifier"),
    risk_level: str | None = Query(default=None, description="Filter by risk level"),
    min_size: int = Query(default=1, description="Minimum narrative size"),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
):
    """List narratives for a run."""
    storage = ParquetStorage()

    if not storage.exists("narratives", run_id):
        raise HTTPException(status_code=404, detail="Narratives not found for this run")

    try:
        narratives_df = storage.load_dataframe("narratives", run_id)

        # Join with risks if available
        if storage.exists("risks", run_id):
            risks_df = storage.load_dataframe("risks", run_id)
            narratives_df = narratives_df.merge(
                risks_df[["narrative_id", "risk_score", "risk_level"]],
                left_on="id",
                right_on="narrative_id",
                how="left",
            )

        # Join with explanations if available
        if storage.exists("explanations", run_id):
            exp_df = storage.load_dataframe("explanations", run_id)
            narratives_df = narratives_df.merge(
                exp_df[["target_id", "explanation_text"]],
                left_on="id",
                right_on="target_id",
                how="left",
            )

        # Apply filters
        narratives_df = narratives_df[narratives_df["size"] >= min_size]

        if risk_level and "risk_level" in narratives_df.columns:
            narratives_df = narratives_df[narratives_df["risk_level"] == risk_level]

        # Sort by risk score
        if "risk_score" in narratives_df.columns:
            narratives_df = narratives_df.sort_values("risk_score", ascending=False)

        total = len(narratives_df)

        # Paginate
        narratives_df = narratives_df.iloc[offset : offset + limit]

        narratives = []
        for _, row in narratives_df.iterrows():
            narratives.append(
                NarrativeResponse(
                    id=row["id"],
                    size=row["size"],
                    keywords=row.get("keywords", []) or [],
                    top_domains=row.get("top_domains", []) or [],
                    top_hashtags=row.get("top_hashtags", []) or [],
                    risk_score=row.get("risk_score"),
                    risk_level=row.get("risk_level"),
                    explanation=row.get("explanation_text"),
                    author_count=row.get("author_count", 0),
                )
            )

        return NarrativeListResponse(narratives=narratives, total=total)

    except Exception as e:
        logger.error("list_narratives_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/narratives/{narrative_id}", response_model=NarrativeResponse, tags=["Narratives"])
async def get_narrative(
    narrative_id: str,
    run_id: str = Query(..., description="Run identifier"),
):
    """Get narrative details."""
    storage = ParquetStorage()

    if not storage.exists("narratives", run_id):
        raise HTTPException(status_code=404, detail="Narratives not found for this run")

    try:
        narratives_df = storage.load_dataframe("narratives", run_id)
        narrative_row = narratives_df[narratives_df["id"] == narrative_id]

        if narrative_row.empty:
            raise HTTPException(status_code=404, detail="Narrative not found")

        row = narrative_row.iloc[0]

        # Get risk
        risk_score = None
        risk_level = None
        if storage.exists("risks", run_id):
            risks_df = storage.load_dataframe("risks", run_id)
            risk_row = risks_df[risks_df["narrative_id"] == narrative_id]
            if not risk_row.empty:
                risk_score = risk_row.iloc[0]["risk_score"]
                risk_level = risk_row.iloc[0]["risk_level"]

        # Get explanation
        explanation = None
        if storage.exists("explanations", run_id):
            exp_df = storage.load_dataframe("explanations", run_id)
            exp_row = exp_df[exp_df["target_id"] == narrative_id]
            if not exp_row.empty:
                explanation = exp_row.iloc[0]["explanation_text"]

        return NarrativeResponse(
            id=row["id"],
            size=row["size"],
            keywords=row.get("keywords", []) or [],
            top_domains=row.get("top_domains", []) or [],
            top_hashtags=row.get("top_hashtags", []) or [],
            risk_score=risk_score,
            risk_level=risk_level,
            explanation=explanation,
            author_count=row.get("author_count", 0),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_narrative_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# Coordination
@router.get("/coordination", response_model=CoordinationListResponse, tags=["Coordination"])
async def list_coordination_groups(
    run_id: str = Query(..., description="Run identifier"),
    min_score: float = Query(default=0.0, description="Minimum coordination score"),
    min_size: int = Query(default=2, description="Minimum group size"),
    limit: int = Query(default=50, le=200),
):
    """List coordination groups for a run."""
    storage = ParquetStorage()

    if not storage.exists("coordination_groups", run_id):
        raise HTTPException(status_code=404, detail="Coordination data not found for this run")

    try:
        groups_df = storage.load_dataframe("coordination_groups", run_id)

        # Apply filters
        groups_df = groups_df[groups_df["score"] >= min_score]
        groups_df = groups_df[groups_df["size"] >= min_size]

        # Sort by score
        groups_df = groups_df.sort_values("score", ascending=False)

        total = len(groups_df)
        groups_df = groups_df.head(limit)

        groups = []
        for _, row in groups_df.iterrows():
            groups.append(
                CoordinationGroupResponse(
                    id=row["id"],
                    size=row["size"],
                    score=row["score"],
                    author_ids=row.get("author_ids", []) or [],
                    narrative_ids=row.get("narrative_ids", []) or [],
                    evidence_summary=row.get("evidence_summary", ""),
                )
            )

        return CoordinationListResponse(groups=groups, total=total)

    except Exception as e:
        logger.error("list_coordination_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# Graph queries (direct Neo4j)
@router.get("/graph/stats", tags=["Graph"])
async def get_graph_stats():
    """Get graph statistics from Neo4j."""
    conn = get_neo4j_connection()

    if not conn.verify_connectivity():
        raise HTTPException(status_code=503, detail="Neo4j not available")

    try:
        from narrative_graph.graph.metrics import GraphMetrics

        metrics = GraphMetrics(conn)
        summary = metrics.get_graph_summary()

        return summary

    except Exception as e:
        logger.error("get_graph_stats_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/graph/narrative/{narrative_id}", tags=["Graph"])
async def get_narrative_subgraph(narrative_id: str):
    """Get subgraph for a narrative."""
    conn = get_neo4j_connection()

    if not conn.verify_connectivity():
        raise HTTPException(status_code=503, detail="Neo4j not available")

    try:
        from narrative_graph.graph.export import GraphExporter

        exporter = GraphExporter(conn)
        graph_data = exporter.export_narrative_subgraph(narrative_id)

        return graph_data

    except Exception as e:
        logger.error("get_narrative_subgraph_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
