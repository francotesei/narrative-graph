"""FastAPI application for Narrative Graph Intelligence."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from narrative_graph.api.routes import router
from narrative_graph.logging import setup_logging

# Initialize logging
setup_logging()

app = FastAPI(
    title="Narrative Graph Intelligence API",
    description="API for analyzing narratives, detecting coordination, and assessing risks",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(router)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "Narrative Graph Intelligence API",
        "version": "0.1.0",
        "docs": "/docs",
    }
