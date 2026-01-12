"""Structured logging configuration for Narrative Graph Intelligence."""

import logging
import sys
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import structlog
from structlog.types import EventDict, Processor

from narrative_graph.config import get_settings

# Context variables for request/run tracking
run_id_var: ContextVar[str | None] = ContextVar("run_id", default=None)
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)


def get_run_id() -> str | None:
    """Get the current run ID from context."""
    return run_id_var.get()


def set_run_id(run_id: str) -> None:
    """Set the run ID in context."""
    run_id_var.set(run_id)


def generate_run_id(prefix: str = "run") -> str:
    """Generate a new unique run ID."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def get_request_id() -> str | None:
    """Get the current request ID from context."""
    return request_id_var.get()


def set_request_id(request_id: str) -> None:
    """Set the request ID in context."""
    request_id_var.set(request_id)


def add_context_info(
    logger: logging.Logger, method_name: str, event_dict: EventDict
) -> EventDict:
    """Add run_id and request_id to log events."""
    run_id = get_run_id()
    request_id = get_request_id()

    if run_id:
        event_dict["run_id"] = run_id
    if request_id:
        event_dict["request_id"] = request_id

    return event_dict


def setup_logging(
    level: str | None = None,
    log_format: str | None = None,
    log_file: str | None = None,
) -> None:
    """Configure structured logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR). Defaults to config value.
        log_format: Output format ('json' or 'console'). Defaults to config value.
        log_file: Path to log file. Defaults to config value.
    """
    settings = get_settings()

    level = level or settings.logging.level
    log_format = log_format or settings.logging.format
    log_file = log_file or settings.logging.log_file

    # Convert level string to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Common processors
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
        add_context_info,
    ]

    if log_format == "json":
        # JSON format for production
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        # Console format for development
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    # Configure structlog
    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure standard logging
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # File handler (if specified)
    handlers: list[logging.Handler] = [console_handler]
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.handlers = []
    for handler in handlers:
        root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)

    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("neo4j").setLevel(logging.WARNING)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance.

    Args:
        name: Logger name. If None, uses the calling module's name.

    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


class LogContext:
    """Context manager for adding temporary context to logs."""

    def __init__(self, **kwargs: Any) -> None:
        self.context = kwargs
        self._tokens: dict[str, Any] = {}

    def __enter__(self) -> "LogContext":
        structlog.contextvars.bind_contextvars(**self.context)
        return self

    def __exit__(self, *args: Any) -> None:
        structlog.contextvars.unbind_contextvars(*self.context.keys())
