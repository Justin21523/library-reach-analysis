"""
Logging configuration for LibraryReach.

We keep logging setup centralized so every module (CLI, pipeline, API) produces
consistent output. This is important in data projects because reproducibility
depends on knowing *which inputs and settings produced which outputs*.
"""

from __future__ import annotations

# Python's built-in logging is sufficient for Phase 1 and avoids extra dependencies.
import logging
# `Path` is used to ensure log paths are created safely and portably.
from pathlib import Path


def configure_logging(log_dir: Path, level: str = "INFO") -> logging.Logger:
    # Ensure the log directory exists before we create a file handler.
    log_dir.mkdir(parents=True, exist_ok=True)
    # Use a stable filename so users know where to look for logs across runs.
    log_path = log_dir / "libraryreach.log"

    # Use a named logger so we can control formatting/handlers without touching the root logger.
    logger = logging.getLogger("libraryreach")
    # Normalize log level strings like "info" -> "INFO" to match logging's expectations.
    logger.setLevel(level.upper())
    # Disable propagation so logs are not duplicated by ancestor/root handlers (common pitfall).
    logger.propagate = False

    # Add handlers only once so repeated imports (e.g., uvicorn reload) do not duplicate logs.
    if not logger.handlers:
        # A simple, readable format is ideal for Phase 1 debugging and CLI usage.
        fmt = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # StreamHandler prints to stderr/stdout so you can see progress in the terminal.
        stream = logging.StreamHandler()
        # Attach the formatter so terminal logs match file logs.
        stream.setFormatter(fmt)
        # Match handler level to logger level so filtering is consistent.
        stream.setLevel(level.upper())

        # FileHandler persists logs so you can audit runs after the fact.
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        # Use the same formatter so logs are comparable across destinations.
        file_handler.setFormatter(fmt)
        # Match handler level to logger level for predictable filtering.
        file_handler.setLevel(level.upper())

        # Attach handlers to the named logger.
        logger.addHandler(stream)
        logger.addHandler(file_handler)

    # Return the configured logger so callers can log immediately after bootstrap.
    return logger
