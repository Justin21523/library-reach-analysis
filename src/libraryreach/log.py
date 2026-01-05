from __future__ import annotations

import logging
from pathlib import Path


def configure_logging(log_dir: Path, level: str = "INFO") -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "libraryreach.log"

    logger = logging.getLogger("libraryreach")
    logger.setLevel(level.upper())
    logger.propagate = False

    if not logger.handlers:
        fmt = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        stream = logging.StreamHandler()
        stream.setFormatter(fmt)
        stream.setLevel(level.upper())

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(fmt)
        file_handler.setLevel(level.upper())

        logger.addHandler(stream)
        logger.addHandler(file_handler)

    return logger

