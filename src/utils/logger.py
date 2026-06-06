"""Shared logger factory for all pipeline modules."""

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a logger with a consistent format for pipeline modules.

    Args:
        name: Typically __name__ of the calling module.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)
    return logger
