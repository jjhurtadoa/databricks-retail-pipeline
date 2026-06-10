"""Unit tests for logger module."""

import logging

from src.utils.logger import get_logger


def test_logger_creation():
    """Verify get_logger returns a valid logger."""
    logger = get_logger("test_module")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_module"


def test_logger_format():
    """Verify logger uses expected format."""
    logger = get_logger("format_test")

    # Get the formatter from the handler
    if logger.handlers:
        handler = logger.handlers[0]
        formatter = handler.formatter

        # Check that the formatter includes expected parts
        assert formatter is not None
        format_string = formatter._fmt
        assert "asctime" in format_string
        assert "levelname" in format_string
        assert "name" in format_string
        assert "message" in format_string


def test_logger_no_duplicate_handlers():
    """Verify logger does not add duplicate handlers on repeated calls."""
    logger1 = get_logger("dedup_test")
    handler_count_1 = len(logger1.handlers)

    logger2 = get_logger("dedup_test")
    handler_count_2 = len(logger2.handlers)

    # Should be the same logger object, no new handlers added
    assert handler_count_1 == handler_count_2
    assert logger1 is logger2


def test_logger_level():
    """Verify logger is set to INFO level."""
    logger = get_logger("level_test")
    assert logger.level == logging.INFO
