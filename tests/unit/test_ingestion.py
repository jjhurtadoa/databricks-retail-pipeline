"""Unit tests for ingestion module (watermark and extraction logic)."""

import json
import tempfile
from pathlib import Path

from src.ingestion.raw_extract import EPOCH, load_watermarks, save_watermarks


def test_load_watermarks_empty():
    """Verify load_watermarks returns epoch defaults when file doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        watermarks_file = Path(tmpdir) / "watermarks.json"
        result = load_watermarks(watermarks_file)
        # Should return epoch defaults for all tables
        assert "customers" in result
        assert result["customers"] == EPOCH


def test_load_watermarks_existing():
    """Verify load_watermarks correctly reads existing watermarks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        watermarks_file = Path(tmpdir) / "watermarks.json"
        test_data = {"customers": "2024-01-01", "products": "2024-01-02"}

        with open(watermarks_file, "w") as f:
            json.dump(test_data, f)

        result = load_watermarks(watermarks_file)
        assert result == test_data


def test_save_watermarks():
    """Verify save_watermarks creates/updates watermarks file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        watermarks_file = Path(tmpdir) / "watermarks.json"
        test_data = {"customers": "2024-06-01", "orders": "2024-06-01"}

        save_watermarks(watermarks_file, test_data)

        # Read back and verify
        with open(watermarks_file, "r") as f:
            loaded = json.load(f)

        assert loaded == test_data


def test_watermark_roundtrip():
    """Verify save → load roundtrip preserves data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        watermarks_file = Path(tmpdir) / "watermarks.json"
        original = {
            "customers": "2024-06-01T10:30:00",
            "products": "2024-06-01T09:45:00",
            "orders": "2024-06-01T08:00:00",
            "order_items": "2024-06-01T07:15:00",
        }

        save_watermarks(watermarks_file, original)
        loaded = load_watermarks(watermarks_file)

        assert loaded == original
