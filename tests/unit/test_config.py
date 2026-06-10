"""Unit tests for configuration module."""

from src.utils.config import Config


def test_config_defaults():
    """Verify Config loads default values when env vars are not set."""
    # These should always have defaults
    assert Config.SOURCE_DB_PATH == "data/source/retail_source.db"
    assert Config.RAW_OUTPUT_PATH == "data/raw"
    assert Config.INGESTION_BATCH_SIZE == 500
    assert Config.UNITY_CATALOG == "retail"
    assert Config.BRONZE_SCHEMA == "bronze"
    assert Config.SILVER_SCHEMA == "silver"
    assert Config.GOLD_SCHEMA == "gold"


def test_config_volume_path():
    """Verify Volume path is set correctly."""
    path = Config.VOLUME_SOURCE_PATH
    assert path is not None
    assert "/Volumes/" in path or "dbfs:" in path
    assert "retail" in path.lower()


def test_config_env_override(monkeypatch):
    """Test that Config respects environment variable overrides."""
    monkeypatch.setenv("UNITY_CATALOG", "test_catalog")

    # Reimport to pick up new env var
    import importlib

    import src.utils.config

    importlib.reload(src.utils.config)

    assert src.utils.config.Config.UNITY_CATALOG == "test_catalog"
