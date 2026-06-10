# Databricks Medallion Pipeline — Phase 2 Complete

## Overview

Fully automated data pipeline following medallion architecture (Bronze → Silver → Gold) on Databricks with Unity Catalog. All logic migrated from notebooks to Python modules for production orchestration.

## Architecture

```
SQLite (source_db)
    ↓
[raw_extract.py] → JSONL files → Databricks Volume (/Volumes/retail/raw/source_data)
    ↓
[bronze.py] → Bronze Delta (raw, append-only, normalized timestamps)
    ↓
[silver.py] → Silver Delta (deduplicated, conformed, MERGE-based SCD Type 1)
    ↓
[gold.py] → Gold Delta (star schema: dims + fact) + KPI views + OPTIMIZE
```

## Running Locally (Dev)

### Unit Tests

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run all unit tests
pytest tests/unit -v

# Run with coverage report
pytest tests/unit --cov=src --cov-report=html
```

**Tests cover:**
- `test_config.py`: Configuration loading and defaults
- `test_logger.py`: Logger factory and formatting
- `test_ingestion.py`: Watermark save/load (JSON operations)

### Code Quality

```bash
# Install linting dependencies
pip install black flake8 isort

# Auto-format code
black src tests

# Sort imports
isort src tests

# Lint
flake8 src tests
```

## Running in Databricks

### Option A: Manual Python Task (Dev/Testing)

Create a new Job task:
- **Type:** Python
- **Source:** Workspace file
- **Path:** `/Repos/<your-repo>/src/transformation/bronze/bronze.py` (or silver/gold)

### Option B: Automated Job DAG (Production)

Deploy the multi-task job:

```bash
# Using Databricks CLI
databricks jobs create --json-file infra/jobs/medallion_pipeline.yml

# Or update if exists:
databricks jobs update --job-id <job-id> --json-file infra/jobs/medallion_pipeline.yml

# Trigger a run:
databricks jobs run-now --job-id <job-id>
```

**Job structure (infra/jobs/medallion_pipeline.yml):**
- Task 1: Bronze (depends on: nothing) → append raw into Bronze tables
- Task 2: Silver (depends on: Bronze) → dedup & MERGE into Silver tables
- Task 3: Gold (depends on: Silver) → build star schema, dims, fact, KPI views
- Schedule: Daily at 2 AM UTC
- Notifications: Email on success/failure
- Max concurrent runs: 1 (serialized)

## Configuration

### Environment Variables (.env)

```env
# Source database
SOURCE_DB_PATH=data/source/retail_source.db
RAW_OUTPUT_PATH=data/raw

# Databricks
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=<your-pat-token>
DATABRICKS_WAREHOUSE_ID=<your-warehouse-id>

# Unity Catalog
UNITY_CATALOG=retail
BRONZE_SCHEMA=bronze
SILVER_SCHEMA=silver
GOLD_SCHEMA=gold

# Volume path for raw JSONL
VOLUME_SOURCE_PATH=/Volumes/retail/raw/source_data
```

All configs have sensible defaults in [src/utils/config.py](src/utils/config.py).

## CI/CD Pipeline (.github/workflows/ci.yml)

Runs on every push to `main` and all PRs:

1. **Lint** (flake8, black, isort)
   - Code format & style checks
   - Stop build on syntax errors

2. **Unit Tests** (pytest)
   - Config, logger, ingestion modules
   - Requires lint to pass
   - Uploads coverage to Codecov

3. **Security** (bandit)
   - Static security scanning
   - Non-blocking (informational)

All steps run on `ubuntu-latest`.

## Project Structure

```
src/
├── transformation/
│   ├── bronze/
│   │   ├── bronze.py       # Ingest from Volume → Bronze Delta
│   │   └── bronze.ipynb    # Reference notebook (legacy)
│   ├── silver/
│   │   ├── silver.py       # Bronze → dedupe → Silver Delta
│   │   └── silver.ipynb    # Reference notebook (legacy)
│   ├── gold/
│   │   ├── gold.py         # Silver → star schema → Gold Delta
│   │   └── gold.ipynb      # Reference notebook (legacy)
│   ├── __init__.py
├── ingestion/
│   ├── raw_extract.py      # SQLite → JSONL watermark-based extraction
│   └── upload_to_volume.py # JSONL → Databricks Volume
├── utils/
│   ├── config.py           # Configuration from environment
│   ├── logger.py           # Shared logger factory
│   └── __init__.py
├── generation/
│   └── ...                 # SQLite source generation & scheduler
└── quality/
    └── validators.py       # Placeholder for Phase 3 (reusable DQ)

tests/
├── unit/
│   ├── test_config.py
│   ├── test_logger.py
│   ├── test_ingestion.py
│   └── __init__.py
├── integration/            # Placeholder for Phase 3
└── __init__.py

infra/
└── jobs/
    └── medallion_pipeline.yml  # Databricks Job DAG config

.github/workflows/
└── ci.yml                  # GitHub Actions CI pipeline

pytest.ini                  # Pytest configuration
requirements.txt           # Python dependencies
.env.example              # Env var template
```

## Key Decisions

1. **No displays, only logging** — All `display()` calls replaced with `logger.info/warning/error` for production-ready output.

2. **Entry points for Jobs** — Each layer (bronze.py, silver.py, gold.py) has `if __name__ == "__main__"` guard and `run(spark)` orchestrator function.

3. **Notebooks kept as reference** — Legacy `.ipynb` files remain for now; will be archived after stable production runs.

4. **Fail-fast policy** — Validation errors raise `RuntimeError` and stop execution (not warnings).

5. **SCD Type 1 throughout** — Silver and Gold use MERGE upserts with simple overwrite-on-match logic.

6. **Static date dimension** — `dim_date` is rebuilt (overwrite) each run for 2020-2030 range.

7. **Star schema design** — One fact table (`fact_order_line`, grain = order_item_id) + 3 dims (customer, product, date); LEFT JOIN products to retain soft-deleted items.

8. **Performance tuning** — Fact table is OPTIMIZE'd with ZORDER by (date_id, customer_id); dimensions are simply OPTIMIZE'd.

## Next Steps (Phase 3 onwards)

- **Phase 3:** Implement reusable validators in [src/quality/validators.py](src/quality/validators.py) and integrate into Silver/Gold pipelines.
- **Phase 4:** Add integration tests for Spark transformations (requires Databricks Connect or local Spark).
- **Monitoring:** Emit pipeline metrics to Databricks workspace logs or external observability platform.

## Troubleshooting

**Bronze fails with "No SparkSession":**
- Running outside Databricks? Bronze.py requires Databricks runtime.
- If testing locally, mock SparkSession or use Databricks Connect.

**MERGE errors (Silver/Gold):**
- Ensure source tables exist in Bronze/Silver first.
- Check primary key column names match configuration.

**KPI views missing data:**
- Verify Gold fact and dims are populated.
- Check for NULL values in foreign key columns.

**CI fails on lint:**
- Run `black src tests && isort src tests` locally to auto-fix.
- Check flake8 errors with `flake8 src tests`.

---

For questions or issues, review logs in Databricks Job run history or local test output.
