# Retail Orders Data Engineering Pipeline

End-to-end interview-prep project focused on SQL, PySpark, Delta Lake, data modeling, and data quality.

## Current Architecture

```
SQLite source (simulated OLTP)
  -> local raw JSONL staging (data/raw)
  -> Unity Catalog Volume upload
  -> Bronze Delta (Databricks)
  -> Silver Delta (Databricks)
  -> Gold marts (Databricks)
```

## Project Structure

```
src/
  generation/
    init_source_db.py      # Creates SQLite schema and indexes
    generate_changes.py    # Simulates incremental source changes
    scheduler.py           # Optional recurring generation jobs
    view_db.py             # Quick source DB viewer
  ingestion/
    raw_extract.py         # SQLite -> data/raw JSONL (watermark incremental)
  transformation/
    bronze.py              # Databricks-side Bronze table creation
    silver.py              # Databricks-side Silver logic (next)
    gold.py                # Databricks-side Gold logic (next)
  quality/
  utils/

data/
  source/                  # SQLite source DB
  raw/                     # Local raw staging output + watermarks.json
```

## Command Runbook

### 1. Environment setup (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. First-time source initialization

```powershell
python src/generation/init_source_db.py
```

### 3. Generate source changes

Run one simulated batch:

```powershell
python src/generation/generate_changes.py
```

Or run scheduler once (all tiers):

```powershell
python src/generation/scheduler.py --once
```

Or keep scheduler running continuously:

```powershell
python src/generation/scheduler.py
```

### 4. View SQLite data quickly

```powershell
python src/generation/view_db.py
```

### 5. Extract source to local raw staging

First run (full load):

```powershell
python src/ingestion/raw_extract.py --full
```

Next runs (incremental by watermark):

```powershell
python src/ingestion/raw_extract.py
```

### 6. Verify local raw output

```powershell
Get-ChildItem data/raw -Recurse
Get-Content data/raw/watermarks.json
```

### 7. Databricks CLI checks

Check CLI availability:

```powershell
databricks --version
```

Configure token auth (legacy CLI v0.x):

```powershell
databricks configure --token
```

Validate access:

```powershell
databricks workspace ls /
```

### 8. Databricks prerequisites (Catalog, Schema, Volumes)

This project assumes Unity Catalog is enabled and you have:

- Catalog: `main`
- Schema: `retail` (required, under catalog `main`)
- Volumes: `main.retail.bronze`, `main.retail.silver`, `main.retail.gold`

Check available schemas:

```powershell
databricks schemas list main
```

Create schema if needed:

```powershell
databricks schemas create retail --catalog-name main
```

Create volumes once (if they do not exist):

```powershell
# Bronze volume (managed)
databricks volumes create main retail bronze MANAGED --comment "Raw retail data files"

# Silver volume (managed)
databricks volumes create main retail silver MANAGED --comment "Cleaned retail data"

# Gold volume (managed)
databricks volumes create main retail gold MANAGED --comment "Aggregated retail data"
```

### 9. Upload local raw files to Bronze volume

```powershell
databricks fs mkdirs dbfs:/Volumes/main/retail/bronze/raw
python src/ingestion/upload_to_volume.py --full --profile DEFAULT
databricks fs ls dbfs:/Volumes/main/retail/bronze/raw
```

## Standard Daily Workflow

```powershell
python src/generation/scheduler.py --once
python src/ingestion/raw_extract.py
python src/ingestion/upload_to_volume.py --profile DEFAULT
```

## .env keys used now

Required for local execution:

- `SOURCE_DB_PATH` (default `data/source/retail_source.db`)
- `RAW_OUTPUT_PATH` (default `data/raw`)
- `INGESTION_BATCH_SIZE` (default `500`)

Reserved for Databricks-related scripts:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_PROFILE` (optional, default `DEFAULT`)

## What is already implemented

- SQLite source schema + synthetic change generation
- Watermark-based incremental extraction to JSONL
- Local raw staging folder with per-table outputs

## Next implementation steps

1. Build Databricks notebook/job to read `dbfs:/Volumes/main/retail/bronze/raw/*` and create Bronze Delta tables.
2. Implement Silver Delta merge logic with `is_deleted` handling and deduplication.
3. Build Gold star schema (`fact_order_line`, dimensions, KPI views).
