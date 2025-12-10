Revature ETL project by Burcu Bas

## What this project does (simple)
- Reads customer and sales data from CSV and JSON files in `data/`.
- Uses rules in `config/etl.yml` to check every row (required fields, valid email, allowed currency, etc).
- Fixes column types so numbers, dates, and text match the schema before loading.
- Loads good rows into PostgreSQL staging tables (`stg_customers`, `stg_sales`).
- Saves bad rows into `stg_rejects` with the rule that failed and the raw row content.
- Moves processed files into `data/archive` and loops again every 30 seconds.

## Layers
- **Config**: `config/etl.yml` holds DB creds, file paths, schemas, and validation rules.
- **Read**: `src/readers/*_reader.py` pulls CSV/JSON into pandas DataFrames.
- **Validate**: `src/validator.py` checks required columns and rule expressions; splits good vs rejected rows.
- **Clean**: `src/cleaner.py` coerces columns to the types defined in the schema.
- **Load**: `src/loader.py` writes good rows to staging tables and rejected rows to `stg_rejects`.
- **Orchestrate**: `src/main.py` loops through sources, calls each step, archives processed files, and waits 30 seconds between cycles.

## How to run
- Make sure PostgreSQL is running and matches the creds in `config/etl.yml`.
- Install deps: `pip install -r requirements.txt`
- Place input files at the paths listed under `sources` in `config/etl.yml`.
- Start the loop: `python src/main.py`

## How to test
- Install deps: `pip install -r requirements.txt`
- Run: `pytest -q`
