# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a dbt project for analyzing NYC taxi trip data (yellow and green taxis). It uses DuckDB as the local development database and supports BigQuery for production. The project follows a medallion architecture pattern with staging, intermediate, and marts layers.

## Common Commands

```bash
# Install dependencies (uses uv package manager)
uv sync

# Download and prepare taxi data (creates DuckDB database with 2019-2020 data)
# Set GITHUB_TOKEN env var to avoid rate limiting
uv run python download_data.py

# Install dbt packages
uv run dbt deps

# Run all models
uv run dbt run

# Run a specific model
uv run dbt run --select fct_trips

# Run models in dev mode (uses date sampling defined in dbt_project.yml)
uv run dbt run --target dev

# Run tests
uv run dbt test

# Test a specific model
uv run dbt test --select fct_trips

# Load seed data
uv run dbt seed

# Generate documentation
uv run dbt docs generate && uv run dbt docs serve
```

## Architecture

### Data Flow
```
Raw Sources (green_tripdata, yellow_tripdata)
    ↓
Staging (stg_green_tripdata, stg_yellow_tripdata) - views, type casting, filtering
    ↓
Intermediate (int_trips_unioned → int_trips) - union, enrichment, deduplication
    ↓
Marts (fct_trips, dim_zones, dim_vendors) - star schema fact/dimension tables
    ↓
Reporting (fct_monthly_zone_revenue) - aggregated business metrics
```

### Key Design Patterns

- **Multi-database support**: Sources and macros use `target.type` conditionals for BigQuery vs DuckDB compatibility
- **Dev sampling**: Staging models filter to January 2019 in dev target (configurable via `dev_start_date`/`dev_end_date` vars)
- **Incremental loading**: `fct_trips` uses incremental materialization with merge strategy
- **Surrogate keys**: Generated via `dbt_utils.generate_surrogate_key()` in `int_trips`

### Seeds
- `taxi_zone_lookup.csv`: NYC TLC zone reference data (265 zones)
- `payment_type_lookup.csv`: Payment method codes and descriptions

### Custom Macros
- `safe_cast()`: Uses BigQuery's `safe_cast` or standard `cast` based on target
- `get_vendor_data()`: Maps vendor IDs to names via CASE statement
- `get_trip_duration_minutes()`: Calculates trip duration from timestamps

## Database Setup

The project requires a `profiles.yml` file (not committed). For local DuckDB development:

```yaml
taxi_rides_ny:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: taxi_rides_ny.duckdb
```

For BigQuery, set the `GCP_PROJECT_ID` environment variable.
