# NYC Taxi Rides - dbt Project

A dbt project for analyzing NYC yellow and green taxi trip data (2019-2020). Uses DuckDB for local development and supports BigQuery for production. Follows a medallion architecture pattern (staging, intermediate, marts) with a star schema output.

## "Show your work" queries for homework
```sql
select count(*) from prod.fct_monthly_zone_revenue;

select
    f.pickup_zone,
    d.zone,
    sum(f.revenue_monthly_total_amount) as annual_revenue
from
    prod.fct_monthly_zone_revenue f
    join prod.dim_zones d on f.pickup_zone = d.zone
where
    year(f.revenue_month) = 2020
    and
    f.pickup_zone in ('East Harlem North','East Harlem South','Morningside Heights','Washington Heights')
    and
    f.service_type = 'Green'
group by f.pickup_zone, d.zone
order by sum(f.revenue_monthly_total_amount) desc;

select
    sum(total_monthly_trips)
from
    prod.fct_monthly_zone_revenue
where
    revenue_month = '2019-10-01'
    and
    service_type = 'Green';

SELECT count(*) as total_rows FROM prod.stg_fhv_tripdata;




```
## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

```bash
uv sync
```

### GitHub Token (optional, recommended)

To avoid GitHub API rate limits when downloading data, provide a personal access token. Either create a `.env` file:

```
GITHUB_TOKEN=ghp_your_token_here
```

Or export it directly:

```bash
export GITHUB_TOKEN=ghp_your_token_here
```

An exported env var takes priority over the `.env` file.

### dbt Profile

Create a `profiles.yml` file in the project root (not committed to git):

```yaml
taxi_rides_ny:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: taxi_rides_ny.duckdb
```

## Downloading Data

The `download_data.py` script downloads taxi trip data from the DataTalksClub GitHub releases, converts CSV.gz files to Parquet, and loads them into DuckDB. Downloads run concurrently (4 at a time) with progress bars.

### Download Configuration

Configure which files to download in `download_config.yml`. Each entry in the `datasets` list expands its own cartesian product of `taxi_types × years × months`. Duplicates across groups are removed automatically.

```yaml
datasets:
  - taxi_types: [yellow, green]
    years: [2019, 2020]
    months: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
  - taxi_types: [fhv]
    years: [2019]
    months: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
```

This downloads yellow/green for 2019-2020 (48 files) and fhv for 2019 only (12 files) — 60 files total. CLI args (`--taxi-type`, `--year`, `--month`) act as filters on the expanded list rather than overrides.

### Download Commands

```bash
# Download everything and load into DuckDB
uv run python download_data.py

# Preview what would be downloaded
uv run python download_data.py --dry-run

# Filter to green taxi data for 2020
uv run python download_data.py --taxi-type green --year 2020

# Filter to a single file
uv run python download_data.py --taxi-type green --year 2020 --month 6

# Re-download all files, even if they already exist
uv run python download_data.py --force

# Download without loading into DuckDB
uv run python download_data.py --no-load

# Use a custom config file
uv run python download_data.py --config my_config.yml
```

Run `uv run python download_data.py --help` for all options.

## dbt Project

### Running dbt

```bash
# Install dbt packages (dbt_utils, codegen)
uv run dbt deps

# Load seed data (zone lookup, payment type lookup)
uv run dbt seed

# Run all models
uv run dbt run

# Run in dev mode (samples data to Jan 2019 only)
uv run dbt run --target dev

# Run a specific model
uv run dbt run --select fct_trips

# Run tests
uv run dbt test

# Generate and serve documentation
uv run dbt docs generate && uv run dbt docs serve
```

### Data Architecture

The project follows a medallion architecture with four layers:

```
Raw Sources (prod.green_tripdata, prod.yellow_tripdata)
    |
Staging ── views, type casting, null filtering
    |       stg_green_tripdata, stg_yellow_tripdata
    |
Intermediate ── union, surrogate keys, deduplication
    |       int_trips_unioned → int_trips
    |
Marts ── star schema fact/dimension tables
    |       fct_trips, dim_zones, dim_vendors
    |
Reporting ── aggregated business metrics
            fct_monthly_zone_revenue
```

### Models

**Staging** (materialized as views)
- `stg_green_tripdata` — Standardizes column names, casts types, filters null vendors. Uses `safe_cast()` macro for BigQuery compatibility.
- `stg_yellow_tripdata` — Same standardization for yellow taxi data.
- In dev target, both filter to a configurable date range (default: January 2019).

**Intermediate** (materialized as tables)
- `int_trips_unioned` — Unions green and yellow staging data, normalizes schema differences (adds `service_type` column, fills green-only fields for yellow).
- `int_trips` — Generates surrogate `trip_id` key, joins payment type descriptions from seed, deduplicates on natural key.

**Marts** (materialized as tables)
- `fct_trips` — Core fact table (incremental, merge strategy). Joins zone names/boroughs from `dim_zones`, adds computed `trip_duration_minutes`.
- `dim_zones` — Zone dimension from `taxi_zone_lookup` seed (265 NYC TLC zones with borough and service zone).
- `dim_vendors` — Vendor dimension derived from `fct_trips`, maps IDs to names (Creative Mobile Technologies, VeriFone Inc.).

**Reporting**
- `fct_monthly_zone_revenue` — Monthly revenue aggregation by pickup zone and service type. Includes fare breakdowns, trip counts, and averages.

### Seeds

| File                       | Rows | Description                                              |
|----------------------------|------|----------------------------------------------------------|
| `taxi_zone_lookup.csv`     | 265  | NYC TLC zones with borough, zone name, and service zone  |
| `payment_type_lookup.csv`  | 7    | Payment method codes (credit card, cash, dispute, etc.)  |

### Custom Macros

| Macro                                        | Purpose                                                               |
|----------------------------------------------|-----------------------------------------------------------------------|
| `safe_cast(column, type)`                    | Cross-database type casting (BigQuery `safe_cast` vs standard `cast`) |
| `get_vendor_data(column)`                    | Maps vendor IDs to company names via CASE statement                   |
| `get_trip_duration_minutes(pickup, dropoff)` | Calculates trip duration in minutes using `datediff`                  |

### Tests

Tests are defined in `schema.yml` files alongside models:

- **Uniqueness and not-null** on primary keys (`trip_id`, `vendor_id`, `location_id`)
- **Accepted values** on categorical fields (`service_type` must be Green or Yellow)
- **Relationships** for foreign key integrity (pickup/dropoff location IDs reference `dim_zones`)
- **Unique combination of columns** on `fct_monthly_zone_revenue` (zone + month + service type)

## Testing

Run the `download_data.py` test suite with:

```bash
uv run pytest -v
```

### What we test and why

We focus on the **pure logic and input boundaries** — the code we actually wrote:

- **`validate_config`** — Config validation is the most important thing to test: bad input should fail fast with clear errors, and multiple errors should be reported together.
- **`build_file_list`** — The cartesian product logic that combines config values with CLI filters. Easy to get wrong, cheap to test.
- **`load_config`** — Basic YAML loading and the missing-file error path.
- **`parse_args`** — Verifies defaults, flag behavior, type coercion, and that argparse rejects invalid values.
- **`get_github_headers`** — Tests the env-var boundary: token present vs. absent.
- **`update_gitignore`** — File creation, append, and no-op cases using pytest's `tmp_path`.
- **`download_all_files`** — One integration-style test verifying the abort-after-N-consecutive-failures safety net.

### What we skip and why

We deliberately skip thin I/O wrappers like `download_file`, `convert_to_parquet`, and `load_into_duckdb`. These are mostly calls to httpx, DuckDB, and rich — testing them would just be testing mock wiring, not our logic. They are better covered by manual runs and integration tests against real data.
