# NYC Taxi Rides - dbt Project

Downloads NYC yellow and green taxi trip data (2019-2020) from the DataTalksClub GitHub releases, converts to Parquet, loads into DuckDB, and transforms with dbt.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

Install dependencies:

```bash
uv sync
```

### GitHub Token (optional, recommended)

To avoid GitHub API rate limits, provide a personal access token. Either create a `.env` file:

```
GITHUB_TOKEN=ghp_your_token_here
```

Or export it directly:

```bash
export GITHUB_TOKEN=ghp_your_token_here
```

An exported env var takes priority over the `.env` file.

## Configuration

The download is configured via `download_config.yml`:

```yaml
taxi_types:
  - yellow
  - green

years:
  - 2019
  - 2020

months: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
```

The cartesian product of these lists determines which files are downloaded.

## Usage

Download all files and load into DuckDB:

```bash
uv run python download_data.py
```

Download only green taxi data:

```bash
uv run python download_data.py --taxi-type green
```

Download a single file:

```bash
uv run python download_data.py --taxi-type green --year 2020 --month 6
```

Download without loading into DuckDB:

```bash
uv run python download_data.py --no-load
```

Use a custom config file:

```bash
uv run python download_data.py --config my_config.yml
```

See all options:

```bash
uv run python download_data.py --help
```

## dbt Workflow

After downloading data:

```bash
uv run dbt deps
uv run dbt seed
uv run dbt run
uv run dbt test
```
