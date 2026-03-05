# NYC Taxi Data Downloader

A robust, asynchronous Python script to download NYC Taxi Trip Record data, convert it to Parquet format, and optionally load it into a DuckDB database.

## Prerequisites

- **Python**: 3.13 or higher
- **Dependencies**:
    - `duckdb`: For Parquet conversion and database loading.
    - `httpx`: For asynchronous HTTP requests.
    - `pyyaml`: For parsing the configuration file.
    - `python-dotenv`: For managing environment variables.
    - `rich`: For beautiful terminal progress bars and output.

## Installation

1. Ensure you have [uv](https://github.com/astral-sh/uv) installed (recommended) or use `pip`.
2. Install the required dependencies:

```bash
uv pip install -r pyproject.toml
# OR
pip install duckdb httpx pyyaml python-dotenv rich
```

## Configuration

The script uses a YAML configuration file (`download_config.yml` by default) to specify which datasets to download.

### `download_config.yml` Structure

```yaml
datasets:
  - taxi_types: [yellow, green]
    years: [2019, 2020]
    months: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
  - taxi_types: [fhv]
    years: [2019]
    months: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
```

- `taxi_types`: List of taxi types (`yellow`, `green`, `fhv`, `fhvhv`).
- `years`: List of years to download.
- `months`: List of months (1-12) to download.

### GitHub Token (Optional)

To avoid GitHub API rate limiting, you can provide a GitHub personal access token via an environment variable or a `.env` file:

```env
GITHUB_TOKEN=your_token_here
```

## Usage

Run the script using Python:

```bash
python download_data.py [OPTIONS]
```

### Command-Line Arguments

- `--config PATH`: Path to the YAML configuration file (default: `download_config.yml`).
- `--taxi-type TYPE`: Filter downloads to a specific taxi type (e.g., `yellow`).
- `--year YEAR`: Filter downloads to a specific year (e.g., `2020`).
- `--month {1-12}`: Filter downloads to a specific month (1-12).
- `--raw`: Download raw `.csv.gz` files **without** converting them to Parquet or loading them into DuckDB.
- `--no-load`: Skip loading the downloaded data into DuckDB.
- `--dry-run`: Show which files would be downloaded without actually downloading them.
- `--force`: Re-download and overwrite existing files.

### Examples

**1. Download everything in the config and load into DuckDB:**
```bash
python download_data.py
```

**2. Dry run for a specific year and taxi type:**
```bash
python download_data.py --dry-run --year 2019 --taxi-type yellow
```

**3. Download raw CSV files for a specific month:**
```bash
python download_data.py --raw --month 1
```

**4. Re-download files even if they already exist:**
```bash
python download_data.py --force
```

## Output

- **Data Files**: Downloaded files are stored in the `data/` directory, organized by taxi type.
    - Default: stored as `data/${TAXI_TYPE}/${file_name}.parquet`. (The raw `.csv.gz` is deleted after conversion).
    - With `--raw`: stored as `data/raw/${TAXI_TYPE}/${YEAR}/${MONTH}/${file_name}.csv.gz`.
- **Database**: By default, data is loaded into a DuckDB database named `taxi_rides_ny.duckdb` in the `prod` schema.
- **Git**: The `data/` directory is automatically added to `.gitignore` to prevent committing large data files.
