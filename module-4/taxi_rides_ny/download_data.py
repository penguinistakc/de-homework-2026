import argparse
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import httpx
import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
CONCURRENT_DOWNLOADS = 4
CHUNK_SIZE = 64 * 1024  # 64KB chunks
KNOWN_TAXI_TYPES = {"yellow", "green", "fhv", "fhvhv"}
MAX_CONSECUTIVE_FAILURES = 5

console = Console()


def get_github_headers() -> dict[str, str]:
    """Get headers for GitHub API requests, including auth token if available."""
    headers = {
        "Accept": "application/octet-stream",
        "User-Agent": "taxi-rides-ny-downloader",
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
        console.print("[green]Using GitHub token for authenticated requests[/green]")
    else:
        console.print(
            "[yellow]No GITHUB_TOKEN found - using unauthenticated requests "
            "(may be rate limited)[/yellow]"
        )
    return headers


def convert_to_parquet(csv_gz_path: Path, parquet_path: Path) -> None:
    """Convert a CSV.gz file to Parquet format using DuckDB."""
    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (SELECT * FROM read_csv_auto('{csv_gz_path}'))
            TO '{parquet_path}' (FORMAT PARQUET)
        """)
    finally:
        con.close()
    # Remove the CSV.gz file to save space
    csv_gz_path.unlink()


async def download_file(
    client: httpx.AsyncClient,
    url: str,
    dest_path: Path,
    progress: Progress,
    task_id: TaskID,
) -> None:
    """Download a file with progress tracking."""
    async with client.stream("GET", url, follow_redirects=True) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        progress.update(task_id, total=total if total else None)

        with open(dest_path, "wb") as f:
            async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                progress.update(task_id, advance=len(chunk))


async def download_and_convert(
    client: httpx.AsyncClient,
    executor: ThreadPoolExecutor,
    taxi_type: str,
    year: int,
    month: int,
    progress: Progress,
    force: bool = False,
) -> Path | None:
    """Download a single file and convert it to Parquet."""
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    parquet_path = data_dir / parquet_filename

    if parquet_path.exists():
        if force:
            parquet_path.unlink()
            progress.console.print(f"[yellow]Re-downloading {parquet_filename} (--force)[/yellow]")
        else:
            progress.console.print(f"[dim]Skipping {parquet_filename} (already exists)[/dim]")
            return parquet_path

    csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
    csv_gz_path = data_dir / csv_gz_filename
    url = f"{BASE_URL}/{taxi_type}/{csv_gz_filename}"

    # Create progress task for this download
    task_id = progress.add_task(
        f"[cyan]{csv_gz_filename}",
        total=None,
        start=True,
    )

    try:
        await download_file(client, url, csv_gz_path, progress, task_id)
        progress.update(task_id, description=f"[yellow]{csv_gz_filename} (converting)")

        # Run parquet conversion in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            executor,
            convert_to_parquet,
            csv_gz_path,
            parquet_path,
        )

        progress.update(task_id, description=f"[green]{parquet_filename} ✓")
        return parquet_path

    except httpx.HTTPStatusError as e:
        progress.update(task_id, description=f"[red]{csv_gz_filename} (failed: {e.response.status_code})")
        raise
    except Exception:
        progress.update(task_id, description=f"[red]{csv_gz_filename} (failed)")
        raise


async def download_all_files(
    files_to_download: list[tuple[str, int, int]], force: bool = False
) -> list[Path]:
    """Download all taxi data files concurrently."""
    headers = get_github_headers()
    semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
    consecutive_failures = 0
    aborted = False

    console.print(f"\n[bold]Downloading {len(files_to_download)} files...[/bold]\n")

    async with httpx.AsyncClient(headers=headers, timeout=300.0) as client:
        with ThreadPoolExecutor(max_workers=CONCURRENT_DOWNLOADS) as executor:
            with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(bar_width=30),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console,
                transient=False,
            ) as progress:

                async def download_with_tracking(
                    taxi_type: str, year: int, month: int
                ) -> Path | Exception | None:
                    nonlocal consecutive_failures, aborted
                    async with semaphore:
                        if aborted:
                            return None
                        try:
                            result = await download_and_convert(
                                client, executor, taxi_type, year, month, progress, force=force
                            )
                            consecutive_failures = 0
                            return result
                        except Exception as e:
                            consecutive_failures += 1
                            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                                aborted = True
                                progress.console.print(
                                    f"\n[red bold]Aborting: {consecutive_failures} consecutive "
                                    f"download failures — check your config[/red bold]"
                                )
                            return e

                tasks = [
                    download_with_tracking(taxi_type, year, month)
                    for taxi_type, year, month in files_to_download
                ]
                results = await asyncio.gather(*tasks)

    # Filter out exceptions and None values
    successful_paths = [r for r in results if isinstance(r, Path)]
    failed_count = len([r for r in results if isinstance(r, Exception)])

    if failed_count:
        console.print(f"\n[red]Failed to download {failed_count} files[/red]")

    return successful_paths


def load_into_duckdb(db_path: str = "taxi_rides_ny.duckdb") -> None:
    """Load all parquet files into DuckDB."""
    console.print(f"\n[bold]Loading data into {db_path}...[/bold]")

    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS prod")

        for taxi_type in ["yellow", "green"]:
            data_dir = Path("data") / taxi_type
            if not data_dir.exists():
                continue

            parquet_files = list(data_dir.glob("*.parquet"))
            if not parquet_files:
                continue

            console.print(f"  Loading {len(parquet_files)} {taxi_type} taxi files...")
            con.execute(f"""
                CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
                SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
            """)

            row_count = con.execute(
                f"SELECT COUNT(*) FROM prod.{taxi_type}_tripdata"
            ).fetchone()[0]
            console.print(f"  [green]Loaded {row_count:,} {taxi_type} taxi records[/green]")
    finally:
        con.close()


def update_gitignore() -> None:
    """Ensure data directory is in .gitignore."""
    gitignore_path = Path(".gitignore")
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    if "data/" not in content:
        with open(gitignore_path, "a") as f:
            f.write("\n# Data directory\ndata/\n" if content else "# Data directory\ndata/\n")


def load_config(config_path: str) -> dict:
    """Load download configuration from a YAML file."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def validate_config(config: dict) -> None:
    """Validate config values and exit with an error if invalid."""
    errors = []

    unknown_types = set(config["taxi_types"]) - KNOWN_TAXI_TYPES
    if unknown_types:
        errors.append(
            f"Unknown taxi type(s): {', '.join(sorted(unknown_types))}. "
            f"Valid types: {', '.join(sorted(KNOWN_TAXI_TYPES))}"
        )

    for year in config["years"]:
        if not (2009 <= year <= 2030):
            errors.append(f"Year {year} is outside the valid range (2009-2030)")

    for month in config["months"]:
        if not (1 <= month <= 12):
            errors.append(f"Month {month} is outside the valid range (1-12)")

    if errors:
        console.print("[red bold]Config validation failed:[/red bold]")
        for error in errors:
            console.print(f"  [red]- {error}[/red]")
        raise SystemExit(1)


def build_file_list(
    config: dict,
    taxi_type: str | None = None,
    year: int | None = None,
    month: int | None = None,
) -> list[tuple[str, int, int]]:
    """Build the list of files to download from config, filtered by CLI args."""
    taxi_types = [taxi_type] if taxi_type else config["taxi_types"]
    years = [year] if year else config["years"]
    months = [month] if month else config["months"]

    return [
        (t, y, m)
        for t in taxi_types
        for y in years
        for m in months
    ]


def categorize_files(
    files: list[tuple[str, int, int]],
) -> tuple[list[tuple[str, int, int]], list[tuple[str, int, int]]]:
    """Split file list into (new, existing) based on whether parquet files exist on disk."""
    new, existing = [], []
    for entry in files:
        taxi_type, year, month = entry
        parquet_path = Path("data") / taxi_type / f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        (existing if parquet_path.exists() else new).append(entry)
    return new, existing


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download NYC taxi trip data and load into DuckDB."
    )
    parser.add_argument(
        "--config",
        default="download_config.yml",
        help="Path to YAML config file (default: download_config.yml)",
    )
    parser.add_argument(
        "--taxi-type",
        choices=["yellow", "green"],
        help="Download only this taxi type",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Download only this year (e.g. 2020)",
    )
    parser.add_argument(
        "--month",
        type=int,
        choices=range(1, 13),
        metavar="{1-12}",
        help="Download only this month (1-12)",
    )
    parser.add_argument(
        "--no-load",
        action="store_true",
        help="Skip loading data into DuckDB after download",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show files that would be downloaded, without downloading",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download and overwrite existing parquet files",
    )
    return parser.parse_args()


async def main() -> None:
    """Main entry point."""
    load_dotenv()
    args = parse_args()

    config = load_config(args.config)
    validate_config(config)
    files_to_download = build_file_list(
        config,
        taxi_type=args.taxi_type,
        year=args.year,
        month=args.month,
    )

    if not files_to_download:
        console.print("[red]No files to download with the given filters.[/red]")
        return

    if args.dry_run:
        new_files, existing_files = categorize_files(files_to_download)
        new_count = len(new_files)
        existing_count = len(existing_files)
        console.print(
            f"\n[bold]Would download {len(files_to_download)} files "
            f"({new_count} new, {existing_count} already exist):[/bold]"
        )
        for taxi_type, year, month in files_to_download:
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            if (taxi_type, year, month) in existing_files:
                if args.force:
                    console.print(f"  [yellow]FORCE[/yellow] {filename}  (will re-download)")
                else:
                    console.print(f"  [dim]SKIP[/dim]  {filename}  (already exists, use --force to re-download)")
            else:
                console.print(f"  [green]NEW[/green]   {filename}")
        return

    update_gitignore()
    await download_all_files(files_to_download, force=args.force)

    if not args.no_load:
        load_into_duckdb()

    console.print("\n[bold green]Done![/bold green]")


if __name__ == "__main__":
    asyncio.run(main())
