import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import httpx
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
    semaphore: asyncio.Semaphore,
    executor: ThreadPoolExecutor,
    taxi_type: str,
    year: int,
    month: int,
    progress: Progress,
) -> Path | None:
    """Download a single file and convert it to Parquet."""
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    parquet_path = data_dir / parquet_filename

    if parquet_path.exists():
        progress.console.print(f"[dim]Skipping {parquet_filename} (already exists)[/dim]")
        return parquet_path

    csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
    csv_gz_path = data_dir / csv_gz_filename
    url = f"{BASE_URL}/{taxi_type}/{csv_gz_filename}"

    async with semaphore:
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
        except Exception as e:
            progress.update(task_id, description=f"[red]{csv_gz_filename} (failed)")
            raise


async def download_all_files() -> list[Path]:
    """Download all taxi data files concurrently."""
    headers = get_github_headers()
    semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

    # Create list of all files to download
    files_to_download = [
        (taxi_type, year, month)
        for taxi_type in ["yellow", "green"]
        for year in [2019, 2020]
        for month in range(1, 13)
    ]

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
                tasks = [
                    download_and_convert(
                        client, semaphore, executor, taxi_type, year, month, progress
                    )
                    for taxi_type, year, month in files_to_download
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

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


async def main() -> None:
    """Main entry point."""
    update_gitignore()
    await download_all_files()
    load_into_duckdb()
    console.print("\n[bold green]Done![/bold green]")


if __name__ == "__main__":
    asyncio.run(main())
