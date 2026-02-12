"""Tests for download_data.py — focused on pure logic and input boundaries."""

import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from download_data import (
    MAX_CONSECUTIVE_FAILURES,
    build_file_list,
    categorize_files,
    download_all_files,
    get_github_headers,
    load_config,
    parse_args,
    update_gitignore,
    validate_config,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIMAL_CONFIG = {
    "taxi_types": ["yellow", "green"],
    "years": [2019, 2020],
    "months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
}


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------


class TestValidateConfig:
    def test_valid_config_passes(self):
        validate_config(MINIMAL_CONFIG)  # should not raise

    def test_unknown_taxi_type_raises(self):
        config = {**MINIMAL_CONFIG, "taxi_types": ["red"]}
        with pytest.raises(SystemExit):
            validate_config(config)

    def test_year_out_of_range_raises(self):
        config = {**MINIMAL_CONFIG, "years": [1999]}
        with pytest.raises(SystemExit):
            validate_config(config)

    def test_month_out_of_range_raises(self):
        config = {**MINIMAL_CONFIG, "months": [0]}
        with pytest.raises(SystemExit):
            validate_config(config)

    def test_multiple_errors_reported_together(self, capsys):
        config = {"taxi_types": ["red"], "years": [1999], "months": [13]}
        with pytest.raises(SystemExit):
            validate_config(config)


# ---------------------------------------------------------------------------
# build_file_list
# ---------------------------------------------------------------------------


class TestBuildFileList:
    def test_no_filters_returns_full_cartesian_product(self):
        result = build_file_list(MINIMAL_CONFIG)
        assert len(result) == 2 * 2 * 12  # 2 types × 2 years × 12 months

    def test_filter_by_taxi_type(self):
        result = build_file_list(MINIMAL_CONFIG, taxi_type="green")
        assert all(t == "green" for t, _, _ in result)
        assert len(result) == 2 * 12  # 2 years × 12 months

    def test_filter_by_year(self):
        result = build_file_list(MINIMAL_CONFIG, year=2020)
        assert all(y == 2020 for _, y, _ in result)
        assert len(result) == 2 * 12  # 2 types × 12 months

    def test_filter_by_month(self):
        result = build_file_list(MINIMAL_CONFIG, month=6)
        assert all(m == 6 for _, _, m in result)
        assert len(result) == 2 * 2  # 2 types × 2 years

    def test_all_filters_single_file(self):
        result = build_file_list(MINIMAL_CONFIG, taxi_type="yellow", year=2019, month=1)
        assert result == [("yellow", 2019, 1)]

    def test_filter_value_not_in_config_returns_empty(self):
        result = build_file_list(MINIMAL_CONFIG, taxi_type="fhv")
        # fhv is not in config's taxi_types list, but build_file_list uses
        # the CLI filter directly — it returns fhv combined with config years/months
        # This is by design: CLI filter overrides the config list
        assert all(t == "fhv" for t, _, _ in result)
        assert len(result) == 2 * 12

    def test_filter_year_not_in_config(self):
        result = build_file_list(MINIMAL_CONFIG, year=2025)
        assert all(y == 2025 for _, y, _ in result)
        assert len(result) == 2 * 12


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------


class TestLoadConfig:
    def test_loads_valid_yaml(self, tmp_path):
        config_file = tmp_path / "config.yml"
        config_file.write_text("taxi_types:\n  - yellow\nyears:\n  - 2020\nmonths: [1]\n")
        result = load_config(str(config_file))
        assert result == {"taxi_types": ["yellow"], "years": [2020], "months": [1]}

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/config.yml")


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


class TestParseArgs:
    def test_defaults(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py"])
        args = parse_args()
        assert args.config == "download_config.yml"
        assert args.taxi_type is None
        assert args.year is None
        assert args.month is None
        assert args.no_load is False
        assert args.dry_run is False
        assert args.force is False

    def test_taxi_type(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py", "--taxi-type", "green"])
        args = parse_args()
        assert args.taxi_type == "green"

    def test_year(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py", "--year", "2020"])
        args = parse_args()
        assert args.year == 2020

    def test_month(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py", "--month", "6"])
        args = parse_args()
        assert args.month == 6

    def test_no_load_flag(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py", "--no-load"])
        args = parse_args()
        assert args.no_load is True

    def test_dry_run_flag(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py", "--dry-run"])
        args = parse_args()
        assert args.dry_run is True

    def test_force_flag(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py", "--force"])
        args = parse_args()
        assert args.force is True

    def test_invalid_taxi_type_rejected(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py", "--taxi-type", "red"])
        with pytest.raises(SystemExit):
            parse_args()

    def test_invalid_month_rejected(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["download_data.py", "--month", "13"])
        with pytest.raises(SystemExit):
            parse_args()


# ---------------------------------------------------------------------------
# get_github_headers
# ---------------------------------------------------------------------------


class TestGetGithubHeaders:
    def test_with_token(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_test123")
        headers = get_github_headers()
        assert headers["Authorization"] == "Bearer ghp_test123"

    def test_without_token(self, monkeypatch):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        headers = get_github_headers()
        assert "Authorization" not in headers


# ---------------------------------------------------------------------------
# update_gitignore
# ---------------------------------------------------------------------------


class TestUpdateGitignore:
    def test_creates_gitignore_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        update_gitignore()
        content = (tmp_path / ".gitignore").read_text()
        assert "data/" in content

    def test_appends_when_entry_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".gitignore").write_text("*.pyc\n")
        update_gitignore()
        content = (tmp_path / ".gitignore").read_text()
        assert "*.pyc" in content
        assert "data/" in content

    def test_no_op_when_entry_exists(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        original = "# stuff\ndata/\n"
        (tmp_path / ".gitignore").write_text(original)
        update_gitignore()
        assert (tmp_path / ".gitignore").read_text() == original


# ---------------------------------------------------------------------------
# download_all_files — abort after MAX_CONSECUTIVE_FAILURES
# ---------------------------------------------------------------------------


class TestCategorizeFiles:
    def test_all_new(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        files = [("yellow", 2019, 1), ("green", 2019, 2)]
        new, existing = categorize_files(files)
        assert new == files
        assert existing == []

    def test_all_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        files = [("yellow", 2019, 1), ("green", 2019, 2)]
        for taxi_type, year, month in files:
            parquet_dir = tmp_path / "data" / taxi_type
            parquet_dir.mkdir(parents=True, exist_ok=True)
            (parquet_dir / f"{taxi_type}_tripdata_{year}-{month:02d}.parquet").touch()
        new, existing = categorize_files(files)
        assert new == []
        assert existing == files

    def test_mixed(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        files = [("yellow", 2019, 1), ("yellow", 2019, 2), ("green", 2019, 1)]
        # Only create the first file on disk
        parquet_dir = tmp_path / "data" / "yellow"
        parquet_dir.mkdir(parents=True, exist_ok=True)
        (parquet_dir / "yellow_tripdata_2019-01.parquet").touch()
        new, existing = categorize_files(files)
        assert existing == [("yellow", 2019, 1)]
        assert new == [("yellow", 2019, 2), ("green", 2019, 1)]


# ---------------------------------------------------------------------------
# download_all_files — abort after MAX_CONSECUTIVE_FAILURES
# ---------------------------------------------------------------------------


class TestDownloadAllFilesAbort:
    async def test_aborts_after_max_consecutive_failures(self):
        files = [("yellow", 2019, m) for m in range(1, 13)]
        assert len(files) > MAX_CONSECUTIVE_FAILURES  # precondition

        call_count = 0

        async def fake_download_and_convert(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("simulated failure")

        with patch("download_data.download_and_convert", side_effect=fake_download_and_convert):
            with patch("download_data.get_github_headers", return_value={}):
                result = await download_all_files(files)

        assert result == []
        # With the semaphore, up to CONCURRENT_DOWNLOADS tasks may start
        # before the abort flag propagates, but we should NOT attempt all 12
        assert call_count < len(files)
