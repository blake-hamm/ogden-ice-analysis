"""Tests for the data_loader module."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import polars as pl
import pytest

from ogden_ice_analysis import data_loader
from ogden_ice_analysis.data_loader import (
    AVAILABLE_DATASETS,
    _download_file,
    _ensure_cache_dir,
    _get_cache_path,
    _get_remote_url,
    clear_cache,
    get_cache_info,
    list_datasets,
    load_dataset,
)


class TestListDatasets:
    """Tests for list_datasets function."""

    def test_returns_sorted_list(self) -> None:
        """Test that list_datasets returns a sorted list of available datasets."""
        result = list_datasets()
        assert isinstance(result, list)
        assert result == sorted(result)
        assert all(isinstance(name, str) for name in result)

    def test_returns_all_available_datasets(self) -> None:
        """Test that list_datasets returns all available datasets."""
        result = list_datasets()
        assert set(result) == AVAILABLE_DATASETS


class TestEnsureCacheDir:
    """Tests for _ensure_cache_dir function."""

    def test_creates_directory_if_not_exists(self, tmp_path: Path) -> None:
        """Test that _ensure_cache_dir creates the directory if it doesn't exist."""
        test_cache = tmp_path / ".cache" / "test"

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            result = _ensure_cache_dir()
            assert result == test_cache
            assert test_cache.exists()
            assert test_cache.is_dir()

    def test_returns_existing_directory(self, tmp_path: Path) -> None:
        """Test that _ensure_cache_dir returns existing directory without error."""
        test_cache = tmp_path / ".cache" / "test"
        test_cache.mkdir(parents=True)

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            result = _ensure_cache_dir()
            assert result == test_cache


class TestGetCachePath:
    """Tests for _get_cache_path function."""

    def test_returns_correct_path(self, tmp_path: Path) -> None:
        """Test that _get_cache_path returns the correct cache file path."""
        test_cache = tmp_path / ".cache"

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            result = _get_cache_path("test-dataset")
            assert result == test_cache / "test-dataset.parquet"

    def test_ensures_cache_dir_exists(self, tmp_path: Path) -> None:
        """Test that _get_cache_path ensures cache directory exists."""
        test_cache = tmp_path / ".cache"
        assert not test_cache.exists()

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            _get_cache_path("test-dataset")
            assert test_cache.exists()


class TestGetRemoteUrl:
    """Tests for _get_remote_url function."""

    def test_returns_correct_url(self) -> None:
        """Test that _get_remote_url constructs the correct GitHub URL."""
        result = _get_remote_url("arrests-latest")
        expected = (
            "https://raw.githubusercontent.com/deportationdata/ice/refs/heads/main/data"
            "/arrests-latest.parquet"
        )
        assert result == expected

    def test_url_contains_base_and_dataset(self) -> None:
        """Test that URL contains both base path and dataset name."""
        result = _get_remote_url("test-dataset")
        assert data_loader.REPO_BASE_URL in result
        assert "test-dataset.parquet" in result


class TestLoadDataset:
    """Tests for load_dataset function."""

    def test_raises_error_for_invalid_dataset(self) -> None:
        """Test that load_dataset raises ValueError for unknown dataset."""
        with pytest.raises(ValueError, match="Unknown dataset 'invalid-dataset'"):
            load_dataset("invalid-dataset")

    def test_error_includes_available_datasets(self) -> None:
        """Test that error message includes available datasets."""
        with pytest.raises(ValueError, match="Available:"):
            load_dataset("unknown")

    @patch.object(data_loader, "_download_file")
    def test_downloads_if_not_cached(
        self,
        mock_download: Mock,
        tmp_path: Path,
    ) -> None:
        """Test that dataset is downloaded if not in cache."""
        test_cache = tmp_path / ".cache"
        test_cache.mkdir(parents=True)

        # Create a dummy parquet file for testing
        df = pl.DataFrame({"col1": [1, 2, 3]})
        cache_file = test_cache / "arrests-latest.parquet"
        df.write_parquet(cache_file)

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            # File already exists, so download should not be called
            load_dataset("arrests-latest")
            mock_download.assert_not_called()

    @patch.object(data_loader, "_download_file")
    def test_uses_cache_if_available(
        self,
        mock_download: Mock,
        tmp_path: Path,
    ) -> None:
        """Test that cached file is used without re-downloading."""
        test_cache = tmp_path / ".cache"
        test_cache.mkdir(parents=True)

        # Create a cached parquet file
        df = pl.DataFrame({"col1": [1, 2, 3]})
        cache_file = test_cache / "arrests-latest.parquet"
        df.write_parquet(cache_file)

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            result = load_dataset("arrests-latest")
            mock_download.assert_not_called()
            assert isinstance(result, pl.LazyFrame)

    def test_returns_lazy_frame_by_default(self, tmp_path: Path) -> None:
        """Test that load_dataset returns LazyFrame by default."""
        test_cache = tmp_path / ".cache"
        test_cache.mkdir(parents=True)

        df = pl.DataFrame({"col1": [1, 2, 3]})
        cache_file = test_cache / "arrests-latest.parquet"
        df.write_parquet(cache_file)

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            result = load_dataset("arrests-latest")
            assert isinstance(result, pl.LazyFrame)

    def test_returns_dataframe_when_lazy_false(self, tmp_path: Path) -> None:
        """Test that load_dataset returns DataFrame when lazy=False."""
        test_cache = tmp_path / ".cache"
        test_cache.mkdir(parents=True)

        df = pl.DataFrame({"col1": [1, 2, 3]})
        cache_file = test_cache / "arrests-latest.parquet"
        df.write_parquet(cache_file)

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            result = load_dataset("arrests-latest", lazy=False)
            assert isinstance(result, pl.DataFrame)

    def test_lazy_frame_can_be_collected(self, tmp_path: Path) -> None:
        """Test that LazyFrame can be collected into DataFrame."""
        test_cache = tmp_path / ".cache"
        test_cache.mkdir(parents=True)

        df = pl.DataFrame({"col1": [1, 2, 3]})
        cache_file = test_cache / "arrests-latest.parquet"
        df.write_parquet(cache_file)

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            lazy_df = load_dataset("arrests-latest")
            collected = lazy_df.collect()  # type: ignore[attr-defined]
            assert isinstance(collected, pl.DataFrame)
            assert collected.shape == (3, 1)


class TestDownloadFile:
    """Tests for _download_file function."""

    @patch("ogden_ice_analysis.data_loader.download_file_with_lfs_fallback")
    def test_downloads_file_successfully(
        self,
        mock_download: Mock,
        tmp_path: Path,
    ) -> None:
        """Test successful file download via LFS fallback."""
        dest = tmp_path / "test.parquet"
        url = "https://example.com/test.parquet"

        _download_file(url, dest, "test-dataset")

        mock_download.assert_called_once()

    @patch("ogden_ice_analysis.data_loader.download_file_with_lfs_fallback")
    def test_raises_error_on_failure(self, mock_download: Mock) -> None:
        """Test that download errors are propagated."""
        mock_download.side_effect = RuntimeError("Download failed")

        url = "https://example.com/test.parquet"
        with pytest.raises(RuntimeError, match="Download failed"):
            _download_file(url, Path("/tmp/test.parquet"), "test-dataset")


class TestClearCache:
    """Tests for clear_cache function."""

    def test_removes_all_parquet_files(self, tmp_path: Path) -> None:
        """Test that clear_cache removes all parquet files from cache."""
        test_cache = tmp_path / ".cache"
        test_cache.mkdir(parents=True)

        # Create some test parquet files
        (test_cache / "file1.parquet").touch()
        (test_cache / "file2.parquet").touch()
        (test_cache / "other.txt").touch()  # Should not be removed

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            clear_cache()

        assert not (test_cache / "file1.parquet").exists()
        assert not (test_cache / "file2.parquet").exists()
        assert (test_cache / "other.txt").exists()  # Still exists

    def test_handles_nonexistent_cache(self) -> None:
        """Test that clear_cache handles missing cache directory gracefully."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_cache = Path(tmp_dir) / "nonexistent"

            with patch.object(data_loader, "CACHE_DIR", test_cache):
                # Should not raise an error
                clear_cache()


class TestGetCacheInfo:
    """Tests for get_cache_info function."""

    def test_returns_info_for_all_datasets(self) -> None:
        """Test that get_cache_info returns info for all available datasets."""
        info = get_cache_info()

        assert set(info.keys()) == AVAILABLE_DATASETS

        for _dataset_name, dataset_info in info.items():
            assert isinstance(dataset_info, dict)
            assert "cached" in dataset_info
            assert "size_bytes" in dataset_info
            assert "modified" in dataset_info
            assert isinstance(dataset_info["cached"], bool)

    def test_reports_cached_status_correctly(self, tmp_path: Path) -> None:
        """Test that cached status is reported correctly."""
        test_cache = tmp_path / ".cache"
        test_cache.mkdir(parents=True)

        # Create one cached file
        df = pl.DataFrame({"col1": [1]})
        cache_file = test_cache / "arrests-latest.parquet"
        df.write_parquet(cache_file)

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            info = get_cache_info()

        assert info["arrests-latest"]["cached"] is True
        size = info["arrests-latest"]["size_bytes"]
        assert isinstance(size, int)
        assert size > 0  # type: ignore[operator]
        assert info["arrests-latest"]["modified"] is not None

        # Check that other datasets are not cached
        assert info["detainers-latest"]["cached"] is False
        assert info["detainers-latest"]["size_bytes"] is None

    def test_reports_file_size_and_timestamp(self, tmp_path: Path) -> None:
        """Test that file size and modification time are reported."""
        test_cache = tmp_path / ".cache"
        test_cache.mkdir(parents=True)

        df = pl.DataFrame({"col1": list(range(100))})
        cache_file = test_cache / "detention-stays-latest.parquet"
        df.write_parquet(cache_file)

        with patch.object(data_loader, "CACHE_DIR", test_cache):
            info = get_cache_info()

        assert info["detention-stays-latest"]["cached"] is True
        size = info["detention-stays-latest"]["size_bytes"]
        assert isinstance(size, int)
        assert size > 0  # type: ignore[operator]
        assert isinstance(info["detention-stays-latest"]["modified"], float)


class TestModuleExports:
    """Tests for module-level exports."""

    def test_available_datasets_is_set(self) -> None:
        """Test that AVAILABLE_DATASETS is a non-empty set of strings."""
        assert isinstance(AVAILABLE_DATASETS, set)
        assert len(AVAILABLE_DATASETS) > 0
        assert all(isinstance(name, str) for name in AVAILABLE_DATASETS)
        # All should end with "-latest" based on current datasets
        assert all(name.endswith("-latest") for name in AVAILABLE_DATASETS)
