"""Data loader for ICE deportation datasets from GitHub.

This module provides utilities to lazily load parquet datasets from the
deportationdata/ice repository with automatic local caching.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from ogden_ice_analysis.lfs_downloader import download_file_with_lfs_fallback

if TYPE_CHECKING:
    pass

# Repository configuration
CACHE_DIR = Path(".cache/ogden-ice-analysis")

# Dataset to repository mapping (owner, repo)
DATASET_REPOS = {
    "arrests-latest": ("deportationdata", "ice"),
    "detainers-latest": ("deportationdata", "ice"),
    "detention-stays-latest": ("deportationdata", "ice"),
    "detention-stints-latest": ("deportationdata", "ice"),
    "facilities-daily-population-latest": ("deportationdata", "ice"),
    "facilities-latest": ("deportationdata", "ice-detention-facilities"),
}

AVAILABLE_DATASETS = set(DATASET_REPOS)


def _ensure_cache_dir() -> Path:
    """Ensure the cache directory exists."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR


def _get_cache_path(dataset_name: str) -> Path:
    """Get the local cache path for a dataset."""
    cache_dir = _ensure_cache_dir()
    return cache_dir / f"{dataset_name}.parquet"


def _get_repo_base_url(owner: str, repo: str) -> str:
    """Get the base raw URL for a repository."""
    return f"https://raw.githubusercontent.com/{owner}/{repo}/refs/heads/main/data"


def _get_remote_url(dataset_name: str) -> str:
    """Get the remote URL for a dataset."""
    owner, repo = DATASET_REPOS[dataset_name]
    base = _get_repo_base_url(owner, repo)
    return f"{base}/{dataset_name}.parquet"


def list_datasets() -> list[str]:
    """List all available dataset names."""
    return sorted(AVAILABLE_DATASETS)


def load_dataset(
    dataset_name: str,
    *,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load a parquet dataset from the ICE deportation data repository.

    Downloads the dataset from GitHub if not cached locally. Uses lazy loading
    by default for efficient memory usage and query optimization.

    Examples:
        >>> df_lazy = load_dataset("arrests-latest")
        >>> df_lazy.filter(pl.col("date") > "2024-01-01").collect()

        >>> df = load_dataset("arrests-latest", lazy=False)

    """
    if dataset_name not in AVAILABLE_DATASETS:
        available = ", ".join(sorted(AVAILABLE_DATASETS))
        msg = f"Unknown dataset '{dataset_name}'. Available: {available}"
        raise ValueError(msg)

    cache_path = _get_cache_path(dataset_name)

    # Download if not cached
    if not cache_path.exists():
        remote_url = _get_remote_url(dataset_name)
        _download_file(remote_url, cache_path, dataset_name)

    if lazy:
        return pl.scan_parquet(cache_path)

    return pl.read_parquet(cache_path)


def _download_file(url: str, dest: Path, dataset_name: str) -> None:
    """Download a file from URL to destination path."""
    owner, repo = DATASET_REPOS[dataset_name]
    filepath = f"data/{dataset_name}.parquet"
    download_file_with_lfs_fallback(url, dest, owner, repo, filepath)


def clear_cache() -> None:
    """Clear all cached dataset files."""
    if not CACHE_DIR.exists():
        return

    for file_path in CACHE_DIR.glob("*.parquet"):
        file_path.unlink()


def get_cache_info() -> dict[str, dict[str, object]]:
    """Get information about cached datasets."""
    info = {}

    for dataset_name in AVAILABLE_DATASETS:
        cache_path = _get_cache_path(dataset_name)

        if cache_path.exists():
            stat = cache_path.stat()
            info[dataset_name] = {
                "cached": True,
                "size_bytes": stat.st_size,
                "modified": stat.st_mtime,
            }
        else:
            info[dataset_name] = {"cached": False, "size_bytes": None, "modified": None}

    return info
