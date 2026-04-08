"""Git LFS file downloader.

Download files tracked by Git LFS from GitHub without requiring git-lfs.
Uses the Git LFS Batch API to download actual binary content.
"""

from __future__ import annotations

import base64
import re
from pathlib import Path

import requests

LFS_HEADERS = {
    "Accept": "application/vnd.git-lfs+json",
    "Content-Type": "application/vnd.git-lfs+json",
}

LFS_POINTER_REGEX = re.compile(
    r"^version https://git-lfs\.github\.com/spec/v1\n"
    r"oid sha256:([a-f0-9]{64})\n"
    r"size (\d+)$"
)


def _parse_lfs_pointer(content: str) -> tuple[str, int]:
    """Parse an LFS pointer file to extract OID and size."""
    match = LFS_POINTER_REGEX.match(content.strip())

    if not match:
        msg = "Invalid LFS pointer format"
        raise ValueError(msg)

    return match.group(1), int(match.group(2))


def _is_lfs_pointer(content: bytes) -> bool:
    """Check if content appears to be an LFS pointer file."""
    return content.startswith(b"version https://git-lfs.github.com/spec/v1")


def _get_lfs_pointer_from_github(owner: str, repo: str, filepath: str) -> str:
    """Fetch the LFS pointer file content from GitHub."""
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{filepath}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    return base64.b64decode(data["content"]).decode("utf-8")


def _call_lfs_batch_api(owner: str, repo: str, oid: str, size: int) -> dict:
    """Call the Git LFS Batch API to get download information."""
    batch_url = f"https://github.com/{owner}/{repo}.git/info/lfs/objects/batch"

    payload = {
        "operation": "download",
        "transfer": ["basic"],
        "objects": [{"oid": oid, "size": size}],
    }

    response = requests.post(batch_url, json=payload, headers=LFS_HEADERS, timeout=30)
    response.raise_for_status()

    return response.json()


def _download_lfs_file(
    download_url: str,
    headers: dict[str, str] | None,
    dest: Path,
    expected_size: int,
) -> None:
    """Download the actual LFS file content."""
    response = requests.get(download_url, headers=headers, timeout=300, stream=True)
    response.raise_for_status()

    dest.parent.mkdir(parents=True, exist_ok=True)

    with dest.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    # Verify file size
    actual_size = dest.stat().st_size
    if actual_size != expected_size:
        msg = (
            f"Downloaded file size mismatch: expected {expected_size}, "
            f"got {actual_size}"
        )
        raise RuntimeError(msg)


def download_lfs_file(owner: str, repo: str, filepath: str, dest: Path) -> None:
    """Download a Git LFS-tracked file from GitHub."""
    # Get pointer and parse it
    pointer = _get_lfs_pointer_from_github(owner, repo, filepath)
    oid, size = _parse_lfs_pointer(pointer)

    # Get download URL from LFS API
    batch_response = _call_lfs_batch_api(owner, repo, oid, size)

    # Extract download action
    objects = batch_response.get("objects", [])
    if not objects:
        msg = "No objects in LFS Batch API response"
        raise RuntimeError(msg)

    obj = objects[0]

    if "error" in obj:
        error = obj["error"]
        msg = f"LFS error: {error.get('code')} - {error.get('message')}"
        raise RuntimeError(msg)

    download_action = obj.get("actions", {}).get("download")
    if not download_action:
        msg = "No download action in LFS Batch API response"
        raise RuntimeError(msg)

    # Download the file
    _download_lfs_file(
        download_action["href"],
        download_action.get("header"),
        dest,
        size,
    )


def download_file_with_lfs_fallback(
    url: str,
    dest: Path,
    owner: str,
    repo: str,
    filepath: str,
) -> None:
    """Download a file, falling back to LFS if we get a pointer."""
    response = requests.get(url, timeout=300, stream=True)
    response.raise_for_status()

    dest.parent.mkdir(parents=True, exist_ok=True)

    with dest.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    # Check if we got an LFS pointer
    with dest.open("rb") as f:
        first_bytes = f.read(50)

    if _is_lfs_pointer(first_bytes):
        # Re-download via LFS
        download_lfs_file(owner, repo, filepath, dest)
