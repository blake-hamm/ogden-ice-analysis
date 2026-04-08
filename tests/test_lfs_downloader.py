"""Tests for the lfs_downloader module."""

import base64
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from ogden_ice_analysis.lfs_downloader import (
    _call_lfs_batch_api,
    _download_lfs_file,
    _get_lfs_pointer_from_github,
    _is_lfs_pointer,
    _parse_lfs_pointer,
    download_file_with_lfs_fallback,
    download_lfs_file,
)


class TestParseLFSPointer:
    """Tests for _parse_lfs_pointer function."""

    def test_parses_valid_pointer(self) -> None:
        """Test parsing a valid LFS pointer file."""
        pointer = """version https://git-lfs.github.com/spec/v1
oid sha256:2def878d6a8bc86131a8acba4ba5bdbf3e00c8c4d7e228b82b592a66082e0347
size 27895224"""

        oid, size = _parse_lfs_pointer(pointer)

        assert oid == "2def878d6a8bc86131a8acba4ba5bdbf3e00c8c4d7e228b82b592a66082e0347"
        assert size == 27895224

    def test_raises_on_invalid_format(self) -> None:
        """Test that invalid pointer format raises error."""
        with pytest.raises(ValueError, match="Invalid LFS pointer"):
            _parse_lfs_pointer("not a valid pointer")


class TestIsLFSPointer:
    """Tests for _is_lfs_pointer function."""

    def test_returns_true_for_lfs_pointer(self) -> None:
        """Test that LFS pointer content is detected."""
        content = b"version https://git-lfs.github.com/spec/v1\noid sha256:..."
        assert _is_lfs_pointer(content) is True

    def test_returns_false_for_binary_content(self) -> None:
        """Test that binary content is not detected as LFS pointer."""
        assert _is_lfs_pointer(b"PAR1\x00\x01\x02") is False


class TestGetLFSPointerFromGitHub:
    """Tests for _get_lfs_pointer_from_github function."""

    @patch("requests.get")
    def test_fetches_and_decodes_pointer(self, mock_get: Mock) -> None:
        """Test fetching and decoding an LFS pointer file."""
        pointer_content = """version https://git-lfs.github.com/spec/v1
oid sha256:abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1
size 1000"""

        mock_response = Mock()
        mock_response.json.return_value = {
            "content": base64.b64encode(pointer_content.encode()).decode()
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = _get_lfs_pointer_from_github("owner", "repo", "path/file.parquet")

        assert "version https://git-lfs.github.com/spec/v1" in result
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_raises_on_http_error(self, mock_get: Mock) -> None:
        """Test that HTTP errors are raised."""
        mock_get.side_effect = requests.exceptions.HTTPError("404")

        with pytest.raises(requests.exceptions.HTTPError):
            _get_lfs_pointer_from_github("owner", "repo", "file.parquet")


class TestCallLFSBatchAPI:
    """Tests for _call_lfs_batch_api function."""

    @patch("requests.post")
    def test_returns_batch_response(self, mock_post: Mock) -> None:
        """Test successful batch API call."""
        expected = {
            "objects": [
                {
                    "oid": "abc123",
                    "size": 1000,
                    "actions": {"download": {"href": "https://example.com/download"}},
                }
            ]
        }

        mock_response = Mock()
        mock_response.json.return_value = expected
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = _call_lfs_batch_api("owner", "repo", "abc123", 1000)

        assert result == expected

    @patch("requests.post")
    def test_raises_on_http_error(self, mock_post: Mock) -> None:
        """Test that HTTP errors are raised."""
        mock_post.side_effect = requests.exceptions.HTTPError("500")

        with pytest.raises(requests.exceptions.HTTPError):
            _call_lfs_batch_api("owner", "repo", "abc123", 1000)


class TestDownloadLFSFile:
    """Tests for _download_lfs_file function."""

    @patch("requests.get")
    def test_downloads_file_successfully(self, mock_get: Mock, tmp_path: Path) -> None:
        """Test successful file download."""
        dest = tmp_path / "test.parquet"
        content = b"fake parquet content"

        mock_response = Mock()
        mock_response.iter_content.return_value = [content]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        _download_lfs_file("https://example.com/file", None, dest, len(content))

        assert dest.read_bytes() == content

    @patch("requests.get")
    def test_raises_on_size_mismatch(self, mock_get: Mock, tmp_path: Path) -> None:
        """Test that size mismatch raises RuntimeError."""
        dest = tmp_path / "test.parquet"

        mock_response = Mock()
        mock_response.iter_content.return_value = [b"short"]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with pytest.raises(RuntimeError, match="size mismatch"):
            _download_lfs_file("https://example.com/file", None, dest, 1000)


class TestDownloadLFSFileIntegration:
    """Tests for download_lfs_file function."""

    @patch("ogden_ice_analysis.lfs_downloader._download_lfs_file")
    @patch("ogden_ice_analysis.lfs_downloader._call_lfs_batch_api")
    @patch("ogden_ice_analysis.lfs_downloader._get_lfs_pointer_from_github")
    def test_full_download_flow(
        self,
        mock_get_pointer: Mock,
        mock_batch_api: Mock,
        mock_download: Mock,
        tmp_path: Path,
    ) -> None:
        """Test the complete download flow."""
        dest = tmp_path / "file.parquet"

        mock_get_pointer.return_value = """version https://git-lfs.github.com/spec/v1
oid sha256:abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1
size 1000"""

        mock_batch_api.return_value = {
            "objects": [
                {
                    "oid": "abc123...",
                    "size": 1000,
                    "actions": {
                        "download": {
                            "href": "https://example.com/download",
                            "header": {"Auth": "token"},
                        }
                    },
                }
            ]
        }

        download_lfs_file("owner", "repo", "data/file.parquet", dest)

        mock_get_pointer.assert_called_once_with("owner", "repo", "data/file.parquet")
        mock_download.assert_called_once()

    @patch("ogden_ice_analysis.lfs_downloader._get_lfs_pointer_from_github")
    def test_raises_on_invalid_pointer(
        self, mock_get_pointer: Mock, tmp_path: Path
    ) -> None:
        """Test error handling for invalid pointer."""
        mock_get_pointer.return_value = "invalid pointer"

        with pytest.raises(ValueError, match="Invalid LFS pointer"):
            download_lfs_file(
                "owner", "repo", "data/file.parquet", tmp_path / "file.parquet"
            )

    @patch("ogden_ice_analysis.lfs_downloader._call_lfs_batch_api")
    @patch("ogden_ice_analysis.lfs_downloader._get_lfs_pointer_from_github")
    def test_raises_on_empty_objects(
        self, mock_get_pointer: Mock, mock_batch_api: Mock, tmp_path: Path
    ) -> None:
        """Test error when batch API returns no objects."""
        mock_get_pointer.return_value = """version https://git-lfs.github.com/spec/v1
oid sha256:abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1
size 1000"""

        mock_batch_api.return_value = {"objects": []}

        with pytest.raises(RuntimeError, match="No objects"):
            download_lfs_file(
                "owner", "repo", "data/file.parquet", tmp_path / "file.parquet"
            )

    @patch("ogden_ice_analysis.lfs_downloader._call_lfs_batch_api")
    @patch("ogden_ice_analysis.lfs_downloader._get_lfs_pointer_from_github")
    def test_raises_on_lfs_error(
        self, mock_get_pointer: Mock, mock_batch_api: Mock, tmp_path: Path
    ) -> None:
        """Test error when LFS returns an error object."""
        mock_get_pointer.return_value = """version https://git-lfs.github.com/spec/v1
oid sha256:abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1
size 1000"""

        mock_batch_api.return_value = {
            "objects": [
                {
                    "oid": "abc123...",
                    "size": 1000,
                    "error": {"code": 404, "message": "Object not found"},
                }
            ]
        }

        with pytest.raises(RuntimeError, match="LFS error"):
            download_lfs_file(
                "owner", "repo", "data/file.parquet", tmp_path / "file.parquet"
            )


class TestDownloadFileWithLFSFallback:
    """Tests for download_file_with_lfs_fallback function."""

    @patch("requests.get")
    def test_downloads_regular_file(self, mock_get: Mock, tmp_path: Path) -> None:
        """Test regular file download."""
        dest = tmp_path / "file.txt"
        content = b"regular file content"

        mock_response = Mock()
        mock_response.iter_content.return_value = [content]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        download_file_with_lfs_fallback(
            "https://example.com/file.txt",
            dest,
            "owner",
            "repo",
            "path/file.txt",
        )

        assert dest.read_bytes() == content
        assert mock_get.call_count == 1  # Only one call for regular file

    @patch("ogden_ice_analysis.lfs_downloader.download_lfs_file")
    @patch("requests.get")
    def test_falls_back_to_lfs_for_pointer(
        self,
        mock_get: Mock,
        mock_download_lfs: Mock,
        tmp_path: Path,
    ) -> None:
        """Test LFS fallback when downloading a pointer."""
        dest = tmp_path / "file.parquet"
        pointer = b"version https://git-lfs.github.com/spec/v1\noid sha256:..."

        mock_response = Mock()
        mock_response.iter_content.return_value = [pointer]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        download_file_with_lfs_fallback(
            "https://example.com/file.parquet",
            dest,
            "deportationdata",
            "ice",
            "data/file.parquet",
        )

        # Should call regular download first, then LFS download
        assert mock_get.call_count == 1
        mock_download_lfs.assert_called_once_with(
            "deportationdata", "ice", "data/file.parquet", dest
        )

    @patch("requests.get")
    def test_creates_parent_directories(self, mock_get: Mock, tmp_path: Path) -> None:
        """Test that parent directories are created."""
        dest = tmp_path / "nested" / "dirs" / "file.txt"

        mock_response = Mock()
        mock_response.iter_content.return_value = [b"content"]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        download_file_with_lfs_fallback(
            "https://example.com/file.txt",
            dest,
            "owner",
            "repo",
            "file.txt",
        )

        assert dest.exists()
