"""
Comprehensive unit tests for core functions in restsdk_public.py

These tests cover utility functions, path resolution, filesystem operations,
and data transformation functions to increase code coverage and prevent regressions.
"""

import os
import sys
import sqlite3
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

# Add parent directory to path to import restsdk_public
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import functions to test
from restsdk_public import (
    detect_fs_type,
    is_pipe_sensitive_fs,
    resolve_src_path,
    with_retry_db,
    count_files,
    get_directory_summary,
    format_size,
    findNextParent,
    hasAnotherParent,
    findTree,
    filenameToID,
    build_reverse_lookups,
    getRootDirs,
    get_dir_size,
)


class TestFilesystemDetection:
    """Test filesystem type detection and pipe sensitivity."""
    
    def test_detect_fs_type_without_psutil(self, monkeypatch):
        """Test filesystem detection when psutil is not available."""
        import restsdk_public
        monkeypatch.setattr(restsdk_public, 'psutil', None)
        
        result = detect_fs_type("/tmp")
        assert result == (None, None)
    
    @pytest.mark.skipif(not os.path.exists("/tmp"), reason="Requires /tmp directory")
    def test_detect_fs_type_with_real_path(self):
        """Test filesystem detection with a real path."""
        try:
            import psutil
            fstype, mountpoint = detect_fs_type("/tmp")
            # Should return some filesystem type and mountpoint
            assert mountpoint is not None or fstype is None
        except ImportError:
            pytest.skip("psutil not available")
    
    def test_is_pipe_sensitive_fs_true(self):
        """Test detection of pipe-sensitive filesystems."""
        assert is_pipe_sensitive_fs("ntfs") is True
        assert is_pipe_sensitive_fs("NTFS") is True
        assert is_pipe_sensitive_fs("vfat") is True
        assert is_pipe_sensitive_fs("FAT32") is True
        assert is_pipe_sensitive_fs("exfat") is True
        assert is_pipe_sensitive_fs("cifs") is True
        assert is_pipe_sensitive_fs("smb") is True
    
    def test_is_pipe_sensitive_fs_false(self):
        """Test detection of pipe-safe filesystems."""
        assert is_pipe_sensitive_fs("ext4") is False
        assert is_pipe_sensitive_fs("xfs") is False
        assert is_pipe_sensitive_fs("btrfs") is False
        assert is_pipe_sensitive_fs("apfs") is False
        assert is_pipe_sensitive_fs("zfs") is False
    
    def test_is_pipe_sensitive_fs_none(self):
        """Test handling of None filesystem type."""
        assert is_pipe_sensitive_fs(None) is False
        assert is_pipe_sensitive_fs("") is False


class TestPathResolution:
    """Test path resolution and source file location."""
    
    def test_resolve_src_path_flat_layout(self, tmp_path):
        """Test resolving source path in flat layout."""
        # Create a file in flat layout
        content_id = "abc123"
        flat_file = tmp_path / content_id
        flat_file.write_text("test")
        
        result = resolve_src_path(str(tmp_path), content_id)
        assert result == str(flat_file)
        assert os.path.exists(result)
    
    def test_resolve_src_path_sharded_layout(self, tmp_path):
        """Test resolving source path in sharded layout."""
        # Create a file in sharded layout (first char subdirectory)
        content_id = "abc123"
        shard_dir = tmp_path / "a"
        shard_dir.mkdir()
        sharded_file = shard_dir / content_id
        sharded_file.write_text("test")
        
        result = resolve_src_path(str(tmp_path), content_id)
        assert result == str(sharded_file)
        assert os.path.exists(result)
    
    def test_resolve_src_path_prefers_flat(self, tmp_path):
        """Test that flat layout is preferred when both exist."""
        # Create both flat and sharded files
        content_id = "abc123"
        flat_file = tmp_path / content_id
        flat_file.write_text("flat")
        
        shard_dir = tmp_path / "a"
        shard_dir.mkdir()
        sharded_file = shard_dir / content_id
        sharded_file.write_text("sharded")
        
        result = resolve_src_path(str(tmp_path), content_id)
        # Should prefer flat layout (checked first)
        assert result == str(flat_file)
    
    def test_resolve_src_path_missing_returns_default(self, tmp_path):
        """Test that missing files return default path."""
        content_id = "nonexistent"
        result = resolve_src_path(str(tmp_path), content_id)
        # Should return flat path even if doesn't exist
        assert result == str(tmp_path / content_id)
        assert not os.path.exists(result)
    
    def test_resolve_src_path_empty_content_id(self, tmp_path):
        """Test handling of empty content ID."""
        result = resolve_src_path(str(tmp_path), "")
        assert result == str(tmp_path)
    
    def test_resolve_src_path_none_content_id(self, tmp_path):
        """Test handling of None content ID."""
        result = resolve_src_path(str(tmp_path), None)
        assert result == str(tmp_path)


class TestDatabaseRetry:
    """Test database retry logic."""
    
    def test_with_retry_db_success_first_try(self):
        """Test successful operation on first try."""
        mock_fn = Mock(return_value="success")
        result = with_retry_db(mock_fn)
        assert result == "success"
        assert mock_fn.call_count == 1
    
    def test_with_retry_db_success_after_retries(self):
        """Test successful operation after retries."""
        attempts = 0
        def flaky_fn():
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise sqlite3.OperationalError("database is locked")
            return "success"
        
        result = with_retry_db(flaky_fn, attempts=5, delay=0.01)
        assert result == "success"
        assert attempts == 3
    
    def test_with_retry_db_exhausts_retries(self):
        """Test that retries are exhausted for persistent locks."""
        mock_fn = Mock(side_effect=sqlite3.OperationalError("database is locked"))
        
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            with_retry_db(mock_fn, attempts=3, delay=0.01)
        
        assert mock_fn.call_count == 3
    
    def test_with_retry_db_non_lock_error_raises_immediately(self):
        """Test that non-lock errors raise immediately without retry."""
        mock_fn = Mock(side_effect=sqlite3.OperationalError("no such table"))
        
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            with_retry_db(mock_fn, attempts=5, delay=0.01)
        
        # Should fail immediately, not retry
        assert mock_fn.call_count == 1
    
    def test_with_retry_db_database_busy_retries(self):
        """Test that 'database is busy' errors trigger retries."""
        attempts = 0
        def busy_fn():
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise sqlite3.OperationalError("database is busy")
            return "success"
        
        result = with_retry_db(busy_fn, attempts=5, delay=0.01)
        assert result == "success"
        assert attempts == 2


class TestDirectoryOperations:
    """Test directory scanning and size calculation."""
    
    def test_count_files_empty_directory(self, tmp_path):
        """Test counting files in empty directory."""
        assert count_files(str(tmp_path)) == 0
    
    def test_count_files_flat_directory(self, tmp_path):
        """Test counting files in flat directory."""
        # Create some files
        for i in range(5):
            (tmp_path / f"file{i}.txt").write_text("content")
        
        assert count_files(str(tmp_path)) == 5
    
    def test_count_files_nested_directories(self, tmp_path):
        """Test counting files in nested directories."""
        # Create nested structure
        (tmp_path / "file1.txt").write_text("content")
        
        subdir1 = tmp_path / "subdir1"
        subdir1.mkdir()
        (subdir1 / "file2.txt").write_text("content")
        (subdir1 / "file3.txt").write_text("content")
        
        subdir2 = subdir1 / "subdir2"
        subdir2.mkdir()
        (subdir2 / "file4.txt").write_text("content")
        
        assert count_files(str(tmp_path)) == 4
    
    def test_get_directory_summary_empty(self, tmp_path):
        """Test directory summary for empty directory."""
        file_count, total_size = get_directory_summary(str(tmp_path))
        assert file_count == 0
        assert total_size == 0
    
    def test_get_directory_summary_with_files(self, tmp_path):
        """Test directory summary with files of known sizes."""
        # Create files with known sizes
        (tmp_path / "file1.txt").write_text("a" * 100)  # 100 bytes
        (tmp_path / "file2.txt").write_text("b" * 200)  # 200 bytes
        
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "file3.txt").write_text("c" * 300)  # 300 bytes
        
        file_count, total_size = get_directory_summary(str(tmp_path))
        assert file_count == 3
        assert total_size == 600
    
    def test_get_directory_summary_skips_symlinks(self, tmp_path):
        """Test that symlinks are skipped in directory summary."""
        # Create a real file
        real_file = tmp_path / "real.txt"
        real_file.write_text("content")
        
        # Create a symlink
        link = tmp_path / "link.txt"
        try:
            link.symlink_to(real_file)
            
            file_count, total_size = get_directory_summary(str(tmp_path))
            # Should only count the real file, not the symlink
            assert file_count == 1
            assert total_size == 7  # "content" is 7 bytes
        except OSError:
            # Symlinks may not be supported on all platforms
            pytest.skip("Symlinks not supported")
    
    def test_get_directory_summary_handles_missing_files(self, tmp_path):
        """Test that missing files during scan are handled gracefully."""
        # This is harder to test without race conditions, but we can at least
        # verify the function completes without error on a valid directory
        file_count, total_size = get_directory_summary(str(tmp_path))
        assert file_count >= 0
        assert total_size >= 0
    
    def test_get_dir_size_empty(self, tmp_path):
        """Test get_dir_size on empty directory."""
        size = get_dir_size(str(tmp_path))
        assert size == 0
    
    def test_get_dir_size_with_files(self, tmp_path):
        """Test get_dir_size with files."""
        (tmp_path / "file1.txt").write_text("a" * 100)
        (tmp_path / "file2.txt").write_text("b" * 200)
        
        size = get_dir_size(str(tmp_path))
        assert size == 300


class TestFormatSize:
    """Test human-readable size formatting."""
    
    def test_format_size_bytes(self):
        """Test formatting bytes."""
        assert format_size(0) == "0.00 B"
        assert format_size(100) == "100.00 B"
        assert format_size(1023) == "1023.00 B"
    
    def test_format_size_kilobytes(self):
        """Test formatting kilobytes."""
        assert format_size(1024) == "1.00 KB"
        assert format_size(1536) == "1.50 KB"
        assert format_size(10240) == "10.00 KB"
    
    def test_format_size_megabytes(self):
        """Test formatting megabytes."""
        assert format_size(1024 * 1024) == "1.00 MB"
        assert format_size(1024 * 1024 * 2.5) == "2.50 MB"
    
    def test_format_size_gigabytes(self):
        """Test formatting gigabytes."""
        assert format_size(1024 * 1024 * 1024) == "1.00 GB"
        assert format_size(1024 * 1024 * 1024 * 1.5) == "1.50 GB"
    
    def test_format_size_terabytes(self):
        """Test formatting terabytes."""
        assert format_size(1024 * 1024 * 1024 * 1024) == "1.00 TB"
        assert format_size(1024 * 1024 * 1024 * 1024 * 2.5) == "2.50 TB"
    
    def test_format_size_petabytes(self):
        """Test formatting petabytes."""
        assert format_size(1024 ** 5) == "1.00 PB"
        assert format_size(1024 ** 5 * 3.75) == "3.75 PB"


class TestFileDictionaryOperations:
    """Test file dictionary traversal and lookup operations.
    
    Note: These functions rely on module-level 'fileDIC' which is only
    initialized in the main block. We test them by temporarily creating
    the expected module attribute.
    """
    
    @pytest.fixture
    def setup_file_dic(self, monkeypatch):
        """Set up a test file dictionary."""
        import restsdk_public
        
        test_fileDIC = {
            "1": {"Name": "root", "Parent": None, "contentID": "content1"},
            "2": {"Name": "folder1", "Parent": "1", "contentID": None},
            "3": {"Name": "folder2", "Parent": "2", "contentID": None},
            "4": {"Name": "file.txt", "Parent": "3", "contentID": "content4"},
            "5": {"Name": "orphan.txt", "Parent": None, "contentID": "content5"},
        }
        
        # Create the attribute if it doesn't exist, then set it
        if not hasattr(restsdk_public, 'fileDIC'):
            restsdk_public.fileDIC = {}
        monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
        return test_fileDIC
    
    def test_findNextParent_with_parent(self, setup_file_dic):
        """Test finding next parent when it exists."""
        assert findNextParent("2") == "1"
        assert findNextParent("3") == "2"
        assert findNextParent("4") == "3"
    
    def test_findNextParent_no_parent(self, setup_file_dic):
        """Test finding next parent when none exists."""
        assert findNextParent("1") is None
        assert findNextParent("5") is None
    
    def test_findNextParent_nonexistent_id(self, setup_file_dic):
        """Test finding next parent for nonexistent ID."""
        assert findNextParent("999") is None
    
    def test_hasAnotherParent_true(self, setup_file_dic):
        """Test checking for parent when it exists."""
        assert hasAnotherParent("2") is True
        assert hasAnotherParent("3") is True
        assert hasAnotherParent("4") is True
    
    def test_hasAnotherParent_false(self, setup_file_dic):
        """Test checking for parent when none exists."""
        assert hasAnotherParent("1") is False
        assert hasAnotherParent("5") is False
    
    def test_findTree_simple_path(self, setup_file_dic):
        """Test reconstructing a simple file path."""
        # file.txt is in folder2/folder1/root
        path = findTree("4", "file.txt", "3")
        assert path == "root/folder1/folder2/file.txt"
    
    def test_findTree_nested_path(self, setup_file_dic):
        """Test reconstructing a nested path."""
        path = findTree("3", "folder2", "2")
        assert path == "root/folder1/folder2"
    
    def test_findTree_single_level(self, setup_file_dic):
        """Test reconstructing path with single parent."""
        path = findTree("2", "folder1", "1")
        # Should include root since parent ID "1" exists
        assert path == "root/folder1"
    
    def test_build_reverse_lookups(self, setup_file_dic, monkeypatch):
        """Test building reverse lookup dictionaries."""
        import restsdk_public
        
        # Clear existing lookups
        monkeypatch.setattr(restsdk_public, '_contentID_to_fileID', {})
        monkeypatch.setattr(restsdk_public, '_name_to_fileID', {})
        
        build_reverse_lookups()
        
        # Check that lookups were built
        from restsdk_public import _contentID_to_fileID, _name_to_fileID
        
        assert "content1" in _contentID_to_fileID
        assert _contentID_to_fileID["content1"] == "1"
        assert "content4" in _contentID_to_fileID
        assert _contentID_to_fileID["content4"] == "4"
        
        assert "root" in _name_to_fileID
        assert _name_to_fileID["root"] == "1"
        assert "file.txt" in _name_to_fileID
        assert _name_to_fileID["file.txt"] == "4"
    
    def test_filenameToID_by_content_id(self, setup_file_dic, monkeypatch):
        """Test looking up file ID by content ID."""
        import restsdk_public
        
        # Build lookups first
        monkeypatch.setattr(restsdk_public, '_contentID_to_fileID', {
            "content1": "1",
            "content4": "4",
            "content5": "5"
        })
        monkeypatch.setattr(restsdk_public, '_name_to_fileID', {
            "root": "1",
            "file.txt": "4"
        })
        
        assert filenameToID("content1") == "1"
        assert filenameToID("content4") == "4"
    
    def test_filenameToID_by_name_fallback(self, setup_file_dic, monkeypatch):
        """Test looking up file ID by name when content ID not found."""
        import restsdk_public
        
        monkeypatch.setattr(restsdk_public, '_contentID_to_fileID', {})
        monkeypatch.setattr(restsdk_public, '_name_to_fileID', {
            "root": "1",
            "file.txt": "4"
        })
        
        assert filenameToID("file.txt") == "4"
        assert filenameToID("root") == "1"
    
    def test_filenameToID_not_found(self, setup_file_dic, monkeypatch):
        """Test looking up nonexistent filename."""
        import restsdk_public
        
        monkeypatch.setattr(restsdk_public, '_contentID_to_fileID', {})
        monkeypatch.setattr(restsdk_public, '_name_to_fileID', {})
        
        assert filenameToID("nonexistent") is None
    
    def test_getRootDirs_finds_auth_pipe(self, monkeypatch):
        """Test finding root directory with 'auth' and '|' in name."""
        import restsdk_public
        
        test_fileDIC = {
            "1": {"Name": "normal_folder", "Parent": None},
            "2": {"Name": "auth0|someid", "Parent": None},
            "3": {"Name": "another_folder", "Parent": "2"},
        }
        
        # Create the attribute if it doesn't exist
        if not hasattr(restsdk_public, 'fileDIC'):
            restsdk_public.fileDIC = {}
        monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
        
        result = getRootDirs()
        assert result == "auth0|someid"
    
    def test_getRootDirs_no_auth_pipe(self, monkeypatch):
        """Test when no auth pipe directory exists."""
        import restsdk_public
        
        test_fileDIC = {
            "1": {"Name": "folder1", "Parent": None},
            "2": {"Name": "folder2", "Parent": "1"},
        }
        
        # Create the attribute if it doesn't exist
        if not hasattr(restsdk_public, 'fileDIC'):
            restsdk_public.fileDIC = {}
        monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
        
        result = getRootDirs()
        assert result is None


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_count_files_nonexistent_directory(self):
        """Test counting files in nonexistent directory."""
        # Should raise or return 0, depending on implementation
        # Let's test that it doesn't crash
        try:
            result = count_files("/nonexistent/path/that/should/not/exist")
            # If it doesn't raise, should return 0
            assert result >= 0
        except (FileNotFoundError, OSError):
            # This is also acceptable
            pass
    
    def test_get_directory_summary_nonexistent(self):
        """Test directory summary on nonexistent path."""
        try:
            file_count, total_size = get_directory_summary("/nonexistent/path")
            # If it doesn't raise, should return zeros
            assert file_count == 0
            assert total_size == 0
        except (FileNotFoundError, OSError):
            # This is also acceptable
            pass
    
    def test_format_size_negative(self):
        """Test formatting negative sizes."""
        # Should still format (negative file sizes shouldn't happen, but be defensive)
        result = format_size(-100)
        assert "B" in result
    
    def test_resolve_src_path_single_char_content_id(self, tmp_path):
        """Test resolving path with single character content ID."""
        content_id = "a"
        flat_file = tmp_path / content_id
        flat_file.write_text("test")
        
        result = resolve_src_path(str(tmp_path), content_id)
        assert result == str(flat_file)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
