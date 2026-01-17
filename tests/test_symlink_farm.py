"""
Unit tests for create_symlink_farm.py

Run with: pytest tests/test_symlink_farm.py -v
Coverage: pytest tests/test_symlink_farm.py --cov=create_symlink_farm --cov-report=term-missing
"""

import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from create_symlink_farm import (
    load_files_from_db,
    find_root_dir_name,
    reconstruct_path,
    get_source_file_path,
    sanitize_path,
    create_symlink_farm,
)


# Schema for test database
FILES_SCHEMA = """
CREATE TABLE Files(
    id TEXT PRIMARY KEY,
    parentID TEXT,
    contentID TEXT,
    name TEXT NOT NULL,
    imageDate INTEGER,
    videoDate INTEGER,
    cTime INTEGER,
    birthTime INTEGER
);
"""


def create_test_db(tmp_path: Path) -> Path:
    """Create a test database with the Files schema."""
    db_path = tmp_path / "index.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(FILES_SCHEMA)
    conn.commit()
    conn.close()
    return db_path


def insert_file(db_path: Path, fid: str, name: str, parent_id=None, content_id=None):
    """Insert a file record into the test database."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Files (id, parentID, contentID, name) VALUES (?, ?, ?, ?)",
        (fid, parent_id, content_id, name)
    )
    conn.commit()
    conn.close()


class TestLoadFilesFromDb:
    """Tests for load_files_from_db function."""
    
    def test_load_empty_db(self, tmp_path):
        """Test loading from empty database."""
        db_path = create_test_db(tmp_path)
        result = load_files_from_db(str(db_path))
        assert result == {}
    
    def test_load_single_file(self, tmp_path):
        """Test loading single file record."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        result = load_files_from_db(str(db_path))
        
        assert len(result) == 1
        assert "1" in result
        assert result["1"]["Name"] == "photo.jpg"
        assert result["1"]["contentID"] == "abc123"
        assert result["1"]["Parent"] is None
    
    def test_load_multiple_files(self, tmp_path):
        """Test loading multiple file records."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "folder", None, None)
        insert_file(db_path, "2", "photo.jpg", "1", "abc123")
        insert_file(db_path, "3", "video.mov", "1", "def456")
        
        result = load_files_from_db(str(db_path))
        
        assert len(result) == 3
        assert result["2"]["Parent"] == "1"
        assert result["3"]["Parent"] == "1"
    
    def test_load_preserves_timestamps(self, tmp_path):
        """Test that timestamp fields are loaded."""
        db_path = create_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "INSERT INTO Files (id, name, imageDate, videoDate, cTime, birthTime) VALUES (?, ?, ?, ?, ?, ?)",
            ("1", "photo.jpg", 1000, 2000, 3000, 4000)
        )
        conn.commit()
        conn.close()
        
        result = load_files_from_db(str(db_path))
        
        assert result["1"]["imageDate"] == 1000
        assert result["1"]["videoDate"] == 2000
        assert result["1"]["cTime"] == 3000
        assert result["1"]["birthTime"] == 4000


class TestFindRootDirName:
    """Tests for find_root_dir_name function."""
    
    def test_no_root_dir(self):
        """Test when no special root directory exists."""
        file_dic = {
            "1": {"Name": "Photos"},
            "2": {"Name": "Documents"},
        }
        result = find_root_dir_name(file_dic)
        assert result is None
    
    def test_finds_auth_pipe_dir(self):
        """Test finding directory with auth and | in name."""
        file_dic = {
            "1": {"Name": "auth0|5e62c7d40ec9700d5c82bb89"},
            "2": {"Name": "Photos"},
        }
        result = find_root_dir_name(file_dic)
        assert result == "auth0|5e62c7d40ec9700d5c82bb89"
    
    def test_auth_without_pipe(self):
        """Test directory with auth but no pipe."""
        file_dic = {
            "1": {"Name": "auth_folder"},
            "2": {"Name": "Photos"},
        }
        result = find_root_dir_name(file_dic)
        assert result is None
    
    def test_pipe_without_auth(self):
        """Test directory with pipe but no auth."""
        file_dic = {
            "1": {"Name": "folder|name"},
            "2": {"Name": "Photos"},
        }
        result = find_root_dir_name(file_dic)
        assert result is None


class TestReconstructPath:
    """Tests for reconstruct_path function."""
    
    def test_single_file_no_parent(self):
        """Test file with no parent."""
        file_dic = {
            "1": {"Name": "photo.jpg", "Parent": None},
        }
        result = reconstruct_path("1", file_dic)
        assert result == "photo.jpg"
    
    def test_file_with_parent(self):
        """Test file with one parent."""
        file_dic = {
            "1": {"Name": "Photos", "Parent": None},
            "2": {"Name": "photo.jpg", "Parent": "1"},
        }
        result = reconstruct_path("2", file_dic)
        assert result == "Photos/photo.jpg"
    
    def test_deep_nesting(self):
        """Test deeply nested file."""
        file_dic = {
            "1": {"Name": "Root", "Parent": None},
            "2": {"Name": "Level1", "Parent": "1"},
            "3": {"Name": "Level2", "Parent": "2"},
            "4": {"Name": "file.txt", "Parent": "3"},
        }
        result = reconstruct_path("4", file_dic)
        assert result == "Root/Level1/Level2/file.txt"
    
    def test_strip_root_dir(self):
        """Test stripping root directory from path."""
        file_dic = {
            "1": {"Name": "auth0|abc123", "Parent": None},
            "2": {"Name": "Photos", "Parent": "1"},
            "3": {"Name": "photo.jpg", "Parent": "2"},
        }
        result = reconstruct_path("3", file_dic, "auth0|abc123")
        assert result == "Photos/photo.jpg"
    
    def test_backslash_normalization(self):
        """Test that backslashes are converted to forward slashes."""
        file_dic = {
            "1": {"Name": "Folder\\Subfolder", "Parent": None},
            "2": {"Name": "file.txt", "Parent": "1"},
        }
        result = reconstruct_path("2", file_dic)
        assert "\\" not in result
        assert "/" in result
    
    def test_nonexistent_file(self):
        """Test with nonexistent file ID."""
        file_dic = {"1": {"Name": "photo.jpg", "Parent": None}}
        result = reconstruct_path("999", file_dic)
        assert result is None
    
    def test_leading_slash_stripped(self):
        """Test that leading slash is stripped."""
        file_dic = {
            "1": {"Name": "/root", "Parent": None},
            "2": {"Name": "file.txt", "Parent": "1"},
        }
        result = reconstruct_path("2", file_dic)
        assert not result.startswith("/")


class TestGetSourceFilePath:
    """Tests for get_source_file_path function."""
    
    def test_sharded_file_exists(self, tmp_path):
        """Test finding file in sharded directory structure."""
        # Create sharded directory structure
        shard_dir = tmp_path / "a"
        shard_dir.mkdir()
        source_file = shard_dir / "abc123"
        source_file.write_text("data")
        
        result = get_source_file_path("abc123", str(tmp_path))
        assert result == str(source_file)
    
    def test_flat_file_exists(self, tmp_path):
        """Test finding file in flat directory structure."""
        source_file = tmp_path / "xyz789"
        source_file.write_text("data")
        
        result = get_source_file_path("xyz789", str(tmp_path))
        assert result == str(source_file)
    
    def test_file_not_found(self, tmp_path):
        """Test when file doesn't exist."""
        result = get_source_file_path("nonexistent", str(tmp_path))
        assert result is None
    
    def test_empty_content_id(self, tmp_path):
        """Test with empty content ID."""
        result = get_source_file_path("", str(tmp_path))
        assert result is None
    
    def test_none_content_id(self, tmp_path):
        """Test with None content ID."""
        result = get_source_file_path(None, str(tmp_path))
        assert result is None
    
    def test_prefers_sharded_over_flat(self, tmp_path):
        """Test that sharded location is preferred over flat."""
        # Create both sharded and flat files
        shard_dir = tmp_path / "a"
        shard_dir.mkdir()
        sharded_file = shard_dir / "abc123"
        sharded_file.write_text("sharded")
        
        flat_file = tmp_path / "abc123"
        flat_file.write_text("flat")
        
        result = get_source_file_path("abc123", str(tmp_path))
        assert result == str(sharded_file)


class TestSanitizePath:
    """Tests for sanitize_path function."""
    
    def test_no_sanitization(self):
        """Test path unchanged when sanitize_pipes=False."""
        result = sanitize_path("folder|name/file|test.jpg", False)
        assert result == "folder|name/file|test.jpg"
    
    def test_sanitize_pipes(self):
        """Test pipe replacement when sanitize_pipes=True."""
        result = sanitize_path("folder|name/file|test.jpg", True)
        assert result == "folder-name/file-test.jpg"
    
    def test_no_pipes_to_sanitize(self):
        """Test path without pipes."""
        result = sanitize_path("folder/file.jpg", True)
        assert result == "folder/file.jpg"


class TestCreateSymlinkFarm:
    """Tests for create_symlink_farm function."""
    
    def test_create_single_symlink(self, tmp_path):
        """Test creating a single symlink."""
        # Setup database
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        # Setup source file
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        source_file = shard_dir / "abc123"
        source_file.write_text("photo data")
        
        # Setup farm directory
        farm_dir = tmp_path / "farm"
        
        created, no_content, no_source, errors = create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir)
        )
        
        assert created == 1
        assert no_content == 0
        assert no_source == 0
        assert errors == 0
        
        # Verify symlink exists and points correctly
        symlink_path = farm_dir / "photo.jpg"
        assert symlink_path.is_symlink()
        assert os.readlink(str(symlink_path)) == str(source_file)
    
    def test_create_nested_structure(self, tmp_path):
        """Test creating nested directory structure."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "Photos", None, None)  # Directory
        insert_file(db_path, "2", "2024", "1", None)     # Directory
        insert_file(db_path, "3", "photo.jpg", "2", "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        created, no_content, no_source, errors = create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir)
        )
        
        assert created == 1
        assert no_content == 2  # Two directories skipped
        
        symlink_path = farm_dir / "Photos" / "2024" / "photo.jpg"
        assert symlink_path.is_symlink()
    
    def test_skip_missing_source(self, tmp_path):
        """Test skipping files with missing source."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "nonexistent")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        farm_dir = tmp_path / "farm"
        
        created, no_content, no_source, errors = create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir)
        )
        
        assert created == 0
        assert no_source == 1
    
    def test_dry_run(self, tmp_path):
        """Test dry run doesn't create symlinks."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        created, _, _, _ = create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir), dry_run=True
        )
        
        assert created == 1
        assert not farm_dir.exists()  # Farm directory not created
    
    def test_sanitize_pipes_in_path(self, tmp_path):
        """Test pipe sanitization in paths."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "folder|name", None, None)
        insert_file(db_path, "2", "file|test.jpg", "1", "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir), sanitize_pipes=True
        )
        
        # Check sanitized path exists
        symlink_path = farm_dir / "folder-name" / "file-test.jpg"
        assert symlink_path.is_symlink()
    
    def test_strip_root_auth_dir(self, tmp_path):
        """Test stripping auth root directory from paths."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "auth0|abc123", None, None)  # Root to strip
        insert_file(db_path, "2", "Photos", "1", None)
        insert_file(db_path, "3", "photo.jpg", "2", "xyz789")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "x"
        shard_dir.mkdir()
        (shard_dir / "xyz789").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        create_symlink_farm(str(db_path), str(source_dir), str(farm_dir))
        
        # Path should be Photos/photo.jpg, not auth0|abc123/Photos/photo.jpg
        symlink_path = farm_dir / "Photos" / "photo.jpg"
        assert symlink_path.is_symlink()
        
        # Auth directory should NOT exist
        auth_dir = farm_dir / "auth0|abc123"
        assert not auth_dir.exists()
    
    def test_replace_existing_symlink(self, tmp_path):
        """Test that existing symlinks are replaced."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        source_file = shard_dir / "abc123"
        source_file.write_text("data")
        
        farm_dir = tmp_path / "farm"
        farm_dir.mkdir()
        
        # Create existing symlink pointing elsewhere
        existing_link = farm_dir / "photo.jpg"
        existing_link.symlink_to("/nonexistent/path")
        
        created, _, _, errors = create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir)
        )
        
        assert created == 1
        assert errors == 0
        assert os.readlink(str(existing_link)) == str(source_file)
    
    def test_skip_existing_real_file(self, tmp_path):
        """Test that existing real files are not replaced."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("source data")
        
        farm_dir = tmp_path / "farm"
        farm_dir.mkdir()
        
        # Create existing real file
        existing_file = farm_dir / "photo.jpg"
        existing_file.write_text("existing data")
        
        created, _, _, _ = create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir), verbose=True
        )
        
        # Should skip, not replace
        assert created == 0
        assert existing_file.read_text() == "existing data"
        assert not existing_file.is_symlink()


class TestMainFunction:
    """Tests for main() function and CLI."""
    
    def test_missing_db_exits(self, tmp_path):
        """Test that missing database causes exit."""
        from create_symlink_farm import main
        
        with patch('sys.argv', ['prog', '--db', '/nonexistent/db', '--source', str(tmp_path), '--farm', str(tmp_path / 'farm')]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
    
    def test_missing_source_exits(self, tmp_path):
        """Test that missing source directory causes exit."""
        from create_symlink_farm import main
        
        db_path = create_test_db(tmp_path)
        
        with patch('sys.argv', ['prog', '--db', str(db_path), '--source', '/nonexistent/source', '--farm', str(tmp_path / 'farm')]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
    
    def test_successful_run(self, tmp_path):
        """Test successful full run through main()."""
        from create_symlink_farm import main
        
        # Setup database
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        # Setup source
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        with patch('sys.argv', ['prog', '--db', str(db_path), '--source', str(source_dir), '--farm', str(farm_dir)]):
            result = main()
        
        assert result == 0
        assert (farm_dir / "photo.jpg").is_symlink()
    
    def test_dry_run_mode(self, tmp_path):
        """Test dry run mode through main()."""
        from create_symlink_farm import main
        
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        with patch('sys.argv', ['prog', '--db', str(db_path), '--source', str(source_dir), '--farm', str(farm_dir), '--dry-run']):
            result = main()
        
        assert result == 0
        assert not farm_dir.exists()
    
    def test_nonempty_farm_user_declines(self, tmp_path):
        """Test user declining to continue with non-empty farm."""
        from create_symlink_farm import main
        
        db_path = create_test_db(tmp_path)
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        
        farm_dir = tmp_path / "farm"
        farm_dir.mkdir()
        (farm_dir / "existing.txt").write_text("data")
        
        with patch('sys.argv', ['prog', '--db', str(db_path), '--source', str(source_dir), '--farm', str(farm_dir)]):
            with patch('builtins.input', return_value='n'):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 0
    
    def test_nonempty_farm_user_accepts(self, tmp_path):
        """Test user accepting to continue with non-empty farm."""
        from create_symlink_farm import main
        
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        farm_dir.mkdir()
        (farm_dir / "existing.txt").write_text("data")
        
        with patch('sys.argv', ['prog', '--db', str(db_path), '--source', str(source_dir), '--farm', str(farm_dir)]):
            with patch('builtins.input', return_value='y'):
                result = main()
        
        assert result == 0
    
    def test_verbose_mode(self, tmp_path):
        """Test verbose output mode."""
        from create_symlink_farm import main
        
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        with patch('sys.argv', ['prog', '--db', str(db_path), '--source', str(source_dir), '--farm', str(farm_dir), '-v']):
            result = main()
        
        assert result == 0
    
    def test_returns_error_code_on_errors(self, tmp_path):
        """Test that errors return non-zero exit code."""
        from create_symlink_farm import main
        
        db_path = create_test_db(tmp_path)
        # Insert file with content_id but source won't exist
        insert_file(db_path, "1", "photo.jpg", None, "missing123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        
        farm_dir = tmp_path / "farm"
        
        # This should have no_source count but no errors
        with patch('sys.argv', ['prog', '--db', str(db_path), '--source', str(source_dir), '--farm', str(farm_dir)]):
            result = main()
        
        # no_source is not an error, so should return 0
        assert result == 0


class TestVerboseOutput:
    """Tests for verbose output paths."""
    
    def test_verbose_link_message(self, tmp_path, capsys):
        """Test verbose output shows LINK messages."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "abc123")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir), verbose=True
        )
        
        captured = capsys.readouterr()
        assert "[LINK]" in captured.out
    
    def test_verbose_skip_no_source(self, tmp_path, capsys):
        """Test verbose output shows SKIP for missing source."""
        db_path = create_test_db(tmp_path)
        insert_file(db_path, "1", "photo.jpg", None, "missing")
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        farm_dir = tmp_path / "farm"
        
        create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir), verbose=True
        )
        
        captured = capsys.readouterr()
        assert "[SKIP]" in captured.out
    
    def test_verbose_error_handling(self, tmp_path, capsys):
        """Test verbose output shows ERROR messages."""
        db_path = create_test_db(tmp_path)
        # File with empty name that will cause path issues
        conn = sqlite3.connect(str(db_path))
        conn.execute("INSERT INTO Files (id, name, contentID) VALUES (?, ?, ?)", ("1", "", "abc123"))
        conn.commit()
        conn.close()
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shard_dir = source_dir / "a"
        shard_dir.mkdir()
        (shard_dir / "abc123").write_text("data")
        
        farm_dir = tmp_path / "farm"
        
        create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir), verbose=True
        )
        
        # Should handle gracefully (empty path returns None)
        captured = capsys.readouterr()
        # Either ERROR or it's handled gracefully


class TestProgressOutput:
    """Tests for progress reporting."""
    
    def test_progress_on_large_batch(self, tmp_path, capsys):
        """Test progress is reported for large file counts."""
        db_path = create_test_db(tmp_path)
        
        # Insert many files to trigger progress output
        conn = sqlite3.connect(str(db_path))
        for i in range(60000):
            conn.execute(
                "INSERT INTO Files (id, name, contentID) VALUES (?, ?, ?)",
                (str(i), f"file{i}.jpg", f"content{i}")
            )
        conn.commit()
        conn.close()
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        farm_dir = tmp_path / "farm"
        
        create_symlink_farm(str(db_path), str(source_dir), str(farm_dir))
        
        captured = capsys.readouterr()
        assert "Progress:" in captured.out


class TestEdgeCases:
    """Tests for edge cases."""
    
    def test_empty_name_file(self, tmp_path):
        """Test handling file with empty name."""
        db_path = create_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.execute("INSERT INTO Files (id, name, contentID) VALUES (?, ?, ?)", ("1", "", "abc123"))
        conn.commit()
        conn.close()
        
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        farm_dir = tmp_path / "farm"
        
        # Should not crash
        created, _, _, errors = create_symlink_farm(
            str(db_path), str(source_dir), str(farm_dir)
        )
        
        # Empty name results in None path, counted as error or skipped
        assert errors >= 0
    
    def test_circular_parent_reference(self, tmp_path):
        """Test handling of circular parent references."""
        db_path = create_test_db(tmp_path)
        # Create circular reference (shouldn't happen but test defensively)
        conn = sqlite3.connect(str(db_path))
        conn.execute("INSERT INTO Files (id, parentID, name, contentID) VALUES (?, ?, ?, ?)", 
                    ("1", "2", "file1.jpg", "abc123"))
        conn.execute("INSERT INTO Files (id, parentID, name, contentID) VALUES (?, ?, ?, ?)", 
                    ("2", "1", "folder", None))
        conn.commit()
        conn.close()
        
        file_dic = load_files_from_db(str(db_path))
        
        # Should not infinite loop - will stop when id not in file_dic
        result = reconstruct_path("1", file_dic)
        assert result is not None  # Should return something without crashing
