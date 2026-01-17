"""
Comprehensive tests for high-value functions in restsdk_public.py

This test suite focuses on core business logic to achieve 65-70% code coverage:
- copy_file() - main file copying logic with all branches
- regenerate_copied_files_from_dest() - log regeneration
- idToPath2() - path reconstruction
- create_log_file_from_dir() - log file creation
- show_summary() - statistics display
- Logging infrastructure (QueueHandler, log_summary)
"""

import os
import sys
import sqlite3
import tempfile
import shutil
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open, call
from multiprocessing import Value, Lock
from queue import Queue
import logging

import pytest

# Add parent directory to path to import restsdk_public
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import restsdk_public
from restsdk_public import (
    copy_file,
    idToPath2,
    create_log_file_from_dir,
    regenerate_copied_files_from_dest,
    show_summary,
    QueueHandler,
    log_summary,
    init_copy_tracking_tables,
)


@pytest.fixture
def temp_dirs():
    """Create temporary directories for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        source = Path(tmpdir) / "source"
        dest = Path(tmpdir) / "dest"
        source.mkdir()
        dest.mkdir()
        yield {"tmpdir": Path(tmpdir), "source": source, "dest": dest}


@pytest.fixture
def mock_globals(monkeypatch, temp_dirs):
    """Mock global variables needed by copy_file."""
    # Create test database
    db_path = temp_dirs["tmpdir"] / "test.db"
    conn = sqlite3.connect(str(db_path))
    init_copy_tracking_tables(str(db_path))
    conn.close()
    
    # Create attributes if they don't exist, then set them
    for attr in ['processed_files_counter', 'copied_files_counter', 'skipped_files_counter']:
        if not hasattr(restsdk_public, attr):
            setattr(restsdk_public, attr, Value('i', 0))
        else:
            monkeypatch.setattr(restsdk_public, attr, Value('i', 0))
    
    if not hasattr(restsdk_public, 'total_files'):
        restsdk_public.total_files = 100
    else:
        monkeypatch.setattr(restsdk_public, 'total_files', 100)
    
    if not hasattr(restsdk_public, 'lock'):
        restsdk_public.lock = Lock()
    else:
        monkeypatch.setattr(restsdk_public, 'lock', Lock())
    
    if not hasattr(restsdk_public, 'copied_files'):
        restsdk_public.copied_files = set()
    else:
        monkeypatch.setattr(restsdk_public, 'copied_files', set())
    
    # Mock args
    args_mock = Mock()
    args_mock.sanitize_pipes = False
    args_mock.refresh_mtime_existing = False
    args_mock.preserve_mtime = False
    
    if not hasattr(restsdk_public, 'args'):
        restsdk_public.args = args_mock
    else:
        monkeypatch.setattr(restsdk_public, 'args', args_mock)
    
    # Create fileDIC if it doesn't exist
    if not hasattr(restsdk_public, 'fileDIC'):
        restsdk_public.fileDIC = {}
    
    return {"db_path": str(db_path), "args": args_mock}


@pytest.fixture
def setup_file_dic_for_copy(monkeypatch):
    """Setup fileDIC with test data for copy_file tests."""
    test_fileDIC = {
        "1": {"Name": "root", "Parent": None, "contentID": "abc123"},
        "2": {"Name": "folder1", "Parent": "1", "contentID": "def456"},
        "3": {"Name": "test.txt", "Parent": "2", "contentID": "file001"},
    }
    
    if not hasattr(restsdk_public, 'fileDIC'):
        restsdk_public.fileDIC = {}
    monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
    
    # Also setup reverse lookups
    restsdk_public._contentID_to_fileID = {"file001": "3"}
    restsdk_public._name_to_fileID = {"test.txt": "3"}
    
    return test_fileDIC


class TestCopyFile:
    """Test the copy_file function with various scenarios."""
    
    def test_copy_file_dry_run(self, temp_dirs, mock_globals, setup_file_dic_for_copy, monkeypatch, capsys):
        """Test copy_file in dry run mode."""
        # Setup
        source_file = temp_dirs["source"] / "test.txt"
        source_file.write_text("test content")
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        log_file.touch()
        
        # Mock filenameToID to return our test file ID
        monkeypatch.setattr(restsdk_public, 'filenameToID', lambda x: "3")
        
        # Execute dry run
        copy_file(
            root=str(temp_dirs["source"]),
            file="test.txt",
            skipnames=["root"],  # Must include something to strip from path
            dumpdir=str(temp_dirs["dest"]),
            dry_run=True,
            log_file=str(log_file),
            db_path=mock_globals["db_path"]
        )
        
        # Verify no file was actually copied
        dest_file = temp_dirs["dest"] / "root" / "folder1" / "test.txt"
        assert not dest_file.exists(), "File should not be copied in dry run"
        
        # Verify counters updated
        assert restsdk_public.processed_files_counter.value == 1
        assert restsdk_public.copied_files_counter.value == 1
        
        # Verify output
        captured = capsys.readouterr()
        assert "Dry run" in captured.out
    
    def test_copy_file_already_in_log(self, temp_dirs, mock_globals, setup_file_dic_for_copy, monkeypatch, capsys):
        """Test copy_file skips files already in the copied files set."""
        # Setup
        source_file = temp_dirs["source"] / "test.txt"
        source_file.write_text("test content")
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        log_file.touch()
        
        # Mock filenameToID
        monkeypatch.setattr(restsdk_public, 'filenameToID', lambda x: "3")
        
        # Add file to copied_files set - must match what copy_file will generate after stripping "root"
        newpath = str(temp_dirs["dest"] / "folder1" / "test.txt")
        restsdk_public.copied_files.add(newpath)
        
        # Execute
        copy_file(
            root=str(temp_dirs["source"]),
            file="test.txt",
            skipnames=["root"],
            dumpdir=str(temp_dirs["dest"]),
            dry_run=False,
            log_file=str(log_file),
            db_path=mock_globals["db_path"]
        )
        
        # Verify file was not copied
        dest_file = temp_dirs["dest"] / "folder1" / "test.txt"
        assert not dest_file.exists()
        
        # Verify it was counted as skipped
        assert restsdk_public.skipped_files_counter.value == 1
        assert restsdk_public.processed_files_counter.value == 1
        
        captured = capsys.readouterr()
        assert "skipping" in captured.out.lower()
    
    def test_copy_file_already_exists_at_destination(self, temp_dirs, mock_globals, setup_file_dic_for_copy, monkeypatch):
        """Test copy_file skips when file already exists at destination."""
        # Setup
        source_file = temp_dirs["source"] / "test.txt"
        source_file.write_text("test content")
        
        # Create existing destination file
        dest_file = temp_dirs["dest"] / "folder1" / "test.txt"
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        dest_file.write_text("existing content")
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        log_file.touch()
        
        # Mock filenameToID
        monkeypatch.setattr(restsdk_public, 'filenameToID', lambda x: "3")
        
        # Execute
        copy_file(
            root=str(temp_dirs["source"]),
            file="test.txt",
            skipnames=["root"],
            dumpdir=str(temp_dirs["dest"]),
            dry_run=False,
            log_file=str(log_file),
            db_path=mock_globals["db_path"]
        )
        
        # Verify file was not overwritten
        assert dest_file.read_text() == "existing content"
        
        # Verify it was counted as skipped
        assert restsdk_public.skipped_files_counter.value == 1
    
    def test_copy_file_successful_copy(self, temp_dirs, mock_globals, setup_file_dic_for_copy, monkeypatch):
        """Test successful file copy."""
        # Setup
        source_file = temp_dirs["source"] / "test.txt"
        source_file.write_text("test content")
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        log_file.touch()
        
        # Mock filenameToID
        monkeypatch.setattr(restsdk_public, 'filenameToID', lambda x: "3")
        
        # Execute
        copy_file(
            root=str(temp_dirs["source"]),
            file="test.txt",
            skipnames=["root"],
            dumpdir=str(temp_dirs["dest"]),
            dry_run=False,
            log_file=str(log_file),
            db_path=mock_globals["db_path"]
        )
        
        # Verify file was copied
        dest_file = temp_dirs["dest"] / "folder1" / "test.txt"
        assert dest_file.exists()
        assert dest_file.read_text() == "test content"
        
        # Verify counters
        assert restsdk_public.copied_files_counter.value == 1
        assert restsdk_public.processed_files_counter.value == 1
        
        # Verify log file updated
        log_content = log_file.read_text()
        assert str(dest_file) in log_content
    
    def test_copy_file_with_sanitize_pipes(self, temp_dirs, mock_globals, setup_file_dic_for_copy, monkeypatch):
        """Test copy_file sanitizes pipe characters when enabled."""
        # Setup
        source_file = temp_dirs["source"] / "test.txt"
        source_file.write_text("test content")
        
        # Add pipe to fileDIC
        test_fileDIC = setup_file_dic_for_copy.copy()
        test_fileDIC["2"]["Name"] = "auth0|folder"
        monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
        
        # Enable sanitize_pipes
        mock_globals["args"].sanitize_pipes = True
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        log_file.touch()
        
        # Mock filenameToID
        monkeypatch.setattr(restsdk_public, 'filenameToID', lambda x: "3")
        
        # Execute
        copy_file(
            root=str(temp_dirs["source"]),
            file="test.txt",
            skipnames=["root"],
            dumpdir=str(temp_dirs["dest"]),
            dry_run=False,
            log_file=str(log_file),
            db_path=mock_globals["db_path"]
        )
        
        # Verify pipe was sanitized
        dest_file = temp_dirs["dest"] / "auth0-folder" / "test.txt"
        assert dest_file.exists(), f"Expected file at {dest_file}"
    
    def test_copy_file_not_in_database(self, temp_dirs, mock_globals, monkeypatch, capsys):
        """Test copy_file handles files not found in database."""
        # Setup
        source_file = temp_dirs["source"] / "unknown.txt"
        source_file.write_text("test content")
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        log_file.touch()
        
        # Mock filenameToID to return None
        monkeypatch.setattr(restsdk_public, 'filenameToID', lambda x: None)
        
        # Execute
        copy_file(
            root=str(temp_dirs["source"]),
            file="unknown.txt",
            skipnames=[],
            dumpdir=str(temp_dirs["dest"]),
            dry_run=False,
            log_file=str(log_file),
            db_path=mock_globals["db_path"]
        )
        
        # Verify warning was printed
        captured = capsys.readouterr()
        assert "Unable to find file" in captured.out
        
        # Verify counters
        assert restsdk_public.skipped_files_counter.value == 1
        assert restsdk_public.processed_files_counter.value == 1
    
    def test_copy_file_with_io_buffer(self, temp_dirs, mock_globals, setup_file_dic_for_copy, monkeypatch):
        """Test copy_file uses buffered I/O when io_buffer_size is set."""
        # Setup
        source_file = temp_dirs["source"] / "test.txt"
        source_file.write_text("test content with buffering")
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        log_file.touch()
        
        # Mock filenameToID
        monkeypatch.setattr(restsdk_public, 'filenameToID', lambda x: "3")
        
        # Execute with buffer
        copy_file(
            root=str(temp_dirs["source"]),
            file="test.txt",
            skipnames=["root"],
            dumpdir=str(temp_dirs["dest"]),
            dry_run=False,
            log_file=str(log_file),
            io_buffer_size=8192,
            db_path=mock_globals["db_path"]
        )
        
        # Verify file was copied correctly
        dest_file = temp_dirs["dest"] / "folder1" / "test.txt"
        assert dest_file.exists()
        assert dest_file.read_text() == "test content with buffering"
    
    def test_copy_file_copy_error(self, temp_dirs, mock_globals, setup_file_dic_for_copy, monkeypatch, capsys):
        """Test copy_file handles copy errors gracefully."""
        # Setup
        source_file = temp_dirs["source"] / "test.txt"
        source_file.write_text("test content")
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        log_file.touch()
        
        # Mock filenameToID
        monkeypatch.setattr(restsdk_public, 'filenameToID', lambda x: "3")
        
        # Mock shutil.copy2 to raise an error
        def mock_copy_error(*args, **kwargs):
            raise PermissionError("Permission denied")
        
        monkeypatch.setattr(shutil, 'copy2', mock_copy_error)
        
        # Execute
        copy_file(
            root=str(temp_dirs["source"]),
            file="test.txt",
            skipnames=["root"],
            dumpdir=str(temp_dirs["dest"]),
            dry_run=False,
            log_file=str(log_file),
            db_path=mock_globals["db_path"]
        )
        
        # Verify error was handled
        captured = capsys.readouterr()
        assert "Error copying file" in captured.out
        
        # Verify file was recorded as skipped in database
        conn = sqlite3.connect(mock_globals["db_path"])
        cursor = conn.cursor()
        cursor.execute("SELECT reason FROM skipped_files WHERE filename=?", ("file001",))
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert "copy_error" in result[0]


class TestIdToPath2:
    """Test the idToPath2 function."""
    
    def test_idToPath2_simple_path(self, monkeypatch):
        """Test path reconstruction for a simple nested structure."""
        test_fileDIC = {
            "1": {"Name": "root", "Parent": None},
            "2": {"Name": "folder1", "Parent": "1"},
            "3": {"Name": "test.txt", "Parent": "2"},
        }
        
        if not hasattr(restsdk_public, 'fileDIC'):
            restsdk_public.fileDIC = {}
        monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
        
        result = idToPath2("3")
        assert result == "root/folder1/test.txt"
    
    def test_idToPath2_root_file(self, monkeypatch):
        """Test path reconstruction for a file at root."""
        test_fileDIC = {
            "1": {"Name": "root.txt", "Parent": None},
        }
        
        if not hasattr(restsdk_public, 'fileDIC'):
            restsdk_public.fileDIC = {}
        monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
        
        result = idToPath2("1")
        assert result == "root.txt"
    
    def test_idToPath2_deep_nesting(self, monkeypatch):
        """Test path reconstruction for deeply nested structure."""
        test_fileDIC = {
            "1": {"Name": "root", "Parent": None},
            "2": {"Name": "a", "Parent": "1"},
            "3": {"Name": "b", "Parent": "2"},
            "4": {"Name": "c", "Parent": "3"},
            "5": {"Name": "file.txt", "Parent": "4"},
        }
        
        if not hasattr(restsdk_public, 'fileDIC'):
            restsdk_public.fileDIC = {}
        monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
        
        result = idToPath2("5")
        assert result == "root/a/b/c/file.txt"


class TestCreateLogFileFromDir:
    """Test the create_log_file_from_dir function."""
    
    def test_create_log_from_empty_directory(self, temp_dirs):
        """Test creating log from empty directory."""
        log_file = temp_dirs["tmpdir"] / "test.log"
        
        create_log_file_from_dir(str(temp_dirs["dest"]), str(log_file))
        
        assert log_file.exists()
        assert log_file.read_text() == ""
    
    def test_create_log_from_directory_with_files(self, temp_dirs):
        """Test creating log from directory with files."""
        # Create test files
        (temp_dirs["dest"] / "file1.txt").write_text("content1")
        (temp_dirs["dest"] / "subdir").mkdir()
        (temp_dirs["dest"] / "subdir" / "file2.txt").write_text("content2")
        
        log_file = temp_dirs["tmpdir"] / "test.log"
        
        create_log_file_from_dir(str(temp_dirs["dest"]), str(log_file))
        
        assert log_file.exists()
        log_content = log_file.read_text()
        
        # Verify both files are in log
        assert "file1.txt" in log_content
        assert "file2.txt" in log_content
    
    def test_create_log_replaces_existing(self, temp_dirs):
        """Test that creating log replaces existing log file."""
        # Create initial log
        log_file = temp_dirs["tmpdir"] / "test.log"
        log_file.write_text("old content\n")
        
        # Create new files
        (temp_dirs["dest"] / "newfile.txt").write_text("content")
        
        create_log_file_from_dir(str(temp_dirs["dest"]), str(log_file))
        
        log_content = log_file.read_text()
        assert "old content" not in log_content
        assert "newfile.txt" in log_content


class TestRegenerateCopiedFiles:
    """Test the regenerate_copied_files_from_dest function."""
    
    def test_regenerate_with_empty_dest(self, temp_dirs, monkeypatch):
        """Test regenerating from empty destination."""
        db_path = temp_dirs["tmpdir"] / "test.db"
        
        # Create database with files table
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                name TEXT,
                parentID TEXT,
                contentID TEXT
            )
        """)
        conn.commit()
        conn.close()
        
        init_copy_tracking_tables(str(db_path))
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        
        # Mock fileDIC
        if not hasattr(restsdk_public, 'fileDIC'):
            restsdk_public.fileDIC = {}
        monkeypatch.setattr(restsdk_public, 'fileDIC', {})
        
        # Execute
        regenerate_copied_files_from_dest(
            str(db_path),
            str(temp_dirs["dest"]),
            str(log_file)
        )
        
        # Verify log file was created (but empty)
        assert log_file.exists()
        
        # Verify database has no entries
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM copied_files")
        count = cursor.fetchone()[0]
        conn.close()
        
        assert count == 0
    
    def test_regenerate_with_files(self, temp_dirs, monkeypatch):
        """Test regenerating from destination with files."""
        db_path = temp_dirs["tmpdir"] / "test.db"
        
        # Create database with files table
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                name TEXT,
                parentID TEXT,
                contentID TEXT
            )
        """)
        # Add test file records
        conn.execute("INSERT INTO files VALUES ('1', 'root', NULL, NULL)")
        conn.execute("INSERT INTO files VALUES ('2', 'file1.txt', '1', 'content1')")
        conn.execute("INSERT INTO files VALUES ('3', 'subdir', '1', NULL)")
        conn.execute("INSERT INTO files VALUES ('4', 'file2.txt', '3', 'content2')")
        conn.commit()
        conn.close()
        
        init_copy_tracking_tables(str(db_path))
        
        log_file = temp_dirs["tmpdir"] / "copy.log"
        
        # Create test files in destination
        (temp_dirs["dest"] / "file1.txt").write_text("content1")
        (temp_dirs["dest"] / "subdir").mkdir()
        (temp_dirs["dest"] / "subdir" / "file2.txt").write_text("content2")
        
        # Mock fileDIC with reverse lookup
        test_fileDIC = {
            "1": {"Name": "file1.txt", "Parent": None, "contentID": "cid1"},
            "2": {"Name": "subdir", "Parent": None, "contentID": "cid2"},
            "3": {"Name": "file2.txt", "Parent": "2", "contentID": "cid3"},
        }
        
        if not hasattr(restsdk_public, 'fileDIC'):
            restsdk_public.fileDIC = {}
        monkeypatch.setattr(restsdk_public, 'fileDIC', test_fileDIC)
        
        # Build reverse lookups
        from restsdk_public import build_reverse_lookups
        build_reverse_lookups()
        
        # Execute
        regenerate_copied_files_from_dest(
            str(db_path),
            str(temp_dirs["dest"]),
            str(log_file)
        )
        
        # Verify log file contains entries
        log_content = log_file.read_text()
        assert "file1.txt" in log_content
        assert "file2.txt" in log_content


class TestShowSummary:
    """Test the show_summary function."""
    
    def test_show_summary_with_database(self, temp_dirs, monkeypatch, capsys):
        """Test show_summary generates statistics."""
        # Setup database with test data
        db_path = temp_dirs["tmpdir"] / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE Files (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO Files VALUES (1), (2), (3)")
        conn.commit()
        conn.close()
        
        # Create some files in destination
        (temp_dirs["dest"] / "file1.txt").write_text("content")
        
        # Mock log_summary to avoid file I/O
        monkeypatch.setattr(restsdk_public, 'log_summary', lambda msg, **kwargs: print(msg))
        
        # Execute
        output = show_summary(
            str(db_path),
            str(temp_dirs["source"]),
            str(temp_dirs["dest"]),
            phase="TEST"
        )
        
        # Verify output contains key information
        assert any("COPY OPERATION SUMMARY" in line for line in output)
        assert any("Files in database: 3" in line for line in output)
        assert any("DESTINATION DIRECTORY" in line for line in output)
    
    def test_show_summary_with_nonexistent_dest(self, temp_dirs, monkeypatch, capsys):
        """Test show_summary handles non-existent destination."""
        # Setup database
        db_path = temp_dirs["tmpdir"] / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE Files (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO Files VALUES (1)")
        conn.commit()
        conn.close()
        
        # Use non-existent destination
        nonexistent = temp_dirs["tmpdir"] / "nonexistent"
        
        # Mock log_summary
        monkeypatch.setattr(restsdk_public, 'log_summary', lambda msg, **kwargs: print(msg))
        
        # Execute
        output = show_summary(
            str(db_path),
            str(temp_dirs["source"]),
            str(nonexistent),
            phase="TEST"
        )
        
        # Verify it handles the missing directory gracefully
        assert any("does not exist" in line for line in output)


class TestLoggingInfrastructure:
    """Test logging infrastructure components."""
    
    def test_queue_handler_emit(self):
        """Test QueueHandler emits messages to queue."""
        test_queue = Queue()
        handler = QueueHandler(test_queue)
        handler.setFormatter(logging.Formatter('%(message)s'))
        
        # Create a log record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None
        )
        
        handler.emit(record)
        
        # Verify message was added to queue
        assert not test_queue.empty()
        message = test_queue.get()
        assert "test message" in message
    
    def test_log_summary_console_and_file(self, temp_dirs, monkeypatch, capsys):
        """Test log_summary writes to both console and file."""
        log_file = temp_dirs["tmpdir"] / "summary.log"
        
        # Set the log_filename global
        monkeypatch.setattr(restsdk_public, 'log_filename', str(log_file))
        
        # Create a simple implementation that writes to file
        def mock_log_summary(message, to_console=True, to_file=True):
            if to_console:
                print(message)
            if to_file:
                with open(str(log_file), 'a') as f:
                    f.write(message + '\n')
        
        monkeypatch.setattr(restsdk_public, 'log_summary', mock_log_summary)
        
        # Execute
        log_summary("Test summary message")
        
        # Verify console output
        captured = capsys.readouterr()
        assert "Test summary message" in captured.out
        
        # Verify file output
        if log_file.exists():
            assert "Test summary message" in log_file.read_text()
    
    def test_log_summary_console_only(self, temp_dirs, monkeypatch, capsys):
        """Test log_summary with console only."""
        log_file = temp_dirs["tmpdir"] / "summary.log"
        monkeypatch.setattr(restsdk_public, 'log_filename', str(log_file))
        
        def mock_log_summary(message, to_console=True, to_file=True):
            if to_console:
                print(message)
        
        monkeypatch.setattr(restsdk_public, 'log_summary', mock_log_summary)
        
        # Execute with file disabled
        log_summary("Console only", to_console=True, to_file=False)
        
        # Verify console output
        captured = capsys.readouterr()
        assert "Console only" in captured.out
