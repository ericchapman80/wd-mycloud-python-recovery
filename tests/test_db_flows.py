import os
import sys
import sqlite3
import tempfile
from pathlib import Path

import pytest

# Add parent directory to path to import restsdk_public
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import restsdk_public
from restsdk_public import (
    init_copy_tracking_tables,
    insert_copied_file,
    insert_skipped_file,
    regenerate_copied_files_from_dest,
)


# Minimal fixture schema that mirrors the real table shapes we touch.
FILES_SCHEMA = """
CREATE TABLE Files(
    id TEXT NOT NULL,
    parentID TEXT,
    contentID TEXT,
    version INTEGER NOT NULL DEFAULT 0,
    name TEXT NOT NULL,
    birthTime INTEGER NOT NULL DEFAULT 0,
    cTime INTEGER NOT NULL DEFAULT 0,
    uTime INTEGER,
    mTime INTEGER,
    size INTEGER NOT NULL DEFAULT 0,
    mimeType TEXT NOT NULL DEFAULT '',
    storageID TEXT NOT NULL DEFAULT '',
    hidden INTEGER NOT NULL DEFAULT 1,
    previewSourceContentID TEXT,
    autoID INTEGER PRIMARY KEY,
    imageDate INTEGER,
    imageWidth INTEGER NOT NULL DEFAULT 0,
    imageHeight INTEGER NOT NULL DEFAULT 0,
    imagePreviewWidth INTEGER NOT NULL DEFAULT 0,
    imagePreviewHeight INTEGER NOT NULL DEFAULT 0,
    imageCameraMake TEXT NOT NULL DEFAULT '',
    imageCameraModel TEXT NOT NULL DEFAULT '',
    imageAperture REAL NOT NULL DEFAULT 0,
    imageExposureTime REAL NOT NULL DEFAULT 0,
    imageISOSpeed INTEGER NOT NULL DEFAULT 0,
    imageFocalLength REAL,
    imageFlashFired INTEGER,
    imageOrientation INTEGER NOT NULL DEFAULT 0,
    imageLatitude REAL,
    imageLongitude REAL,
    imageAltitude REAL,
    imageSmall INTEGER NOT NULL DEFAULT 0,
    videoCodec TEXT NOT NULL DEFAULT '',
    videoCodecProfile TEXT NOT NULL DEFAULT '',
    videoCodecLevel INTEGER NOT NULL DEFAULT 0,
    videoAudioCodec TEXT NOT NULL DEFAULT '',
    videoBitRate REAL NOT NULL DEFAULT 0,
    videoFrameRate REAL NOT NULL DEFAULT 0,
    videoWidth INTEGER NOT NULL DEFAULT 0,
    videoHeight INTEGER NOT NULL DEFAULT 0,
    videoDuration REAL NOT NULL DEFAULT 0,
    audioDuration REAL NOT NULL DEFAULT 0,
    audioTitle TEXT NOT NULL DEFAULT '',
    audioAlbum TEXT NOT NULL DEFAULT '',
    audioArtist TEXT NOT NULL DEFAULT '',
    audioComposer TEXT NOT NULL DEFAULT '',
    audioGenre TEXT NOT NULL DEFAULT '',
    audioYear INTEGER NOT NULL DEFAULT 0,
    audioTrackNum INTEGER NOT NULL DEFAULT 0,
    audioTotalTracks INTEGER NOT NULL DEFAULT 0,
    audioWidth INTEGER NOT NULL DEFAULT 0,
    audioHeight INTEGER NOT NULL DEFAULT 0,
    imageCity TEXT NOT NULL DEFAULT '',
    imageCityAlt TEXT NOT NULL DEFAULT '',
    imageProvince TEXT NOT NULL DEFAULT '',
    imageProvinceAlt TEXT NOT NULL DEFAULT '',
    imageCountry TEXT NOT NULL DEFAULT '',
    imageCountryAlt TEXT NOT NULL DEFAULT '',
    custom TEXT NOT NULL DEFAULT '',
    imagePreviewSourceWidth INTEGER NOT NULL DEFAULT 0,
    imagePreviewSourceHeight INTEGER NOT NULL DEFAULT 0,
    tagged INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    videoOrientation INTEGER NOT NULL DEFAULT 0,
    creatorEntityID TEXT,
    videoDate INTEGER,
    videoLatitude REAL,
    videoLongitude REAL,
    videoAltitude REAL,
    videoCity TEXT NOT NULL DEFAULT '',
    videoCityAlt TEXT NOT NULL DEFAULT '',
    videoProvince TEXT NOT NULL DEFAULT '',
    videoProvinceAlt TEXT NOT NULL DEFAULT '',
    videoCountry TEXT NOT NULL DEFAULT '',
    videoCountryAlt TEXT NOT NULL DEFAULT '',
    month INTEGER NOT NULL DEFAULT 0,
    week INTEGER NOT NULL DEFAULT 0,
    documentPageCount INTEGER NOT NULL DEFAULT 0,
    contentHash TEXT,
    aTime INTEGER,
    category INTEGER,
    documentTitle TEXT NOT NULL DEFAULT '',
    documentAuthor TEXT NOT NULL DEFAULT '',
    documentSubject TEXT NOT NULL DEFAULT '',
    documentCreationDate INTEGER,
    documentModifyDate INTEGER,
    documentPasswordProtected INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX Files_id ON Files(id);
CREATE UNIQUE INDEX Files_contentID ON Files(contentID);
"""


def create_db_with_files(tmp_path: Path):
    db_path = tmp_path / "index.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.executescript(FILES_SCHEMA)
    conn.commit()
    conn.close()
    return db_path


def insert_file(conn, fid, name, parent_id=None, content_id=None, ts=None):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Files (id, parentID, contentID, name, imageDate, videoDate, cTime, birthTime) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (fid, parent_id, content_id, name, ts, ts, ts, ts),
    )
    conn.commit()


def test_init_tables_idempotent(tmp_path):
    db_path = create_db_with_files(tmp_path)
    init_copy_tracking_tables(str(db_path))
    init_copy_tracking_tables(str(db_path))  # second call should not fail

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='copied_files'")
    assert cur.fetchone()[0] == "copied_files"
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='skipped_files'")
    assert cur.fetchone()[0] == "skipped_files"
    conn.close()


def test_regen_log_populates_copied_files(tmp_path):
    db_path = create_db_with_files(tmp_path)
    init_copy_tracking_tables(str(db_path))

    # Seed one file record that should match a destination file name
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Files (id, parentID, contentID, name) VALUES (?, ?, ?, ?)",
        ("1", None, "content-1", "photo.jpg"),
    )
    conn.commit()
    conn.close()

    # Destination directory with a matching file and an unmatched file
    dest = tmp_path / "dest"
    dest.mkdir()
    (dest / "photo.jpg").write_text("data")
    (dest / "extra.bin").write_text("x")

    log_file = tmp_path / "copied.log"
    regenerate_copied_files_from_dest(str(db_path), str(dest), str(log_file))

    # Log should include the matching file path
    with open(log_file) as fh:
        logged = fh.read()
    assert "photo.jpg" in logged

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT file_id, filename FROM copied_files")
    rows = cur.fetchall()
    conn.close()

    assert rows == [("1", "photo.jpg")]


def test_insert_copied_and_skipped(tmp_path):
    db_path = create_db_with_files(tmp_path)
    init_copy_tracking_tables(str(db_path))

    insert_copied_file(str(db_path), "42", "foo.txt")
    insert_skipped_file(str(db_path), "bar.txt", "missing in db")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT file_id, filename FROM copied_files")
    copied = cur.fetchall()
    cur.execute("SELECT filename, reason FROM skipped_files")
    skipped = cur.fetchall()
    conn.close()

    assert copied == [("42", "foo.txt")]
    assert skipped == [("bar.txt", "missing in db")]


def test_retry_wrapped_inserts_do_not_raise(tmp_path):
    """
    Ensure insert helpers succeed (or no-op) even when called back-to-back,
    exercising the retry/busy_timeout path.
    """
    db_path = create_db_with_files(tmp_path)
    init_copy_tracking_tables(str(db_path))

    # First insert should succeed
    insert_copied_file(str(db_path), "1", "foo.txt")
    # Second insert is a no-op (OR IGNORE) but still should not raise
    insert_copied_file(str(db_path), "1", "foo.txt")

    # Similarly for skipped_files
    insert_skipped_file(str(db_path), "bar.txt", "missing")
    insert_skipped_file(str(db_path), "bar.txt", "missing")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM copied_files")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT COUNT(*) FROM skipped_files")
    assert cur.fetchone()[0] == 1
    conn.close()


def test_refresh_mtime_existing(tmp_path, monkeypatch):
    db_path = create_db_with_files(tmp_path)
    init_copy_tracking_tables(str(db_path))

    # Seed file with timestamp
    conn = sqlite3.connect(str(db_path))
    insert_file(conn, "1", "photo.jpg", None, "content-1", 1_600_000_000_000)
    conn.close()

    # Set up globals expected by copy_file
    restsdk_public.fileDIC = {
        "1": {
            "Name": "photo.jpg",
            "Parent": None,
            "contentID": "content-1",
            "imageDate": 1_600_000_000_000,
            "videoDate": None,
            "cTime": None,
            "birthTime": None,
        }
    }
    restsdk_public.total_files = 1
    restsdk_public.processed_files_counter = restsdk_public.Value("i", 0)
    restsdk_public.copied_files_counter = restsdk_public.Value("i", 0)
    restsdk_public.skipped_files_counter = restsdk_public.Value("i", 0)
    restsdk_public.lock = restsdk_public.Lock()
    restsdk_public.copied_files = set()
    # Build reverse lookup dictionaries for O(1) filename->ID mapping
    restsdk_public.build_reverse_lookups()

    src_dir = tmp_path / "src"
    dest_dir = tmp_path / "dest"
    src_dir.mkdir()
    dest_dir.mkdir()
    src_file = src_dir / "content-1"
    dest_file = dest_dir / "photo.jpg"
    src_file.write_text("data")
    dest_file.write_text("existing")  # Already present

    # Pretend args
    class Args:
        preserve_mtime = True
        refresh_mtime_existing = True
        sanitize_pipes = False
    restsdk_public.args = Args()

    before_mtime = dest_file.stat().st_mtime
    restsdk_public.copy_file(str(src_dir), "content-1", [str(src_dir)], str(dest_dir), False, str(tmp_path / "log.log"))
    after_mtime = dest_file.stat().st_mtime

    # mtime should update (set to 1_600_000_000_000 ms -> seconds)
    assert int(after_mtime) == 1_600_000_000
    # counters should reflect skip path
    assert restsdk_public.skipped_files_counter.value == 1
