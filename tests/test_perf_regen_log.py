import os
import sys
import sqlite3
import time
from pathlib import Path

import pytest

# Add parent directory to path to import restsdk_public
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from restsdk_public import init_copy_tracking_tables, regenerate_copied_files_from_dest


def seed_db(db_path: Path, rows: int):
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE Files(
            id TEXT NOT NULL PRIMARY KEY,
            parentID TEXT,
            contentID TEXT UNIQUE,
            version INTEGER NOT NULL DEFAULT 0,
            name TEXT NOT NULL
        );
        """
    )
    init_copy_tracking_tables(str(db_path))
    payload = [(f"id-{i}", None, f"content-{i}", f"file-{i}.bin") for i in range(rows)]
    cur.executemany("INSERT INTO Files (id, parentID, contentID, version, name) VALUES (?, ?, ?, 0, ?)", payload)
    conn.commit()
    conn.close()


@pytest.mark.perf
@pytest.mark.skipif(os.environ.get("PERF_TEST_ROWS") is None, reason="Set PERF_TEST_ROWS to enable perf test")
def test_regen_log_perf(tmp_path, perf_row_count):
    rows = perf_row_count
    db_path = tmp_path / "perf.db"
    seed_db(db_path, rows)

    dest = tmp_path / "dest"
    dest.mkdir()
    # Create dest files for the first 1% of rows to exercise matching logic
    sample = max(1, rows // 100)
    for i in range(sample):
        (dest / f"file-{i}.bin").write_text("x")

    log_file = tmp_path / "perf.log"
    start = time.time()
    regenerate_copied_files_from_dest(str(db_path), str(dest), str(log_file))
    elapsed = time.time() - start

    # Assert log exists and DB has the matched entries
    assert log_file.exists()
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM copied_files")
    copied_count = cur.fetchone()[0]
    conn.close()

    assert copied_count == sample

    # Soft budget: regen should complete within a modest time for the seeded size
    # (tunable threshold; generous for small/medium datasets)
    assert elapsed < 5, f"regen-log took too long for {rows} rows: {elapsed:.2f}s"
