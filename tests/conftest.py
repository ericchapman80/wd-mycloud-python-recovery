import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

@pytest.fixture(autouse=True)
def cleanup_db_connections():
    """Clean up thread-local database connections after each test."""
    yield
    # Import here to avoid circular dependency
    try:
        from restsdk_public import close_all_db_connections
        close_all_db_connections()
    except ImportError:
        pass

# Default perf size; can be overridden via PERF_TEST_ROWS env var
def pytest_addoption(parser):
    parser.addoption(
        "--perf-rows",
        action="store",
        default=os.environ.get("PERF_TEST_ROWS"),
        help="Number of rows to seed for perf tests (defaults to env PERF_TEST_ROWS).",
    )


@pytest.fixture
def perf_row_count(pytestconfig):
    val = pytestconfig.getoption("--perf-rows")
    if val is None:
        return None
    try:
        return int(val)
    except ValueError:
        return None
