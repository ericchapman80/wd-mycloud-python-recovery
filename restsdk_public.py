import argparse
import copy
import logging
import multiprocessing
import os
import shutil
import pprint
import sqlite3
import sys
import threading
import time
import os
import traceback
from collections import defaultdict
try:
    import psutil
except ImportError:
    psutil = None
try:
    from argparse import BooleanOptionalAction
except ImportError:
    class BooleanOptionalAction(argparse.Action):
        def __init__(self, option_strings, dest, default=None, **kwargs):
            _option_strings = []
            for option in option_strings:
                _option_strings.append(option)
                if option.startswith('--'):
                    _option_strings.append('--no-' + option[2:])
            super().__init__(option_strings=_option_strings, dest=dest, nargs=0, default=default, **kwargs)

        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, not option_string.startswith('--no-'))
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Value
from shutil import copyfile
import datetime
import gc
from queue import Queue
from threading import Thread

# Thread-safe database connection pool to prevent FD leaks
_db_lock = threading.Lock()
_db_connections = {}  # thread_id -> connection

def get_thread_db_connection(db_path):
    """Get or create a thread-local database connection."""
    thread_id = threading.get_ident()
    if thread_id not in _db_connections:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA busy_timeout=5000")
        _db_connections[thread_id] = conn
    return _db_connections[thread_id]

def close_all_db_connections():
    """Close all thread-local database connections."""
    for conn in _db_connections.values():
        try:
            conn.close()
        except:
            pass
    _db_connections.clear()

# Increase file descriptor limit at startup (prevents "Too many open files" errors)
def _increase_fd_limit():
    """Attempt to increase the file descriptor limit to 65536 or max available."""
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = min(65536, hard)  # Don't exceed hard limit
        if soft < target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
            new_soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            print(f"Increased file descriptor limit: {soft} → {new_soft}")
        else:
            print(f"File descriptor limit already sufficient: {soft}")
    except (ImportError, ValueError, OSError) as e:
        print(f"Warning: Could not increase file descriptor limit: {e}")
        print("Consider running: ulimit -n 65536")

_increase_fd_limit()

# Preflight import
try:
    from preflight import preflight_summary, print_preflight_report
except ImportError as e:
    print("❌ ERROR: Could not import preflight module. Make sure preflight.py is in the same directory.")
    print("Tip: run `bash setup.sh` or `pip install -r requirements.txt` to install dependencies.")
    print(f"Details: {e}")
    sys.exit(1)

##Intended for python3.6 on linux, probably won't work on Windows
##This software is distributed without any warranty. It will probably brick your computer.
#--db=/mnt/backupdrive/restsdk/data/db/index.db --filedir=/mnt/backupdrive/restsdk/data/files --dumpdir=/mnt/nfs-media --dry_run --log_file=/home/chapman/projects/mycloud-restsdk-recovery-script/copied_file.log
#sudo python3 restsdk_public.py --db=/mnt/backupdrive/restsdk/data/db/index.db --filedir=/mnt/backupdrive/restsdk/data/files --dumpdir=/mnt/nfs-media --dry_run --log_file=/home/chapman/projects/mycloud-restsdk-recovery-script/copied_file.log --thread-count=12

# Generate a timestamp for the log file
current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# log_filename is used to store run information, such as progress and errors, in a timestamped log file.
log_filename = f'summary_{current_time}.log'

# log_file is used to track the files that have been successfully copied to avoid duplication in future runs.
# log_file = args.log_file

# Set up a logging queue for asynchronous logging
log_queue = Queue()

# Define a custom logging handler to use the queue
class QueueHandler(logging.Handler):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        self.queue.put(self.format(record))

# Define a worker thread to process log messages asynchronously
def log_worker():
    with open(log_filename, 'a') as log_file:
        while True:
            message = log_queue.get()
            if message == "STOP":
                break
            log_file.write(message + '\n')
            log_file.flush()

# Start the logging worker thread
log_thread = Thread(target=log_worker, daemon=True)
log_thread.start()

# Set up the logging configuration
queue_handler = QueueHandler(log_queue)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
queue_handler.setFormatter(formatter)
logging.getLogger().addHandler(queue_handler)
logging.getLogger().setLevel(logging.INFO)  # Default level is INFO
def print_help():
    print("Usage: python restsdk_public.py [options]")
    print("Options:")
    print("  --preflight           Run hardware and file system pre-flight check (requires --filedir and --dumpdir)")
    print("  --dry_run             Perform a dry run (do not copy files)")
    print("  --db                  Path to the file DB (example: /restsdk/data/db/index.db)")
    print("  --filedir             Path to the files directory (example: /restsdk/data/files)")
    print("  --dumpdir             Path to the directory to dump files (example: /location/to/dump/files/to)")
    print("  --log_file            Path to the log file (example: /location/to/log/file.log)")
    print("  --create_log          Create a log file from an existing run where logging was not in place")
    print("  --regen-log           Regenerate the log file from the destination directory only, then exit")
    print("  --resume              Resume a previous run, regenerating the log before copying (default)")
    print("  --no-regen-log        Use with --resume to skip regenerating the log (advanced)")
    print("  --thread-count        Number of threads to use")
    print("  --log_level {DEBUG,INFO,WARNING}  Logging level (default INFO)")
    print("  --preserve-mtime      After copy, set destination mtime from DB timestamps (imageDate/videoDate/cTime/birthTime)")
    print("  --refresh-mtime-existing  Refresh mtime on existing dest files without recopying when preserve-mtime is on")
    print("  --sanitize-pipes      Replace '|' with '-' in destination paths (use for Windows/NTFS/SMB targets)")
    print("  --io-buffer-size      Optional I/O buffer size in bytes for file copies (default: use shutil.copy2 defaults)")
    print("  --io-max-concurrency  Optional max concurrent disk operations (semaphore). 0 disables limiting")

PIPE_FS_TAGS = ("ntfs", "vfat", "fat", "msdos", "exfat", "cifs", "smb")

def detect_fs_type(path):
    """Best-effort detection of filesystem type for a path."""
    if not psutil:
        return (None, None)
    target = os.path.abspath(path)
    best = (None, None, -1)
    for part in psutil.disk_partitions(all=True):
        mp = part.mountpoint
        if target == mp or target.startswith(mp.rstrip(os.sep) + os.sep):
            if len(mp) > best[2]:
                best = (part.fstype, mp, len(mp))
    return (best[0], best[1])

def is_pipe_sensitive_fs(fstype):
    if not fstype:
        return False
    fs = fstype.lower()
    return any(tag in fs for tag in PIPE_FS_TAGS)

# --- SQL DDL/DML and Hybrid Copy Logic ---
import sqlite3

def init_copy_tracking_tables(db_path):
    """
    Ensure the copied_files and skipped_files tables exist in the database.
    Uses TEXT for file_id and filename to match Files table schema.
    Adds mtime_refreshed flag to track whether we applied DB timestamps to the dest file.
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS copied_files (
            file_id TEXT PRIMARY KEY,
            filename TEXT,
            copied_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            mtime_refreshed INTEGER DEFAULT 0
        )''')
        # Safe ALTER in case table already exists without the new column
        c.execute("PRAGMA table_info(copied_files)")
        cols = {row[1] for row in c.fetchall()}
        if "mtime_refreshed" not in cols:
            c.execute("ALTER TABLE copied_files ADD COLUMN mtime_refreshed INTEGER DEFAULT 0")
        c.execute('''CREATE TABLE IF NOT EXISTS skipped_files (
            filename TEXT PRIMARY KEY,
            reason TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()

def insert_copied_file(db_path, file_id, filename):
    """Insert a copied file record using thread-local connection."""
    def _op():
        with _db_lock:
            conn = get_thread_db_connection(db_path)
            c = conn.cursor()
            c.execute(
                '''INSERT OR IGNORE INTO copied_files (file_id, filename, mtime_refreshed) VALUES (?, ?, 0)''',
                (str(file_id), str(filename)),
            )
            conn.commit()
    with_retry_db(_op)

def insert_skipped_file(db_path, filename, reason):
    """Insert a skipped file record using thread-local connection."""
    def _op():
        with _db_lock:
            conn = get_thread_db_connection(db_path)
            c = conn.cursor()
            c.execute(
                '''INSERT OR IGNORE INTO skipped_files (filename, reason) VALUES (?, ?)''',
                (str(filename), str(reason)),
            )
            conn.commit()
    with_retry_db(_op)

def regenerate_copied_files_from_dest(db_path, dumpdir, log_file):
    """
    Scan the destination directory, update copied_files table and regenerate log file.
    Matches by FULL RELATIVE PATH to handle duplicate filenames in different directories.
    Provides progress output for user feedback during long scans.
    """
    tmp_log = log_file + ".tmp"
    total_files = 0
    matched_files = 0
    unmatched_files = 0
    
    print("Building path-to-ID lookup from database (this enables accurate matching)...")
    
    # Build a lookup of reconstructed_path -> file_id for accurate matching
    # This handles duplicate filenames in different directories correctly
    path_to_file_id = {}
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        c = conn.cursor()
        c.execute("SELECT id, name, parentID, contentID FROM files")
        all_files = c.fetchall()
    
    # Build temporary fileDIC for path reconstruction
    temp_fileDIC = {
        row[0]: {"Name": row[1], "Parent": row[2], "contentID": row[3]}
        for row in all_files
    }
    
    def reconstruct_path(file_id):
        """Reconstruct the relative path for a file_id."""
        parts = []
        current_id = file_id
        while current_id is not None:
            entry = temp_fileDIC.get(current_id)
            if not entry:
                break
            parts.append(entry["Name"])
            current_id = entry["Parent"]
        return "/".join(reversed(parts)) if parts else None
    
    # Find root directory names to strip (same logic as copy function's skipnames)
    root_dirs_to_strip = []
    for fid, meta in temp_fileDIC.items():
        name = meta.get("Name", "")
        if 'auth' in name and '|' in name:
            root_dirs_to_strip.append(name)
            break  # Only need the first one
    
    print(f"Building path lookup for {len(temp_fileDIC)} files...")
    if root_dirs_to_strip:
        print(f"  (stripping root dir prefix: {root_dirs_to_strip[0][:30]}...)")
    
    for file_id, meta in temp_fileDIC.items():
        # Skip directories (they have no contentID or are marked as dirs)
        if not meta.get("contentID"):
            continue
        rel_path = reconstruct_path(file_id)
        if rel_path:
            # Normalize path separators
            rel_path = rel_path.replace("\\", "/")
            # Strip root directory names (same transformation as copy function)
            for root_dir in root_dirs_to_strip:
                rel_path = rel_path.replace(root_dir + "/", "")
                rel_path = rel_path.replace(root_dir, "")
            rel_path = rel_path.lstrip("/")
            if rel_path:  # Only add if path is not empty after stripping
                path_to_file_id[rel_path] = file_id
    print(f"Built lookup with {len(path_to_file_id)} file paths")
    
    print("Scanning destination directory and regenerating log file...")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        c = conn.cursor()
        with open(tmp_log, 'w') as f:
            for root, dirs, files in os.walk(dumpdir):
                for file in files:
                    file_path = os.path.join(root, file)
                    f.write(file_path + '\n')
                    
                    # Compute relative path from dumpdir
                    rel_path = os.path.relpath(file_path, dumpdir)
                    rel_path = rel_path.replace("\\", "/").lstrip("/")
                    
                    # Try to match by full relative path (most accurate)
                    file_id = path_to_file_id.get(rel_path)
                    
                    # If not found, try with pipes (in case --sanitize-pipes was used)
                    if not file_id and "-" in rel_path:
                        rel_path_with_pipes = rel_path.replace("-", "|")
                        file_id = path_to_file_id.get(rel_path_with_pipes)
                    
                    if file_id:
                        # Store the relative path in filename column (matches what the copy function logs)
                        c.execute('INSERT OR IGNORE INTO copied_files (file_id, filename) VALUES (?, ?)', 
                                  (str(file_id), str(rel_path)))
                        if c.rowcount == 1:
                            matched_files += 1
                            if matched_files <= 10 or matched_files % 1000 == 0:
                                print(f"  [MATCH] {rel_path} -> file_id={file_id}")
                    else:
                        unmatched_files += 1
                        if unmatched_files <= 10:
                            print(f"  [NO MATCH] {rel_path} (not in source DB - may be manually added)")
                    
                    total_files += 1
                    if total_files % 10000 == 0:
                        print(f"  Progress: {total_files} files scanned, {matched_files} matched, {unmatched_files} unmatched...")
                        conn.commit()
        
        print(f"\nFinished scanning destination directory.")
        print(f"  Total files scanned: {total_files}")
        print(f"  Matched to source DB: {matched_files}")
        print(f"  Not in source DB: {unmatched_files} (these are ignored - may be manually added files)")
        conn.commit()
    os.replace(tmp_log, log_file)
    
    # Free memory from temporary dictionaries used only for log regeneration
    del temp_fileDIC
    del path_to_file_id
    del all_files
    import gc
    gc.collect()
    print("  (freed temporary path lookup memory)")

def findNextParent(fileID):
    """
    Finds the next parent directory from the data dictionary.
    Args: fileID (str): The ID of the file to find the next parent for.
    Returns: str: The ID of the next parent db item in the chain.
    """
    # O(1) direct lookup instead of O(n) loop
    entry = fileDIC.get(fileID)
    return entry['Parent'] if entry else None
        
def hasAnotherParent(fileID):
    """
    Checks if the data dictionary item has another parent.
    Args: fileID (str): The ID of the file to check.
    Returns: bool: True if the file has another parent, False otherwise.
    """
    if fileDIC[fileID]['Parent'] != None:
        return True
    else:
        return False
    
def findTree(fileID, name, parent):
    """
    Finds the original file path for a given file ID, name, and parent.
    Parameters:
        fileID (int): The ID of the file.
        name (str): The name of the file.
        parent (str): The parent directory of the file.
    Returns: str: The original file path.
    """
    path = fileDIC[parent]['Name'] + "/" + name
    current_parent = parent
    while current_parent is not None and hasAnotherParent(current_parent):
        current_parent = findNextParent(current_parent)
        path = fileDIC[current_parent]['Name'] + '/' + path
    return path

def idToPath2(fileID):
    """
    Converts a file ID into its original path by traversing the fileDIC dictionary and reconstructing the path.
    Args: fileID (int): The ID of the file.
    Returns: str: The original path of the file.
    """
    value = fileDIC[fileID]
    if value['Parent'] != None:
        path = findTree(fileID, value['Name'], value['Parent'])
    else:
        path = fileDIC[fileID]['Name']
    return path

# Reverse lookup dictionaries for O(1) filename->ID mapping (built after fileDIC is populated)
_contentID_to_fileID = {}
_name_to_fileID = {}

def build_reverse_lookups():
    """
    Build reverse lookup dictionaries for O(1) filename->ID mapping.
    Must be called after fileDIC is populated.
    """
    global _contentID_to_fileID, _name_to_fileID
    _contentID_to_fileID = {}
    _name_to_fileID = {}
    for file_id, meta in fileDIC.items():
        cid = meta.get('contentID')
        name = meta.get('Name')
        if cid:
            _contentID_to_fileID[cid] = file_id
        if name:
            _name_to_fileID[name] = file_id
    logging.info(f"Built reverse lookups: {len(_contentID_to_fileID)} contentIDs, {len(_name_to_fileID)} names")

def filenameToID(filename):
    """
    Return the DB id for a given filesystem filename.
    Tries contentID first, then falls back to Name (for DBs where contentID != filename).
    Uses O(1) lookup via pre-built reverse dictionaries.
    Parameters: filename (str): The name of the file to search for.
    Returns: str or None: The key corresponding to the filename if found, or None if not found.
    """
    # O(1) lookup instead of O(n) loop
    file_id = _contentID_to_fileID.get(filename)
    if file_id is not None:
        return str(file_id)
    file_id = _name_to_fileID.get(filename)
    if file_id is not None:
        return str(file_id)
    return None

def resolve_src_path(base_dir, cid):
    """
    Return the most likely source path for a contentID, trying flat and
    first-character subdir layouts common on MyCloud dumps.
    """
    candidates = []
    if cid:
        candidates.append(os.path.join(base_dir, cid))
        if len(cid) > 0:
            candidates.append(os.path.join(base_dir, cid[0], cid))
    else:
        candidates.append(base_dir)
    for cand in candidates:
        if os.path.exists(cand):
            return cand
    return candidates[0]

def getRootDirs():
    """
    Returns the name of the root directory that contains the 'auth' folder with a '|' character in its name.
    Returns: str: The name of the root directory.
    """
    #quick function to find annoying "auth folder" name for filtering purposes
    for keys,values in fileDIC.items():
        if 'auth' in values['Name'] and '|' in values['Name']:
            return str(values['Name'])
        
def copy_file(root, file, skipnames, dumpdir, dry_run, log_file, disk_semaphore=None, io_buffer_size=0, db_path=None):
    # Use provided db_path or fall back to global db
    _db = db_path if db_path else globals().get('db')
    filename = str(file)
    print('FOUND FILE ' + filename + ' SEARCHING......', end="\n")
    print('Processing ' + str(processed_files_counter.value) + ' of ' + str(total_files) + ' files', end="\n")
    fileID = filenameToID(str(file))
    fullpath = None
    if fileID is not None:
        fullpath = idToPath2(fileID)
    if fullpath is not None:
        newpath = None
        for paths in skipnames:
            newpath = fullpath.replace(paths, '')
        if newpath is not None:
            newpath = os.path.join(dumpdir, newpath.lstrip(os.sep))
            if args.sanitize_pipes:
                newpath = newpath.replace("|", "-")
        fullpath = str(os.path.join(root, file))

        if newpath in copied_files:
            print('File ' + fullpath + ' exists in ' + log_file + ', thus copied in a previous run to avoid duplication, skipping')
            logging.info(f'File {fullpath} exists in {log_file}, thus copied in a previous run to avoid duplication, skipping')
            with skipped_files_counter.get_lock():
                skipped_files_counter.value += 1
            with processed_files_counter.get_lock():
                processed_files_counter.value += 1
            progress = (processed_files_counter.value / total_files) * 100
            print(f'Progress: {progress:.2f}%')
            return
        elif os.path.exists(newpath):
            if args.refresh_mtime_existing and args.preserve_mtime:
                meta = fileDIC.get(fileID, {})
                ts = next(
                    (
                        t
                        for t in [
                            meta.get("imageDate"),
                            meta.get("videoDate"),
                            meta.get("cTime"),
                            meta.get("birthTime"),
                        ]
                        if isinstance(t, (int, float)) and t is not None
                    ),
                    None,
                )
                if ts:
                    os.utime(newpath, (ts / 1000, ts / 1000))
            with skipped_files_counter.get_lock():
                skipped_files_counter.value += 1
            with processed_files_counter.get_lock():
                processed_files_counter.value += 1
            progress = (processed_files_counter.value / total_files) * 100
            print(f'Progress: {progress:.2f}%')
            return
        else:
            if dry_run:
                print('Dry run: Skipping copying ' + fullpath + ' to ' + newpath)
                with processed_files_counter.get_lock():
                    processed_files_counter.value += 1
                progress = (processed_files_counter.value / total_files) * 100
                with copied_files_counter.get_lock():
                    copied_files_counter.value += 1
                print(f'Progress: {progress:.2f}%')
                return
            else:
                print('Copying ' + newpath)
                try:
                    os.makedirs(os.path.dirname(newpath), exist_ok=True)
                    # Keep two FDs per copy (src/dest) to stay under limits.
                    if io_buffer_size and io_buffer_size > 0:
                        with open(fullpath, "rb") as fsrc, open(newpath, "wb") as fdst:
                            while True:
                                buf = fsrc.read(io_buffer_size)
                                if not buf:
                                    break
                                fdst.write(buf)
                    else:
                        shutil.copy2(fullpath, newpath)
                    if args.preserve_mtime:
                        meta = fileDIC.get(fileID, {})
                        ts = next(
                            (
                                t
                                for t in [
                                    meta.get("imageDate"),
                                    meta.get("videoDate"),
                                    meta.get("cTime"),
                                    meta.get("birthTime"),
                                ]
                                if isinstance(t, (int, float)) and t is not None
                            ),
                            None,
                        )
                        if ts:
                            os.utime(newpath, (ts / 1000, ts / 1000))
                            if _db:
                                try:
                                    def _op():
                                        with sqlite3.connect(_db) as conn:
                                            conn.execute("PRAGMA busy_timeout=5000")
                                            cur = conn.cursor()
                                            cur.execute("UPDATE copied_files SET mtime_refreshed=1 WHERE file_id=?", (fileID,))
                                            conn.commit()
                                    with_retry_db(_op)
                                except sqlite3.Error:
                                    pass
                    with processed_files_counter.get_lock():
                        processed_files_counter.value += 1
                    progress = (processed_files_counter.value / total_files) * 100
                    with copied_files_counter.get_lock():
                        copied_files_counter.value += 1
                    print(f'Progress: {progress:.2f}%')
                    with lock:
                        with open(log_file, 'a') as f:
                            f.write(newpath + '\n')  # Write destination path to match what we check
                    # Record in database for resume capability (store content_id for consistency)
                    if _db:
                        content_id = fileDIC.get(fileID, {}).get("contentID", filename)
                        insert_copied_file(_db, fileID, content_id)
                except Exception as e:
                    print(f'Error copying file {fullpath} to {newpath}: {e}')
                    logging.error(f'Error copying file {fullpath} to {newpath}: {e}')
                    # Record as skipped so we don't retry forever on permanent errors (use content_id for consistency)
                    if _db:
                        content_id = fileDIC.get(fileID, {}).get("contentID", filename)
                        insert_skipped_file(_db, content_id, f'copy_error: {type(e).__name__}')
    else:
        print(f'Warning: Unable to find file {filename} in the database')
        logging.warning(f'Unable to find file {filename} in the database')
        if _db:
            insert_skipped_file(_db, filename, 'not_in_database')
        with processed_files_counter.get_lock():
            processed_files_counter.value += 1
        progress = (processed_files_counter.value / total_files) * 100
        with skipped_files_counter.get_lock():
            skipped_files_counter.value += 1
        print(f'Progress: {progress:.2f}%')
                    
def create_log_file_from_dir(root_dir, log_file):
    """
    Regenerate the log file by scanning the destination directory and writing all found files to the log.
    Args:
        root_dir (str): The directory to scan (usually the destination/dumpdir).
        log_file (str): The path to the log file to be created.
    """
    tmp_log = log_file + ".tmp"
    with open(tmp_log, 'w') as f:
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                file_path = os.path.join(root, file)
                f.write(file_path + '\n')
    os.replace(tmp_log, log_file)

def get_dir_size(start_path='.'):
    """
    Calculate the total size of a directory and its subdirectories.
    Args: start_path (str): The path of the directory to calculate the size of. Defaults to the current directory.
    Returns: int: The total size of the directory in bytes.
    """
    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            # skip if it is symbolic link
            if not os.path.islink(filepath):
                try:
                    file_stat = os.stat(filepath)
                    total_size += file_stat.st_size
                except OSError:
                    pass

    return total_size

def with_retry_db(fn, attempts=5, delay=0.1):
    """
    Retry wrapper for sqlite ops to reduce 'database is locked' errors under concurrency.
    """
    last_err = None
    for _ in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                last_err = e
                time.sleep(delay)
                continue
            raise
    if last_err:
        raise last_err

def count_files(start_path='.'):
    """Count files recursively under start_path."""
    total = 0
    for _, _, filenames in os.walk(start_path):
        total += len(filenames)
    return total

def get_directory_summary(path):
    """Return (file_count, total_size) for all files under path."""
    total_size = 0
    file_count = 0
    
    for root, _, files in os.walk(path):
        for f in files:
            try:
                fp = os.path.join(root, f)
                if os.path.islink(fp):
                    continue
                total_size += os.path.getsize(fp)
                file_count += 1
            except (OSError, FileNotFoundError):
                continue
                
    return file_count, total_size

def format_size(size_bytes):
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def log_summary(message, to_console=True, to_file=True):
    """Log a message to both console and log file with timestamp."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    
    if to_console:
        print(log_message)
    if to_file:
        with open(log_filename, 'a') as f:
            f.write(log_message + '\n')

def show_summary(db_path, source_dir, dest_dir, phase="INITIAL"):
    """
    Show and log summary of source and destination directories.
    
    Args:
        db_path: Path to the SQLite database
        source_dir: Source directory path
        dest_dir: Destination directory path
        phase: Phase of operation (INITIAL, FINAL, etc.)
    """
    # Create a buffer to collect all output
    output = []
    
    def add_line(text=""):
        output.append(text)
    
    # Header
    add_line("\n" + "="*70)
    add_line(f"{' ' * 25}COPY OPERATION SUMMARY - {phase}")
    add_line("="*70)
    
    # Timestamp
    add_line(f"Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Source stats
    add_line("\nSOURCE DIRECTORY:")
    add_line(f"  Path: {source_dir}")
    
    try:
        # Get total files from database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM Files")
        db_file_count = cursor.fetchone()[0]
        add_line(f"  Files in database: {db_file_count:,}")
        
        # Get actual source files count and size
        add_line("  Scanning source directory (this may take a while)...")
        src_count, src_size = get_directory_summary(source_dir)
        add_line(f"  Files found: {src_count:,}")
        add_line(f"  Total size: {format_size(src_size)}")
        
        # Get destination stats
        add_line("\nDESTINATION DIRECTORY:")
        add_line(f"  Path: {dest_dir}")
        
        if os.path.exists(dest_dir):
            add_line("  Scanning destination directory (this may take a while)...")
            dest_count, dest_size = get_directory_summary(dest_dir)
            add_line(f"  Existing files: {dest_count:,}")
            add_line(f"  Existing size: {format_size(dest_size)}")
            
            # Calculate remaining
            remaining_count = max(0, db_file_count - dest_count)
            remaining_pct = (remaining_count / db_file_count * 100) if db_file_count > 0 else 0
            add_line(f"\nREMAINING:")
            add_line(f"  Files to copy: {remaining_count:,} ({remaining_pct:.1f}% of total)")
            
            # Calculate estimated time remaining if this is a final summary
            if phase.upper() == "FINAL" and 'start_time' in globals():
                elapsed = time.time() - start_time
                if elapsed > 0 and dest_count > 0:
                    files_per_sec = dest_count / elapsed
                    if files_per_sec > 0:
                        est_remaining = remaining_count / files_per_sec
                        add_line(f"  Estimated time remaining: {format_duration(est_remaining)}")
        else:
            add_line("  Destination does not exist or is not accessible")
            
    except Exception as e:
        add_line(f"  Error generating summary: {str(e)}")
        
    finally:
        if 'conn' in locals():
            conn.close()
    
    # Add footer
    add_line("="*70 + "\n")
    
    # Write all output to log file and console
    for line in output:
        log_summary(line, to_console=True, to_file=True)
    
    # Return the collected output in case it's needed programmatically
    return output

def setup_logging():
    """Set up logging configuration."""
    # Create log directory if it doesn't exist
    log_dir = os.path.dirname(os.path.abspath(log_filename))
    os.makedirs(log_dir, exist_ok=True)
    
    # Clear existing log file
    open(log_filename, 'w').close()
    
    # Log startup information
    log_summary(f"Starting MyCloud Recovery Tool")
    log_summary(f"Command: {' '.join(sys.argv)}")
    log_summary(f"Python: {sys.version}")
    log_summary(f"Platform: {sys.platform}")
    log_summary(f"Working directory: {os.getcwd()}")
    log_summary("-" * 70)

if __name__ == "__main__":
    # Parse arguments first
    parser = argparse.ArgumentParser(description="WD MyCloud REST SDK Recovery Tool")
    parser.add_argument("--preflight", action="store_true", help="Run pre-flight hardware/file check and print recommendations")
    parser.add_argument("--dry_run", action="store_true", default=False, help="Perform a dry run")
    parser.add_argument("--db", help="Path to the file DB")
    parser.add_argument("--filedir", help="Path to the files directory")
    parser.add_argument("--dumpdir", help="Path to the directory to dump files")
    parser.add_argument("--log_file", help="Path to the log file used to track successfully copied files to avoid duplication in future runs")
    parser.add_argument("--create_log", action="store_true", default=False, help="Create a log file from an existing run where logging was not in place")
    parser.add_argument("--resume", action="store_true", help="Resume a previous run, regenerating the log from the destination before resuming (default)")
    parser.add_argument("--regen-log", dest="regen_log", action="store_true", help="Regenerate the log file from the destination directory only, then exit")
    parser.add_argument("--no-regen-log", dest="no_regen_log", action="store_true", help="When used with --resume, skip regenerating the log and use the existing log file as-is (advanced)")
    parser.add_argument("--thread-count", type=int, help="Number of threads to use")
    parser.add_argument("--log_level", type=str, choices=["DEBUG", "INFO", "WARNING"], default="INFO", help="Set the logging level (DEBUG, INFO, WARNING). Default is INFO.")
    parser.add_argument(
        "--preserve-mtime",
        action=BooleanOptionalAction,
        default=True,
        help="After copy, set destination mtime to the best available timestamp from the DB (imageDate, videoDate, cTime). Enabled by default; use --no-preserve-mtime to disable.",
    )
    parser.add_argument(
        "--refresh-mtime-existing",
        action="store_true",
        help="If destination file already exists, refresh its mtime from DB timestamps without recopying.",
    )
    parser.add_argument(
        "--io-buffer-size",
        type=int,
        default=0,
        help="Optional I/O buffer size in bytes for file copies (default: use shutil.copy2 defaults).",
    )
    parser.add_argument(
        "--io-max-concurrency",
        type=int,
        default=0,
        help="Optional max concurrent disk operations (semaphore). 0 disables limiting.",
    )
    parser.add_argument(
        "--sanitize-pipes",
        action="store_true",
        help="Replace '|' with '-' in destination paths; useful for Windows/NTFS/FAT/SMB targets that disallow pipe characters.",
    )
    parser.add_argument(
        "--low-memory",
        action="store_true",
        help="Reduce memory usage by skipping timestamp fields in file dictionary. Disables --preserve-mtime. Recommended for systems with <16GB RAM.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only the first N files (for testing). 0 = no limit (default).",
    )
    args = parser.parse_args()
    
    # Initialize start time and logging
    start_time = time.time()
    setup_logging()
    log_summary(f"Start time: {time.ctime(start_time)}")
    
    def format_duration(seconds):
        mins, secs = divmod(int(seconds), 60)
        hrs, mins = divmod(mins, 60)
        days, hrs = divmod(hrs, 24)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hrs:
            parts.append(f"{hrs}h")
        if mins:
            parts.append(f"{mins}m")
        parts.append(f"{secs}s")
        return " ".join(parts)
    
    run_start = start_time
    regen_elapsed = 0.0
    copy_phase_start = run_start
    
    # Low-memory mode disables preserve-mtime
    if args.low_memory:
        args.preserve_mtime = False
        args.refresh_mtime_existing = False

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    if args.db:
        init_copy_tracking_tables(args.db)

    if args.preflight:
        if not args.filedir or not args.dumpdir:
            print("\n❗ Please provide both --filedir (source) and --dumpdir (destination) for pre-flight check.\n")
            print_help()
            sys.exit(1)
        if args.db:
            try:
                init_copy_tracking_tables(args.db)
                print(f"✅ Verified copy tracking tables in {args.db}")
            except sqlite3.Error as e:
                print(f"⚠️  Could not verify copy tracking tables in {args.db}: {e}")
        summary = preflight_summary(args.filedir, args.dumpdir)
        print_preflight_report(summary, args.filedir, args.dumpdir)
        if psutil:
            dest_fs, dest_mp = detect_fs_type(args.dumpdir)
            if is_pipe_sensitive_fs(dest_fs):
                print(f"\n⚠️  Destination filesystem appears to be {dest_fs} ({dest_mp}); it may reject '|' in filenames.")
                print("    Consider adding --sanitize-pipes if you see errors copying to this destination.")
        sys.exit(0)

    if args.regen_log or args.create_log:
        if not args.dumpdir or not args.log_file or not args.db:
            print("\n❗ Please provide --dumpdir, --log_file, and --db for log regeneration.\n")
            sys.exit(1)
        print(f"Regenerating log file {args.log_file} from destination {args.dumpdir} and updating copied_files table...")
        regen_start = time.time()
        regenerate_copied_files_from_dest(args.db, args.dumpdir, args.log_file)
        regen_elapsed = time.time() - regen_start
        print(f"Log file and copied_files table regeneration complete. Duration: {format_duration(regen_elapsed)}")
        sys.exit(0)

    if not args.db or not args.filedir or not args.dumpdir or not args.log_file:
        print("\n❗ Missing required arguments. Please provide --db, --filedir, --dumpdir, and --log_file.\n")
        sys.exit(1)

    # Expose globals for helper functions
    db = args.db
    filedir = args.filedir
    dumpdir = args.dumpdir
    dry_run = args.dry_run
    log_file = args.log_file
    # Default to 4 threads to reduce memory pressure (was cpu_count)
    thread_count = args.thread_count if args.thread_count else min(4, os.cpu_count() or 4)

    lock = Lock()

    print("\n===== RUN STARTING =====")
    print(f"Start time: {time.ctime(run_start)}")
    print(f"DB: {db}")
    print(f"Source: {filedir}")
    print(f"Destination: {dumpdir}")
    print(f"Log file: {log_file}")
    print(f"Thread count: {thread_count}")
    print(f"Dry run: {dry_run}")
    print(f"Preserve mtime: {args.preserve_mtime}")
    print(f"Refresh mtime on existing: {args.refresh_mtime_existing}")
    print(f"Sanitize pipes: {args.sanitize_pipes}")
    if args.resume:
        will_regen = "Yes (default)" if not args.no_regen_log else "No (--no-regen-log)"
        print(f"Resume mode: ON, Will regenerate log: {will_regen}")
    elif args.regen_log:
        print(f"Regen-log-only mode: ON (will regenerate and exit)")
    else:
        print(f"Normal copy mode")
    print("========================\n")
    logging.info(f"Run starting at {time.ctime(run_start)} with db={db}, filedir={filedir}, dumpdir={dumpdir}, log_file={log_file}, threads={thread_count}, dry_run={dry_run}, preserve_mtime={args.preserve_mtime}, refresh_mtime_existing={args.refresh_mtime_existing}, sanitize_pipes={args.sanitize_pipes}, resume={args.resume}, regen_log={args.regen_log}, no_regen_log={args.no_regen_log}")

    filedir_size = get_dir_size(filedir) / (1024 * 1024 * 1024)
    print(f"The size of the directory {filedir} is {filedir_size:.2f} GB")
    logging.info(f"The size of the directory {filedir} is {filedir_size:.2f} GB")

    try:
        with sqlite3.connect(db) as con:
            con.execute("PRAGMA busy_timeout=5000")
            cur = con.cursor()
            cur.execute("SELECT id, name, parentID, contentID, imageDate, videoDate, cTime, birthTime FROM files")
            files = cur.fetchall()
            num_db_rows = len(files)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_contentID ON files (contentID)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_parentID ON files (parentID)")
            con.commit()
    except sqlite3.Error:
        print(f"Error opening database at {db}")
        logging.exception("Error opening database")
        sys.exit(1)

    # Build file dictionary - skip timestamp fields in low-memory mode to save ~40% RAM
    if args.low_memory:
        print("🔋 Low-memory mode: skipping timestamp fields to reduce RAM usage")
        fileDIC = {
            file[0]: {
                "Name": file[1],
                "Parent": file[2],
                "contentID": file[3],
            }
            for file in files
        }
    else:
        fileDIC = {
            file[0]: {
                "Name": file[1],
                "Parent": file[2],
                "contentID": file[3],
                "imageDate": file[4],
                "videoDate": file[5],
                "cTime": file[6],
                "birthTime": file[7],
            }
            for file in files
        }
    
    # Free the raw DB results now that fileDIC is built
    del files
    import gc
    gc.collect()
    print(f"  (built fileDIC with {len(fileDIC)} entries, freed raw DB results)")

    # Build reverse lookup dictionaries for O(1) filename->ID mapping
    build_reverse_lookups()

    # Warn early if destination filesystem likely rejects pipe characters
    pipe_in_db = any("|" in meta["Name"] for meta in fileDIC.values())
    dest_fs, dest_mp = detect_fs_type(dumpdir) if psutil else (None, None)
    if pipe_in_db and is_pipe_sensitive_fs(dest_fs) and not args.sanitize_pipes:
        msg = f"Destination filesystem appears to be {dest_fs} ({dest_mp}); it may reject '|' in filenames. Consider --sanitize-pipes."
        print(f"⚠️  {msg}")
        logging.warning(msg)
    elif pipe_in_db and args.sanitize_pipes and dest_fs:
        logging.info(f"Sanitizing '|' to '-' for destination filesystem type {dest_fs} ({dest_mp}).")

    skipnames = [filedir]
    root_dir_name = getRootDirs()
    if root_dir_name:
        skipnames.append(root_dir_name)

    total_files = sum([len(files) for _, _, files in os.walk(filedir)])
    processed_files_counter = Value("i", 0)
    copied_files_counter = Value("i", 0)
    skipped_files_counter = Value("i", 0)

    copied_files = set()
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            copied_files = set(f.read().splitlines())

    logging.info(f"Parameters: db={db}, filedir={filedir}, dumpdir={dumpdir}, dry_run={dry_run}, log_file={log_file}, create_log={args.create_log}, resume={args.resume}, thread_count={thread_count}")

    def run_standard_copy():
        copy_phase_start = time.time()
        print(f"There are {total_files} files to copy from {filedir} to {dumpdir}")
        logging.info(f"There are {total_files} files to copy from {filedir} to {dumpdir}")
        print(f"There are {num_db_rows} rows in the database to process")
        logging.info(f"There are {num_db_rows} rows in the database to process")
        print(f"The size of file data dictionary is {len(fileDIC)} elements")
        logging.info(f"The size of file data dictionary is {len(fileDIC)} elements")
        print(f"The number of threads used in this run is {thread_count}")
        logging.info(f"The number of threads used in this run is {thread_count}")
        perf_hint = max(1000, min(num_db_rows // 50, 50000))  # ~2% capped at 50k
        print(f"Tip: to run the perf sanity test, set PERF_TEST_ROWS={perf_hint} and run pytest tests/test_perf_regen_log.py")
        logging.info(f"Perf hint suggested rows: {perf_hint}")

        last_logged_percent = -1

        def log_progress():
            copied = copied_files_counter.value
            skipped = skipped_files_counter.value
            processed = processed_files_counter.value
            percent = int((processed / total_files) * 100) if total_files else 100
            msg = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Progress: Copied={copied} Skipped={skipped} Total={total_files} Percent={percent}%"
            print(msg)
            logging.info(msg)

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            for root, dirs, files in os.walk(filedir):
                for file in files:
                    # Check limit if set
                    if args.limit > 0 and processed_files_counter.value >= args.limit:
                        print(f"\n⚠️  Reached --limit of {args.limit} files. Stopping...")
                        logging.info(f"Reached --limit of {args.limit} files. Stopping...")
                        return  # Exit early
                    
                    executor.submit(copy_file, root, file, skipnames, dumpdir, dry_run, log_file)
                    processed = processed_files_counter.value
                    percent = int((processed / total_files) * 100) if total_files else 100
                    if percent > last_logged_percent:
                        last_logged_percent = percent
                        log_progress()

        dumpdir_size = get_dir_size(dumpdir) / (1024 * 1024 * 1024)
        print(f"The size of the source directory {filedir} is {filedir_size:.2f} GB")
        print(f"The size of the destination directory {dumpdir} is {dumpdir_size:.2f} GB")
        print(f"There are {total_files} files to copy from {filedir} to {dumpdir}")
        print(f"There are {num_db_rows} rows in the database to process")
        print(f"The size of file data dictionary is {len(fileDIC)} elements")
        print(f"There are {len(copied_files)} files copied on previous runs of this script, pulled from {log_file}")
        if dry_run:
            print(f"Dry run - No files were actually copied: Total files that would have been copied: {copied_files_counter.value}")
        else:
            print(f"Total files copied: {copied_files_counter.value}")
        print(f"Total files skipped: {skipped_files_counter.value}")
        print(f"Total files in the source directory: {total_files}")
        print(f"Total files in the destination directory: {len(os.listdir(dumpdir))}")
        print(f"The number of threads used in this run is {thread_count}")

        print("\nReconciliation Summary:")
        dest_count = count_files(dumpdir)
        summary_data = [
            ("The size of the source directory", f"{filedir_size:.2f} GB"),
            ("The size of the destination directory", f"{dumpdir_size:.2f} GB"),
            ("Total files to copy", total_files),
            ("Rows in the database to process", num_db_rows),
            ("The size of file data dictionary", len(fileDIC)),
            ("Files copied on previous runs", len(copied_files)),
            ("Total files copied", copied_files_counter.value),
            ("Total files skipped", skipped_files_counter.value),
            ("Total files in the source directory", total_files),
            ("Total files in the destination directory (recursive)", dest_count),
        ]
        for label, value in summary_data:
            print(f"{label}: {value}")
            logging.info(f"{label}: {value}")

        elapsed = time.time() - run_start
        copy_elapsed = time.time() - copy_phase_start
        elapsed_str = format_duration(elapsed)
        copy_elapsed_str = format_duration(copy_elapsed)
        files_per_sec = processed_files_counter.value / elapsed if elapsed > 0 else 0
        print(f"Elapsed time: {elapsed_str} ({files_per_sec:.2f} files/sec)")
        logging.info(f"Elapsed time: {elapsed_str} ({files_per_sec:.2f} files/sec)")
        print(f"Copy phase duration: {copy_elapsed_str}")
        logging.info(f"Copy phase duration: {copy_elapsed_str}")
        print(f"Started at: {time.ctime(run_start)}")
        print(f"Finished at: {time.ctime(run_start + elapsed)}")
        logging.info(f"Started at: {time.ctime(run_start)}")
        logging.info(f"Finished at: {time.ctime(run_start + elapsed)}")

        if processed_files_counter.value != total_files:
            print("Warning: Not all files were processed. Check for errors or incomplete runs.")
            logging.warning("Not all files were processed. Check for errors or incomplete runs.")
        else:
            print("All files have been processed successfully.")
            logging.info("All files have been processed successfully.")

        if os.path.exists(log_file):
            print(f"Resuming from previous run. Log file found: {log_file}")
            logging.info(f"Resuming from previous run. Log file found: {log_file}")
        else:
            print("Starting a new run. No log file found.")
            logging.info("Starting a new run. No log file found.")

    def run_resume_copy():
        regen_elapsed = 0.0  # Initialize to avoid UnboundLocalError when --no-regen-log
        if not args.no_regen_log:
            print(f"Regenerating log file {log_file} from destination {dumpdir} before resuming and updating copied_files table...")
            regen_start = time.time()
            regenerate_copied_files_from_dest(db, dumpdir, log_file)
            regen_elapsed = time.time() - regen_start
            copy_phase_start = time.time()
            print(f"Log file and copied_files table regeneration complete in {format_duration(regen_elapsed)}. Resuming copy process...")
        else:
            print("Skipping log regeneration (using existing log file as-is). Resuming copy process...")
            copy_phase_start = time.time()

        with sqlite3.connect(db) as conn:
            conn.execute("PRAGMA busy_timeout=5000")
            c = conn.cursor()
            c.execute(
                """SELECT f.id, f.contentID, f.name, f.imageDate, f.videoDate, f.cTime, f.birthTime, f.mimeType FROM files f
                   LEFT JOIN copied_files c2 ON f.id = c2.file_id
                   LEFT JOIN skipped_files s ON f.contentID = s.filename
                   WHERE c2.file_id IS NULL AND s.filename IS NULL"""
            )
            files_to_copy = c.fetchall()
            # Load content_ids from copied_files for fast lookup
            c.execute("SELECT filename FROM copied_files")
            already_copied_set = set(row[0] for row in c.fetchall())
            # Load skipped content_ids  
            c.execute("SELECT filename FROM skipped_files")
            skipped_set = set(row[0] for row in c.fetchall())

        print(f"Files to process: {len(files_to_copy)} (filtered by copied_files and skipped_files tables)")
        logging.info(f"Files to process: {len(files_to_copy)}")

        # Check and report file descriptor limit
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            print(f"File descriptor limit: soft={soft}, hard={hard}")
            if soft < 1024:
                print(f"⚠️  Warning: Low file descriptor limit ({soft}). Consider running 'ulimit -n 65535' before starting.")
        except:
            pass

        results = {"copied": 0, "skipped_already": 0, "skipped_problem": 0, "errored": 0, "dry_run": 0}
        
        # Semaphore to limit concurrent file operations and prevent FD exhaustion
        io_semaphore = threading.Semaphore(max(2, thread_count))

        def copy_worker(file_row):
            file_id, content_id, name, image_date, video_date, c_time, birth_time, mime_type = file_row
            try:
                if mime_type == "application/x.wd.dir" or content_id is None:
                    return ("skipped_problem", content_id or name)
                # Note: SQL query already filters by file_id, these are safety checks
                if content_id and content_id in already_copied_set:
                    return ("skipped_already", content_id)
                if content_id and content_id in skipped_set:
                    return ("skipped_problem", content_id)
                if dry_run:
                    print(f"[DRY RUN] Would copy: {content_id}")
                    logging.info(f"[DRY RUN] Would copy: {content_id}")
                    return ("dry_run", content_id)
                rel_path = idToPath2(file_id)
                for skip in skipnames:
                    rel_path = rel_path.replace(skip, "")
                if args.sanitize_pipes:
                    rel_path = rel_path.replace("|", "-")
                # Strip leading slashes so os.path.join works correctly
                rel_path = rel_path.lstrip(os.sep)
                dest_path = os.path.join(dumpdir, rel_path)
                src_path = resolve_src_path(filedir, content_id)
                if not os.path.exists(src_path):
                    msg = f"Source missing: {content_id} resolved to {src_path}"
                    print(f"[ERROR] {msg}")
                    logging.error(msg)
                    insert_skipped_file(db, content_id, "source_missing")
                    return ("skipped_problem", content_id)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                if os.path.exists(dest_path) and args.refresh_mtime_existing:
                    if args.preserve_mtime:
                        ts = next(
                            (
                                t
                                for t in [image_date, video_date, c_time, birth_time]
                                if isinstance(t, (int, float)) and t is not None
                            ),
                            None,
                        )
                        if ts:
                            os.utime(dest_path, (ts / 1000, ts / 1000))
                            try:
                                with _db_lock:
                                    conn = get_thread_db_connection(db)
                                    cur = conn.cursor()
                                    cur.execute("UPDATE copied_files SET mtime_refreshed=1 WHERE file_id=?", (file_id,))
                                    conn.commit()
                            except sqlite3.Error:
                                pass
                    # Record in copied_files since it exists at destination
                    insert_copied_file(db, file_id, content_id)
                    return ("skipped_already", content_id)
                # Use semaphore to limit concurrent file operations
                with io_semaphore:
                    shutil.copy2(src_path, dest_path)
                if args.preserve_mtime:
                    ts = next(
                        (
                            t
                            for t in [image_date, video_date, c_time, birth_time]
                            if isinstance(t, (int, float)) and t is not None
                        ),
                        None,
                    )
                    if ts:
                        os.utime(dest_path, (ts / 1000, ts / 1000))  # convert ms to seconds
                insert_copied_file(db, file_id, content_id)
                print(f"[COPIED] {rel_path}")
                logging.info(f"Copied: {rel_path}")
                return ("copied", rel_path)
            except Exception as copy_err:
                logging.error(f"Error copying {name}: {copy_err}")
                print(f"[ERROR] {name}: {copy_err}")
                # Record in skipped_files so we don't retry forever on permanent errors
                insert_skipped_file(db, content_id or name, f"copy_error: {type(copy_err).__name__}")
                return ("errored", name)

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            for status, rel in executor.map(copy_worker, files_to_copy):
                results[status] += 1

        # Free memory from large data structures no longer needed
        del files_to_copy
        del already_copied_set
        del skipped_set
        import gc
        gc.collect()

        with sqlite3.connect(db) as conn:
            conn.execute("PRAGMA busy_timeout=5000")
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM copied_files")
            copied_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM skipped_files")
            skipped_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM files")
            total_files_db = c.fetchone()[0]
        dest_count = sum(len(files) for _, _, files in os.walk(dumpdir))

        print("\n===== SUMMARY =====")
        print(f"Total files in source (files table): {total_files_db}")
        print(f"Total files copied (copied_files table): {copied_count}")
        print(f"Total files skipped (skipped_files table): {skipped_count}")
        print(f"Total files in destination directory: {dest_count}")
        print(f"Copied this run: {results['copied']}")
        print(f"Skipped (already copied): {results['skipped_already']}")
        print(f"Skipped (problem/skipped): {results['skipped_problem']}")
        print(f"Errored: {results['errored']}")
        print(f"Processed: {sum(results.values())}")
        print(f"Started at: {time.ctime(run_start)}")
        elapsed = time.time() - run_start
        copy_elapsed = time.time() - copy_phase_start if 'copy_phase_start' in locals() else elapsed
        elapsed_str = format_duration(elapsed)
        copy_elapsed_str = format_duration(copy_elapsed)
        files_per_sec = sum(results.values()) / elapsed if elapsed > 0 else 0
        print(f"Elapsed time: {elapsed_str} ({files_per_sec:.2f} files/sec)")
        print(f"Regen duration: {format_duration(regen_elapsed)}")
        print(f"Copy phase duration: {copy_elapsed_str}")

    exit_code = 0
    try:
        # Show initial summary
        show_summary(args.db, args.filedir, args.dumpdir, "INITIAL")
        
        # Run the appropriate copy operation
        if args.resume:
            log_summary("Starting RESUME operation")
            run_resume_copy()
        else:
            log_summary("Starting STANDARD copy operation")
            run_standard_copy()
            
    except KeyboardInterrupt:
        log_summary("\nOperation interrupted by user", to_console=True)
        exit_code = 130  # Standard exit code for Ctrl+C
    except Exception as e:
        error_msg = f"\nFATAL ERROR: {str(e)}\n{traceback.format_exc()}"
        log_summary(error_msg, to_console=True)
        exit_code = 1
    finally:
        # Always show final summary, even if there was an error
        try:
            log_summary("\nGenerating final summary...")
            show_summary(args.db, args.filedir, args.dumpdir, "FINAL")
            
            # Calculate total runtime
            end_time = time.time()
            total_seconds = end_time - start_time
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Log completion status
            status_msg = "COMPLETED SUCCESSFULLY" if exit_code == 0 else f"FAILED WITH CODE {exit_code}"
            log_summary("\n" + "=" * 70)
            log_summary(f"OPERATION {status_msg}")
            log_summary("=" * 70)
            log_summary(f"Start time:    {time.ctime(start_time)}")
            log_summary(f"End time:      {time.ctime(end_time)}")
            log_summary(f"Total runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            log_summary(f"Log file:      {os.path.abspath(log_filename)}")
            log_summary("=" * 70)
            
            # Cleanup resources
            log_summary("Cleaning up resources...")
            close_all_db_connections()
            gc.collect()
            
            # Signal log thread to stop
            log_queue.put("STOP")
            log_thread.join(timeout=5.0)
            
            if log_thread.is_alive():
                log_summary("Warning: Log thread did not shut down cleanly")
        
        except Exception as e:
            log_summary(f"Error during cleanup: {str(e)}", to_console=True)
            exit_code = exit_code or 1
        
    log_summary(f"Exiting with code {exit_code}")
    sys.exit(exit_code)
