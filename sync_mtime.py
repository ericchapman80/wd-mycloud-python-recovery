#!/usr/bin/env python3
"""
Lightweight mtime synchronization tool.

Updates modification times on destination files to match original timestamps
from the database (imageDate, videoDate, cTime, or birthTime). Designed for
minimal memory usage - processes files one at a time without loading the
entire database into memory.

Usage:
    # Dry run (see what would be updated)
    python sync_mtime.py --db /path/to/index.db --dest /mnt/nfs-media --dry-run
    
    # Actually update mtimes
    python sync_mtime.py --db /path/to/index.db --dest /mnt/nfs-media
    
    # Verbose output
    python sync_mtime.py --db /path/to/index.db --dest /mnt/nfs-media --verbose
    
    # Resume from specific file
    python sync_mtime.py --db /path/to/index.db --dest /mnt/nfs-media --resume-from 1000
"""

import argparse
import datetime
import os
import sqlite3
import sys
import time
from pathlib import Path


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def colorize(text, color):
    """Add color to text if terminal supports it."""
    if sys.stdout.isatty():
        return f"{color}{text}{Colors.ENDC}"
    return text


def format_timestamp(ts_ms):
    """Format millisecond timestamp to readable date."""
    if ts_ms:
        return datetime.datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
    return "N/A"


def build_file_dict(conn):
    """
    Build fileDIC dictionary from files table (like restsdk_public.py does).
    
    Returns dictionary: {file_id: {'Name': name, 'Parent': parentID}}
    """
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, parentID FROM files")
    
    file_dict = {}
    for row in cursor:
        file_id, name, parent_id = row
        file_dict[file_id] = {
            'Name': name,
            'Parent': parent_id
        }
    
    return file_dict

def find_tree(file_dict, file_id, current_name, parent_id):
    """
    Recursively build path by traversing parent chain (like findTree in restsdk_public.py).
    """
    if parent_id is None:
        return current_name
    
    parent = file_dict.get(parent_id)
    if parent is None:
        return current_name
    
    parent_path = find_tree(file_dict, parent_id, parent['Name'], parent['Parent'])
    return os.path.join(parent_path, current_name)

def id_to_path(file_dict, file_id):
    """
    Convert file ID to reconstructed path (like idToPath2 in restsdk_public.py).
    """
    value = file_dict.get(file_id)
    if value is None:
        return None
    
    if value['Parent'] is not None:
        path = find_tree(file_dict, file_id, value['Name'], value['Parent'])
    else:
        path = value['Name']
    
    return path

def get_file_info_streaming(db_path, file_dict):
    """
    Stream file information from database without loading everything into memory.
    
    Yields tuples of (file_id, relative_path, timestamp_ms) for files that have
    been copied (exist in copied_files table).
    
    Args:
        db_path: Path to SQLite database
        file_dict: Pre-built file dictionary for path reconstruction
        
    Yields:
        Tuple of (file_id, relative_path, timestamp_ms)
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=5000")
    cursor = conn.cursor()
    
    # Query to get copied files with their metadata
    query = """
        SELECT 
            cf.file_id,
            f.imageDate,
            f.videoDate,
            f.cTime,
            f.birthTime
        FROM copied_files cf
        JOIN files f ON cf.file_id = f.id
        ORDER BY CAST(cf.file_id AS INTEGER)
    """
    
    cursor.execute(query)
    
    for row in cursor:
        file_id = row[0]
        
        # Reconstruct the path using the same logic as restsdk_public.py
        relative_path = id_to_path(file_dict, file_id)
        
        if relative_path is None:
            continue
        
        # Priority: imageDate > videoDate > cTime > birthTime
        timestamp_ms = None
        for ts in [row[1], row[2], row[3], row[4]]:
            if ts is not None and isinstance(ts, (int, float)):
                timestamp_ms = ts
                break
        
        yield (file_id, relative_path, timestamp_ms)
    
    conn.close()


def update_mtime(file_path, timestamp_ms, dry_run=False):
    """
    Update file's modification time.
    
    Args:
        file_path: Path to file
        timestamp_ms: Timestamp in milliseconds
        dry_run: If True, don't actually update
        
    Returns:
        Tuple of (success, old_mtime, new_mtime, error_message)
    """
    try:
        old_mtime = os.path.getmtime(file_path)
        new_mtime = timestamp_ms / 1000  # Convert ms to seconds
        
        if not dry_run:
            os.utime(file_path, (new_mtime, new_mtime))
        
        return (True, old_mtime, new_mtime, None)
    except FileNotFoundError:
        return (False, None, None, "File not found")
    except PermissionError:
        return (False, None, None, "Permission denied")
    except Exception as e:
        return (False, None, None, str(e))


def get_root_auth_dir(file_dict):
    """
    Find the root 'auth' directory name (contains 'auth' and '|' character).
    Like getRootDirs() in restsdk_public.py.
    """
    for file_id, value in file_dict.items():
        name = value.get('Name', '')
        if 'auth' in name and '|' in name:
            return name
    return None

def sync_mtimes(db_path, dest_dir, dry_run=False, verbose=False, resume_from=0, log_file=None, sanitize_pipes=False):
    """
    Synchronize modification times for all copied files.
    
    Args:
        db_path: Path to SQLite database
        dest_dir: Destination directory root
        dry_run: If True, show what would be done without doing it
        verbose: If True, show details for each file
        resume_from: Skip files before this ID (for resuming interrupted runs)
        log_file: Optional file path for logging output
        sanitize_pipes: If True, replace | with - in paths
    """
    # Setup logging to file if requested
    log_fh = None
    if log_file:
        log_fh = open(log_file, 'a')
        def log(msg):
            print(msg)
            log_fh.write(msg + '\n')
            log_fh.flush()
    else:
        log = print
    
    log(colorize(f"\n{'='*70}", Colors.BOLD))
    log(colorize("  MTIME SYNCHRONIZATION TOOL", Colors.BOLD))
    log(colorize(f"{'='*70}\n", Colors.BOLD))
    
    log(f"Started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Database: {db_path}")
    log(f"Destination: {dest_dir}")
    log(f"Mode: {colorize('DRY RUN', Colors.YELLOW) if dry_run else colorize('LIVE UPDATE', Colors.GREEN)}")
    if resume_from > 0:
        log(f"Resuming from file ID: {resume_from}")
    if sanitize_pipes:
        log(f"Sanitizing pipes: | → -")
    
    # Count total files and build file dictionary once
    log("\nCounting files in database...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM copied_files")
    total_files = cursor.fetchone()[0]
    log(f"Total files to process: {total_files:,}")
    
    # Build file dictionary for path reconstruction and find root auth dir
    log("Building file dictionary for path reconstruction...")
    file_dict = build_file_dict(conn)
    log(f"Loaded {len(file_dict):,} file entries")
    
    # Get root auth directory to strip from paths
    root_auth_dir = get_root_auth_dir(file_dict)
    if root_auth_dir:
        log(f"Found root auth directory: {root_auth_dir}")
    
    conn.close()
    log("")
    
    # Statistics
    stats = {
        'total': 0,
        'updated': 0,
        'skipped_no_timestamp': 0,
        'skipped_not_found': 0,
        'errors': 0,
        'no_change_needed': 0
    }
    
    start_time = time.time()
    last_report_time = start_time
    
    try:
        for file_id, relative_path, timestamp_ms in get_file_info_streaming(db_path, file_dict):
            stats['total'] += 1
            
            # Skip if before resume point
            if stats['total'] < resume_from:
                continue
            
            # Strip root auth directory from path (like restsdk_public.py does)
            if root_auth_dir and relative_path.startswith(root_auth_dir):
                relative_path = relative_path[len(root_auth_dir):].lstrip(os.sep)
            
            # Sanitize pipes if requested
            if sanitize_pipes:
                relative_path = relative_path.replace('|', '-')
            
            # Report progress every 1000 files or 5 seconds
            current_time = time.time()
            if stats['total'] % 1000 == 0 or (current_time - last_report_time) >= 5:
                elapsed = current_time - start_time
                rate = stats['total'] / elapsed if elapsed > 0 else 0
                percent = (stats['total'] / total_files * 100) if total_files > 0 else 0
                remaining = (total_files - stats['total']) / rate if rate > 0 else 0
                eta = datetime.timedelta(seconds=int(remaining))
                
                progress_msg = f"Progress: {stats['total']:,}/{total_files:,} ({percent:.1f}%) | {rate:.0f} files/sec | ETA: {eta}"
                try:
                    print(f"\r{progress_msg}", end='', flush=True)
                except BrokenPipeError:
                    # Handle pipe close gracefully (e.g., when piping to head)
                    pass
                
                # Log to file every 10,000 files
                if log_file and stats['total'] % 10000 == 0:
                    log_fh.write(f"\n{datetime.datetime.now().strftime('%H:%M:%S')} - {progress_msg}\n")
                    log_fh.flush()
                
                last_report_time = current_time
            
            # Skip if no timestamp in database
            if timestamp_ms is None:
                stats['skipped_no_timestamp'] += 1
                if verbose:
                    try:
                        print(f"  [SKIP] {relative_path} - No timestamp in database")
                    except BrokenPipeError:
                        pass
                continue
            
            # Build full destination path
            dest_path = os.path.join(dest_dir, relative_path)
            
            # Check if file exists
            if not os.path.exists(dest_path):
                stats['skipped_not_found'] += 1
                if verbose:
                    try:
                        print(colorize(f"  [NOT FOUND] {relative_path}", Colors.YELLOW))
                    except BrokenPipeError:
                        pass
                continue
            
            # Update mtime
            success, old_mtime, new_mtime, error_msg = update_mtime(dest_path, timestamp_ms, dry_run)
            
            if success:
                # Check if change is significant (>1 second difference)
                time_diff = abs(new_mtime - old_mtime)
                if time_diff < 1:
                    stats['no_change_needed'] += 1
                else:
                    stats['updated'] += 1
                    if verbose or time_diff > 86400:  # Always show if >1 day difference
                        old_date = datetime.datetime.fromtimestamp(old_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        new_date = format_timestamp(timestamp_ms)
                        action = colorize("WOULD UPDATE", Colors.YELLOW) if dry_run else colorize("UPDATED", Colors.GREEN)
                        msg = f"  [{action}] {relative_path}\n    Old: {old_date}  →  New: {new_date}  (Δ {time_diff/86400:.1f} days)"
                        if verbose:
                            try:
                                print(msg)
                            except BrokenPipeError:
                                pass
                        if log_file:
                            log_fh.write(msg + '\n')
            else:
                stats['errors'] += 1
                if verbose:
                    msg = colorize(f"  [ERROR] {relative_path} - {error_msg}", Colors.RED)
                    try:
                        print(msg)
                    except BrokenPipeError:
                        pass
                    if log_file:
                        log_fh.write(msg + '\n')
    
    except KeyboardInterrupt:
        print(colorize("\n\n⚠️  Interrupted by user", Colors.YELLOW))
        msg = f"Resume from: --resume-from {stats['total']}"
        print(msg)
        if log_file:
            log_fh.write(f"\n{msg}\n")
    
    finally:
        if log_file and log_fh:
            log_fh.close()
    
    # Final report
    print()  # New line after progress
    elapsed = time.time() - start_time
    log(f"\n{colorize('='*70, Colors.BOLD)}")
    log(colorize("  SUMMARY", Colors.BOLD))
    log(colorize('='*70, Colors.BOLD))
    log(f"\nCompleted: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Total files processed:     {stats['total']:,}")
    log(f"  {colorize('✓', Colors.GREEN)} Updated:                {stats['updated']:,}")
    log(f"  • No change needed:       {stats['no_change_needed']:,}")
    log(f"  • Skipped (no timestamp): {stats['skipped_no_timestamp']:,}")
    log(f"  • Skipped (not found):    {stats['skipped_not_found']:,}")
    log(f"  {colorize('✗', Colors.RED)} Errors:                 {stats['errors']:,}")
    log(f"\nElapsed time: {elapsed:.1f} seconds ({stats['total']/elapsed:.1f} files/sec)")
    
    if dry_run:
        log(colorize("\n⚠️  This was a DRY RUN - no files were modified", Colors.YELLOW))
        log("Remove --dry-run to actually update mtimes")
    
    log("")


def main():
    parser = argparse.ArgumentParser(
        description="Synchronize modification times from database to destination files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be updated
  %(prog)s --db /path/to/index.db --dest /mnt/nfs-media --dry-run
  
  # Actually update modification times
  %(prog)s --db /path/to/index.db --dest /mnt/nfs-media
  
  # Verbose output showing each file
  %(prog)s --db /path/to/index.db --dest /mnt/nfs-media --verbose
  
  # Resume from file 5000 (if interrupted)
  %(prog)s --db /path/to/index.db --dest /mnt/nfs-media --resume-from 5000
"""
    )
    
    parser.add_argument(
        '--db',
        required=True,
        help='Path to SQLite database (index.db)'
    )
    
    parser.add_argument(
        '--dest',
        required=True,
        help='Destination directory root'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually updating files'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show details for each file processed'
    )
    
    parser.add_argument(
        '--resume-from',
        type=int,
        default=0,
        help='Resume from this file number (for interrupted runs)'
    )
    
    parser.add_argument(
        '--log-file',
        help='Write progress and results to this log file (recommended for SSH sessions)'
    )
    
    parser.add_argument(
        '--sanitize-pipes',
        action='store_true',
        help='Replace | with - in file paths (use if restsdk_public.py was run with --sanitize-pipes)'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.exists(args.db):
        print(colorize(f"Error: Database not found: {args.db}", Colors.RED))
        sys.exit(1)
    
    if not os.path.exists(args.dest):
        print(colorize(f"Error: Destination directory not found: {args.dest}", Colors.RED))
        sys.exit(1)
    
    # Run sync
    sync_mtimes(args.db, args.dest, args.dry_run, args.verbose, args.resume_from, args.log_file, args.sanitize_pipes)


if __name__ == '__main__':
    main()
