#!/usr/bin/env python3
"""
Create a symlink farm from MyCloud SQLite database.

This script creates a directory structure of symbolic links that point to the
original source files but with their correct names and folder hierarchy as
defined in the database. This farm can then be used with rsync to:
1. Copy files to destination with correct structure
2. Verify existing copies against the source
3. Identify missing or extra files

Usage:
    # Interactive wizard mode (recommended for new users)
    python create_symlink_farm.py --wizard
    
    # Command-line mode
    python create_symlink_farm.py --db /path/to/index.db --source /path/to/files --farm /tmp/farm

Then use rsync:
    rsync -avL --progress /tmp/farm/ /mnt/nfs-media/
"""

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple


# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def colorize(text: str, color: str) -> str:
    """Add color to text if terminal supports it."""
    if sys.stdout.isatty():
        return f"{color}{text}{Colors.ENDC}"
    return text


def print_header(text: str):
    """Print a styled header."""
    print()
    print(colorize("=" * 60, Colors.CYAN))
    print(colorize(f"  {text}", Colors.BOLD + Colors.CYAN))
    print(colorize("=" * 60, Colors.CYAN))
    print()


def print_step(step_num: int, text: str):
    """Print a numbered step."""
    print(colorize(f"\n📌 Step {step_num}: ", Colors.BOLD + Colors.YELLOW) + text)


def print_success(text: str):
    """Print a success message."""
    print(colorize(f"✅ {text}", Colors.GREEN))


def print_warning(text: str):
    """Print a warning message."""
    print(colorize(f"⚠️  {text}", Colors.YELLOW))


def print_error(text: str):
    """Print an error message."""
    print(colorize(f"❌ {text}", Colors.RED))


def print_info(text: str):
    """Print an info message."""
    print(colorize(f"ℹ️  {text}", Colors.BLUE))


def prompt_path(prompt: str, must_exist: bool = True, is_dir: bool = True) -> str:
    """Prompt user for a path with validation."""
    while True:
        print()
        path = input(colorize(f"{prompt}: ", Colors.BOLD)).strip()
        
        if not path:
            print_error("Path cannot be empty. Please try again.")
            continue
        
        path = os.path.expanduser(path)  # Expand ~ to home directory
        
        if must_exist:
            if is_dir and not os.path.isdir(path):
                print_error(f"Directory not found: {path}")
                print_info("Please check the path and try again.")
                continue
            elif not is_dir and not os.path.isfile(path):
                print_error(f"File not found: {path}")
                print_info("Please check the path and try again.")
                continue
        
        return path


def prompt_yes_no(prompt: str, default: bool = True) -> bool:
    """Prompt user for yes/no with default."""
    default_str = "[Y/n]" if default else "[y/N]"
    while True:
        response = input(colorize(f"{prompt} {default_str}: ", Colors.BOLD)).strip().lower()
        if not response:
            return default
        if response in ('y', 'yes'):
            return True
        if response in ('n', 'no'):
            return False
        print_error("Please enter 'y' or 'n'")


def format_number(n: int) -> str:
    """Format number with commas."""
    return f"{n:,}"


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        mins = seconds / 60
        return f"{mins:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def print_progress_bar(current: int, total: int, width: int = 40, prefix: str = ""):
    """Print a progress bar."""
    if total == 0:
        return
    
    percent = current / total
    filled = int(width * percent)
    bar = "█" * filled + "░" * (width - filled)
    
    sys.stdout.write(f"\r{prefix}[{bar}] {percent*100:.1f}% ({format_number(current)}/{format_number(total)})")
    sys.stdout.flush()
    
    if current >= total:
        print()  # New line when complete


def load_files_from_db(db_path: str) -> Dict[str, dict]:
    """
    Load all file records from the database into a dictionary.
    
    Args:
        db_path: Path to the SQLite database
        
    Returns:
        Dictionary mapping file_id to file metadata
    """
    file_dic = {}
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT id, parentID, contentID, name, imageDate, videoDate, cTime, birthTime
            FROM Files
        """)
        for row in cur.fetchall():
            file_dic[row['id']] = {
                'Name': row['name'],
                'Parent': row['parentID'],
                'contentID': row['contentID'],
                'imageDate': row['imageDate'],
                'videoDate': row['videoDate'],
                'cTime': row['cTime'],
                'birthTime': row['birthTime'],
            }
    return file_dic


def find_root_dir_name(file_dic: Dict[str, dict]) -> Optional[str]:
    """
    Find the root directory name that contains 'auth' and '|'.
    This is a special folder name that needs to be stripped from paths.
    
    Args:
        file_dic: Dictionary of file metadata
        
    Returns:
        Root directory name to strip, or None
    """
    for file_id, meta in file_dic.items():
        name = meta.get('Name', '')
        if 'auth' in name and '|' in name:
            return name
    return None


def reconstruct_path(file_id: str, file_dic: Dict[str, dict], root_dir_to_strip: Optional[str] = None) -> Optional[str]:
    """
    Reconstruct the full path for a file by traversing parent references.
    
    Args:
        file_id: ID of the file
        file_dic: Dictionary of file metadata
        root_dir_to_strip: Optional root directory name to strip from path
        
    Returns:
        Reconstructed relative path, or None if file not found
    """
    if file_id not in file_dic:
        return None
    
    meta = file_dic[file_id]
    name = meta.get('Name', '')
    parent_id = meta.get('Parent')
    
    # Build path by traversing parents
    path_parts = [name]
    current_id = parent_id
    visited = {file_id}  # Track visited nodes to prevent infinite loops
    
    while current_id is not None and current_id in file_dic:
        # Check for circular reference
        if current_id in visited:
            # Circular reference detected, stop traversal
            break
        
        visited.add(current_id)
        parent_meta = file_dic[current_id]
        parent_name = parent_meta.get('Name', '')
        path_parts.insert(0, parent_name)
        current_id = parent_meta.get('Parent')
    
    # Join path parts
    full_path = '/'.join(path_parts)
    
    # Normalize backslashes to forward slashes
    full_path = full_path.replace('\\', '/')
    
    # Strip root directory if specified
    if root_dir_to_strip:
        full_path = full_path.replace(root_dir_to_strip + '/', '')
        full_path = full_path.replace(root_dir_to_strip, '')
    
    # Remove leading slash
    full_path = full_path.lstrip('/')
    
    return full_path if full_path else None


def get_source_file_path(content_id: str, source_dir: str) -> Optional[str]:
    """
    Get the full path to a source file given its content ID.
    Files are stored in sharded directories by first character.
    
    Args:
        content_id: The content ID of the file
        source_dir: Base directory where source files are stored
        
    Returns:
        Full path to the source file, or None if not found
    """
    if not content_id:
        return None
    
    # Files are stored in directories named by first character
    first_char = content_id[0].lower()
    file_path = os.path.join(source_dir, first_char, content_id)
    
    if os.path.exists(file_path):
        return file_path
    
    # Try without sharding (flat directory)
    flat_path = os.path.join(source_dir, content_id)
    if os.path.exists(flat_path):
        return flat_path
    
    return None


def sanitize_path(path: str, sanitize_pipes: bool = False) -> str:
    """
    Sanitize a path for filesystem compatibility.
    
    Args:
        path: Path to sanitize
        sanitize_pipes: Whether to replace | with -
        
    Returns:
        Sanitized path
    """
    if sanitize_pipes:
        path = path.replace('|', '-')
    return path


def create_symlink_farm(
    db_path: str,
    source_dir: str,
    farm_dir: str,
    sanitize_pipes: bool = False,
    dry_run: bool = False,
    verbose: bool = False
) -> Tuple[int, int, int, int]:
    """
    Create a symlink farm from the database.
    
    Args:
        db_path: Path to SQLite database
        source_dir: Directory containing source files
        farm_dir: Directory to create symlink farm in
        sanitize_pipes: Replace | with - in paths
        dry_run: Don't create symlinks, just report
        verbose: Print verbose output
        
    Returns:
        Tuple of (created, skipped_no_content, skipped_no_source, errors)
    """
    # Load files from database
    print_info(f"Loading files from database...")
    file_dic = load_files_from_db(db_path)
    print_success(f"Loaded {format_number(len(file_dic))} file records")
    
    # Find root directory to strip
    root_dir = find_root_dir_name(file_dic)
    if root_dir:
        print_info(f"Will strip root directory: {root_dir[:50]}...")
    
    # Statistics
    created = 0
    skipped_no_content = 0
    skipped_no_source = 0
    errors = 0
    
    # Create farm directory
    if not dry_run:
        os.makedirs(farm_dir, exist_ok=True)
    
    total = len(file_dic)
    start_time = time.time()
    last_update = start_time
    
    print_info(f"Creating symlinks for {format_number(total)} files...")
    print()
    
    for i, (file_id, meta) in enumerate(file_dic.items()):
        # Progress bar - update every 0.5 seconds or every 1000 files
        current_time = time.time()
        if current_time - last_update >= 0.5 or (i + 1) % 1000 == 0 or i == total - 1:
            print_progress_bar(i + 1, total, prefix="Progress: ")
            last_update = current_time
        
        content_id = meta.get('contentID')
        
        # Skip directories (no content ID)
        if not content_id:
            skipped_no_content += 1
            continue
        
        # Get source file path
        source_path = get_source_file_path(content_id, source_dir)
        if not source_path:
            skipped_no_source += 1
            if verbose:
                print(f"  [SKIP] No source file for {content_id}")
            continue
        
        # Reconstruct destination path
        rel_path = reconstruct_path(file_id, file_dic, root_dir)
        if not rel_path:
            errors += 1
            if verbose:
                print(f"  [ERROR] Could not reconstruct path for {file_id}")
            continue
        
        # Sanitize path
        rel_path = sanitize_path(rel_path, sanitize_pipes)
        
        # Full path in farm
        farm_path = os.path.join(farm_dir, rel_path)
        
        if dry_run:
            if verbose:
                print(f"  [DRY-RUN] {source_path} -> {farm_path}")
            created += 1
            continue
        
        try:
            # Create parent directories
            os.makedirs(os.path.dirname(farm_path), exist_ok=True)
            
            # Create symlink (remove existing if present)
            if os.path.islink(farm_path):
                os.remove(farm_path)
            elif os.path.exists(farm_path):
                # Real file exists, skip
                if verbose:
                    print(f"  [SKIP] Real file exists: {farm_path}")
                continue
            
            os.symlink(source_path, farm_path)
            created += 1
            
            if verbose:
                print(f"  [LINK] {source_path} -> {farm_path}")
                
        except OSError as e:
            errors += 1
            if verbose:
                print(f"  [ERROR] {e}")
    
    return created, skipped_no_content, skipped_no_source, errors


def check_dependencies() -> dict:
    """Check for required and optional dependencies."""
    import shutil
    
    deps = {
        'rsync': shutil.which('rsync'),
        'sqlite3_cli': shutil.which('sqlite3'),
    }
    return deps


def print_install_instructions():
    """Print instructions for installing missing dependencies."""
    print()
    print(colorize("Install rsync:", Colors.BOLD))
    print()
    print("  macOS:    brew install rsync")
    print("  Ubuntu:   sudo apt install rsync")
    print("  Fedora:   sudo dnf install rsync")
    print("  Arch:     sudo pacman -S rsync")
    print()


def run_wizard() -> int:
    """Run interactive wizard mode."""
    print_header("Symlink Farm Creator - Interactive Wizard")
    
    # Check dependencies first
    deps = check_dependencies()
    if not deps['rsync']:
        print_warning("rsync is not installed!")
        print("""
rsync is required to copy files from the symlink farm to your destination.
Without it, you can still create the farm but won't be able to sync files.
""")
        print_install_instructions()
        if not prompt_yes_no("Continue anyway?", default=True):
            return 1
    else:
        print_success(f"rsync found: {deps['rsync']}")
    
    print("""
This tool helps you create a "symlink farm" - a directory structure that
mirrors your original files but uses symbolic links instead of copies.

You can then use this farm with rsync to:
  • Verify your existing backup is complete
  • Copy any missing files
  • Identify extra/duplicate files
""")
    
    # Step 1: Database path
    print_step(1, "Locate your MyCloud database")
    print("""
The database file is usually named 'index.db' and located in:
  /mnt/backupdrive/restsdk/data/db/index.db
  
This file contains the metadata about all your files.
""")
    db_path = prompt_path("Enter the path to index.db", must_exist=True, is_dir=False)
    print_success(f"Found database: {db_path}")
    
    # Step 2: Source files directory
    print_step(2, "Locate your source files directory")
    print("""
This is the directory containing the actual file data, usually:
  /mnt/backupdrive/restsdk/data/files
  
Files here have cryptic names like 'a22236cwsmelmd4on2qs2jdf'.
""")
    source_dir = prompt_path("Enter the path to the source files directory", must_exist=True, is_dir=True)
    print_success(f"Found source directory: {source_dir}")
    
    # Step 3: Farm output directory
    print_step(3, "Choose output directory for symlink farm")
    print("""
This is where the symlink farm will be created. It should be on the
SAME filesystem as the source files (for symlinks to work).

Recommended: /tmp/restore-farm or somewhere on the backup drive.
""")
    farm_dir = prompt_path("Enter the output directory path", must_exist=False)
    
    if os.path.exists(farm_dir) and os.listdir(farm_dir):
        print_warning(f"Directory is not empty: {farm_dir}")
        if not prompt_yes_no("Continue anyway?", default=False):
            print_info("Wizard cancelled.")
            return 0
    
    # Step 4: Options
    print_step(4, "Configure options")
    
    sanitize_pipes = False
    print("""
Some filenames may contain the '|' character which can cause issues
on Windows/NTFS/SMB destinations.
""")
    if prompt_yes_no("Replace '|' with '-' in filenames?", default=False):
        sanitize_pipes = True
        print_success("Will sanitize pipe characters")
    
    dry_run = prompt_yes_no("Do a dry run first (no files created)?", default=True)
    if dry_run:
        print_info("Dry run mode - no symlinks will be created")
    
    # Step 5: Confirmation
    print_step(5, "Confirm and run")
    print_header("Configuration Summary")
    print(f"  📁 Database:    {db_path}")
    print(f"  📂 Source:      {source_dir}")
    print(f"  🔗 Farm output: {farm_dir}")
    print(f"  🔧 Sanitize |:  {'Yes' if sanitize_pipes else 'No'}")
    print(f"  🧪 Dry run:     {'Yes' if dry_run else 'No'}")
    print()
    
    if not prompt_yes_no("Proceed with these settings?", default=True):
        print_info("Wizard cancelled.")
        return 0
    
    # Run the farm creation
    print_header("Creating Symlink Farm")
    start_time = time.time()
    
    created, skipped_no_content, skipped_no_source, errors = create_symlink_farm(
        db_path=db_path,
        source_dir=source_dir,
        farm_dir=farm_dir,
        sanitize_pipes=sanitize_pipes,
        dry_run=dry_run,
        verbose=False
    )
    
    elapsed = time.time() - start_time
    
    # Summary
    print_header("Summary")
    print(f"  ✅ Symlinks created:     {format_number(created)}")
    print(f"  📁 Directories skipped:  {format_number(skipped_no_content)}")
    print(f"  ⚠️  Missing source files: {format_number(skipped_no_source)}")
    print(f"  ❌ Errors:               {format_number(errors)}")
    print(f"  ⏱️  Time elapsed:         {format_duration(elapsed)}")
    print()
    
    if dry_run:
        print_info("This was a DRY RUN - no symlinks were actually created.")
        print()
        if prompt_yes_no("Would you like to run for real now?", default=True):
            print_header("Creating Symlink Farm (for real)")
            created, skipped_no_content, skipped_no_source, errors = create_symlink_farm(
                db_path=db_path,
                source_dir=source_dir,
                farm_dir=farm_dir,
                sanitize_pipes=sanitize_pipes,
                dry_run=False,
                verbose=False
            )
            print_header("Final Summary")
            print(f"  ✅ Symlinks created:     {format_number(created)}")
            print(f"  📁 Directories skipped:  {format_number(skipped_no_content)}")
            print(f"  ⚠️  Missing source files: {format_number(skipped_no_source)}")
            print(f"  ❌ Errors:               {format_number(errors)}")
    
    # Next steps
    if not dry_run or created > 0:
        print_header("Next Steps")
        print("""
Now you can use rsync to verify or sync your backup:
""")
        print(colorize("1. Verify what's different (dry run):", Colors.BOLD))
        print(f"   rsync -avnL --checksum {farm_dir}/ /your/destination/")
        print()
        print(colorize("2. Copy missing files:", Colors.BOLD))
        print(f"   rsync -avL --progress {farm_dir}/ /your/destination/")
        print()
        print(colorize("3. Find extra files in destination:", Colors.BOLD))
        print(f"   rsync -avnL /your/destination/ {farm_dir}/")
        print()
        print_info("Replace '/your/destination/' with your actual NFS path (e.g., /mnt/nfs-media/)")
    
    return 0 if errors == 0 else 1


def main():
    parser = argparse.ArgumentParser(
        description='Create a symlink farm from MyCloud database for rsync verification/copying',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive wizard (recommended for new users)
  python create_symlink_farm.py --wizard
  
  # Command-line mode
  python create_symlink_farm.py --db /path/to/index.db --source /path/to/files --farm /tmp/farm
  
  # Dry run first
  python create_symlink_farm.py --db index.db --source /files --farm /tmp/farm --dry-run
"""
    )
    parser.add_argument('--wizard', '-w', action='store_true', help='Run interactive wizard (recommended)')
    parser.add_argument('--db', help='Path to SQLite database (index.db)')
    parser.add_argument('--source', help='Source directory containing files')
    parser.add_argument('--farm', help='Output directory for symlink farm')
    parser.add_argument('--sanitize-pipes', action='store_true', help='Replace | with - in paths')
    parser.add_argument('--dry-run', '-n', action='store_true', help='Dry run - do not create symlinks')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # If wizard mode or no arguments, run wizard
    if args.wizard or (not args.db and not args.source and not args.farm):
        return run_wizard()
    
    # Command-line mode - validate required args
    if not args.db or not args.source or not args.farm:
        print_error("Missing required arguments. Use --wizard for interactive mode or provide --db, --source, and --farm.")
        parser.print_help()
        sys.exit(1)
    
    # Validate inputs
    if not os.path.exists(args.db):
        print_error(f"Database not found: {args.db}")
        sys.exit(1)
    
    if not os.path.isdir(args.source):
        print_error(f"Source directory not found: {args.source}")
        sys.exit(1)
    
    if os.path.exists(args.farm) and os.listdir(args.farm):
        print_warning(f"Farm directory is not empty: {args.farm}")
        if not prompt_yes_no("Continue?", default=False):
            sys.exit(0)
    
    print_header("Symlink Farm Creator")
    print(f"  📁 Database: {args.db}")
    print(f"  📂 Source:   {args.source}")
    print(f"  🔗 Farm:     {args.farm}")
    print(f"  🧪 Dry run:  {args.dry_run}")
    
    start_time = time.time()
    
    created, skipped_no_content, skipped_no_source, errors = create_symlink_farm(
        db_path=args.db,
        source_dir=args.source,
        farm_dir=args.farm,
        sanitize_pipes=args.sanitize_pipes,
        dry_run=args.dry_run,
        verbose=args.verbose
    )
    
    elapsed = time.time() - start_time
    
    print_header("Summary")
    print(f"  ✅ Symlinks created:     {format_number(created)}")
    print(f"  📁 Directories skipped:  {format_number(skipped_no_content)}")
    print(f"  ⚠️  Missing source files: {format_number(skipped_no_source)}")
    print(f"  ❌ Errors:               {format_number(errors)}")
    print(f"  ⏱️  Time elapsed:         {format_duration(elapsed)}")
    
    if not args.dry_run and created > 0:
        print()
        print_info("Next steps:")
        print(f"  # Verify farm structure:")
        print(f"  find {args.farm} -type l | head -20")
        print()
        print(f"  # Copy to destination with rsync:")
        print(f"  rsync -avL --progress {args.farm}/ /mnt/nfs-media/")
        print()
        print(f"  # Dry-run to see what would be copied:")
        print(f"  rsync -avnL {args.farm}/ /mnt/nfs-media/")
    
    return 0 if errors == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
