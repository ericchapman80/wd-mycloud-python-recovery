# WD MyCloud Python Recovery (Legacy - Maintenance Mode)

⚠️ **This tool is in maintenance mode. For new projects, use [wd-mycloud-rsync-recovery](https://github.com/ericchapman80/wd-mycloud-rsync-recovery)**

Recover and transfer files from a Western Digital (WD) MyCloud device using Python-based REST SDK approach.

> **📢 This is the legacy Python-based recovery tool.** It remains available for existing users but is no longer actively developed. Critical bug fixes only.

---

## ☕ Support This Project

If this tool saved your data, consider supporting continued development:

- **GitHub Sponsors:** [Sponsor @ericchapman80](https://github.com/sponsors/ericchapman80)

---

## 🚨 Migration Notice

**Recommended:** Switch to the modern rsync-based tool for:
- ✅ Automatic timestamp preservation (no sync_mtime.py needed)
- ✅ Better resume capability
- ✅ Lower memory usage (~50 MB vs 2-10 GB)
- ✅ Real-time progress tracking
- ✅ Active development and new features

👉 **[Get the modern tool →](https://github.com/ericchapman80/wd-mycloud-rsync-recovery)**

---

## Quick Start (Legacy Tool)

**macOS users (install dependencies first):**
```bash
brew install python@3.12
```

**Setup:**
```bash
# Standard setup
./setup.sh

# Activate virtual environment
source venv/bin/activate

# Run preflight analysis
python preflight.py /path/to/source /path/to/dest

# Run recovery
python restsdk_public.py --db index.db --filedir /source --dumpdir /dest

# For --low-memory mode, sync timestamps afterward
python sync_mtime.py --db index.db --filedir /source --dumpdir /dest
```

## Features

- Multi-threaded file recovery via WD MyCloud REST SDK
- Memory-optimized mode (`--low-memory`)
- Resume capability with path-based matching
- Symlink-based deduplication
- Metadata validation tools
- Preflight system analysis with thread recommendations

## Key CLI Options

| Option | Description |
|--------|-------------|
| `--resume` | Resume a previous run (regenerates log from destination) |
| `--low-memory` | Reduce RAM usage ~40% (disables mtime preservation) |
| `--thread-count N` | Number of threads (default: CPU count) |
| `--preserve-mtime` | Set destination mtime from DB timestamps (default: on) |
| `--sanitize-pipes` | Replace `\|` with `-` for Windows/NTFS/SMB targets |
| `--io-buffer-size N` | Buffer size for manual buffered copies (default: 0) |
| `--io-max-concurrency N` | Limit concurrent disk I/O (default: 0 = no cap) |
| `--preflight` | Run system analysis before copying |

## Low-Memory Mode

For systems with limited RAM (< 16GB) or very large file databases (500K+ files):

```bash
python restsdk_public.py \
    --db=/path/to/index.db \
    --filedir=/path/to/source \
    --dumpdir=/path/to/dest \
    --log_file=copied_file.log \
    --low-memory \
    --thread-count=2 \
    --resume
```

**Memory comparison (500K files):**

| Mode | RAM Usage | Preserve mtime |
|------|-----------|----------------|
| Normal | ~11GB | ✅ Yes |
| `--low-memory` | ~6-7GB | ❌ No |
| `--low-memory --thread-count=2` | ~5-6GB | ❌ No |

## Tools

- **restsdk_public.py** - Main recovery script (Python/REST SDK)
- **sync_mtime.py** - Post-recovery timestamp sync (required for --low-memory)
- **preflight.py** - System analysis and thread recommendations
- **create_symlink_farm.py** - Symlink-based deduplication
- **mtime_check.py** - Metadata validation utility

## Testing

```bash
# Run all legacy tests
./run_tests.sh

# Run with coverage
./run_tests.sh html
```

**Test Coverage:** 63% (stable baseline)

## Why Maintenance Mode?

The modern rsync-based approach (`wd-mycloud-rsync-recovery`) offers:
- Simpler operation (fewer manual steps)
- No separate timestamp sync needed
- Better performance and reliability
- Lower resource usage
- Active feature development

This Python tool remains available for:
- Existing users with established workflows
- Environments where rsync is not available
- Specific use cases requiring Python API access

## Support

- **Active Development:** [wd-mycloud-rsync-recovery](https://github.com/ericchapman80/wd-mycloud-rsync-recovery)
- **Critical Bugs:** Open issues in this repository
- **Questions:** See modern tool documentation

## Running Over SSH

For long-running recoveries over SSH, use `tmux` or `screen`:

```bash
# Start a detachable session
tmux new -s recovery

# Run recovery inside the session
source venv/bin/activate
python restsdk_public.py --resume --db index.db --filedir /source --dumpdir /dest --log_file copied_file.log

# Detach: Ctrl+B then D
# Reattach later: tmux attach -t recovery
```

## Monitoring

While the script runs, monitor progress in another terminal:

```bash
# Follow the log
tail -f summary_*.log

# Check copied files count
sqlite3 /path/to/index.db "SELECT COUNT(*) FROM copied_files"

# Check skipped files
sqlite3 /path/to/index.db "SELECT COUNT(*) FROM skipped_files"

# Run the monitor script
nohup ./monitor.sh /path/to/monitor.log 30 > /dev/null 2>&1 &
```

## FAQ

**Why do I see "File not found in database" errors?**

Files may be missing from the database due to corruption or interrupted operations on the MyCloud device. These are skipped and reported.

**How is the database structured?**

- Main table: `Files`
- `contentID`: On-disk filename (e.g., `a22236cwsmelmd4on2qs2jdf`)
- `name`: Original human-readable filename
- `parentID`: Reference to parent directory for path reconstruction
- Files stored in sharded directories: `/files/a/a22236...`, `/files/b/b12345...`

**When to use `--sanitize-pipes`?**

Needed for destinations that disallow `|` in filenames (Windows NTFS/FAT and many SMB shares). Leave off for Linux/macOS/EXT4/APFS.

## Documentation

- **Modern Tool (Recommended):** [wd-mycloud-rsync-recovery](https://github.com/ericchapman80/wd-mycloud-rsync-recovery)

## License

See [LICENSE](LICENSE) file.

## Credits

Original script by [springfielddatarecovery](https://github.com/springfielddatarecovery/mycloud-restsdk-recovery-script)

Legacy Python approach maintained by [@ericchapman80](https://github.com/ericchapman80)

Modern rsync approach: [wd-mycloud-rsync-recovery](https://github.com/ericchapman80/wd-mycloud-rsync-recovery)
