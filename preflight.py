import os
import platform
import psutil
import shutil
import socket
import time
from pathlib import Path

PIPE_FS_TAGS = ("ntfs", "vfat", "fat", "msdos", "exfat", "cifs", "smb")

def get_cpu_info():
    cpu_count = os.cpu_count()
    cpu_freq = psutil.cpu_freq()
    cpu_model = platform.processor() or platform.uname().processor
    return {
        'cpu_count': cpu_count,
        'cpu_freq': cpu_freq.current if cpu_freq else None,
        'cpu_model': cpu_model,
    }

def get_memory_info():
    mem = psutil.virtual_memory()
    return {
        'total': mem.total,
        'available': mem.available,
        'used': mem.used,
        'percent': mem.percent,
    }

def get_disk_info(path):
    usage = psutil.disk_usage(path)
    fstype = None
    best = (None, -1)
    for part in psutil.disk_partitions(all=True):
        mp = part.mountpoint
        if path == mp or path.startswith(mp.rstrip(os.sep) + os.sep):
            if len(mp) > best[1]:
                best = (part, len(mp))
    if best[0]:
        fstype = best[0].fstype
    return {
        'total': usage.total,
        'used': usage.used,
        'free': usage.free,
        'percent': usage.percent,
        'filesystem': fstype,
    }

def get_network_info():
    addrs = psutil.net_if_addrs()
    stats = psutil.net_if_stats()
    interfaces = {}
    for iface, addr_list in addrs.items():
        iface_stats = stats.get(iface, None)
        interfaces[iface] = {
            'isup': iface_stats.isup if iface_stats else None,
            'speed': iface_stats.speed if iface_stats else None,
            'addresses': [a.address for a in addr_list if a.family in (socket.AF_INET, socket.AF_INET6)],
        }
    return interfaces

def disk_speed_test(path, file_size_mb=128):
    test_file = Path(path) / 'preflight_speed_test.tmp'
    data = os.urandom(1024 * 1024)  # 1MB buffer
    start = time.time()
    with open(test_file, 'wb') as f:
        for _ in range(file_size_mb):
            f.write(data)
    write_time = time.time() - start
    start = time.time()
    with open(test_file, 'rb') as f:
        while f.read(1024 * 1024):
            pass
    read_time = time.time() - start
    os.remove(test_file)
    return {
        'write_MBps': file_size_mb / write_time,
        'read_MBps': file_size_mb / read_time,
    }

def get_file_stats(directory):
    total_files = 0
    total_size = 0
    small = 0
    medium = 0
    large = 0
    pipe_names = 0
    for root, _, files in os.walk(directory):
        for file in files:
            try:
                fp = os.path.join(root, file)
                size = os.path.getsize(fp)
                if "|" in file:
                    pipe_names += 1
                total_files += 1
                total_size += size
                if size < 1 * 1024 * 1024:
                    small += 1
                elif size < 100 * 1024 * 1024:
                    medium += 1
                else:
                    large += 1
            except Exception:
                continue
    return {
        'total_files': total_files,
        'total_size_GB': total_size / (1024 ** 3),
        'small_files': small,
        'medium_files': medium,
        'large_files': large,
        'pipe_names': pipe_names,
    }

def estimate_duration(total_size_gb, min_MBps):
    if min_MBps <= 0:
        return float('inf')
    total_MB = total_size_gb * 1024
    return total_MB / min_MBps / 60  # in minutes

def recommend_thread_count(cpu_count, file_stats, disk_speed_MBps=None, dest_fs=None):
    """
    Recommend thread count based on multiple factors:
    - CPU count
    - File size distribution  
    - Disk write speed (I/O bottleneck)
    - Filesystem type (NFS/network filesystems need fewer threads)
    
    Returns: (recommended_threads, explanation_dict)
    """
    # Base recommendation from CPU and file sizes
    if file_stats['small_files'] > file_stats['medium_files'] + file_stats['large_files']:
        cpu_rec = min(max(4, cpu_count * 2), 32)
        cpu_reason = f"many small files (2x CPU cores, max 32)"
    else:
        cpu_rec = min(max(2, cpu_count), 16)
        cpu_reason = f"mixed/large files (1x CPU cores, max 16)"
    
    # Cap based on disk write speed - more threads don't help if disk is the bottleneck
    # Rule of thumb: ~1 thread per 20 MB/s of write throughput (accounts for overhead)
    if disk_speed_MBps and disk_speed_MBps > 0:
        io_rec = max(2, min(int(disk_speed_MBps / 20) + 1, 16))
        io_reason = f"{disk_speed_MBps:.0f} MB/s write speed"
    else:
        io_rec = cpu_rec
        io_reason = "unknown (defaulting to CPU-based)"
    
    # For network filesystems (NFS, CIFS, SMB), cap at CPU count to avoid contention
    is_network_fs = dest_fs and any(tag in dest_fs.lower() for tag in ['nfs', 'cifs', 'smb', 'fuse'])
    if is_network_fs:
        net_rec = cpu_count
        net_reason = f"{dest_fs} (network filesystem, capped at CPU count)"
    else:
        net_rec = cpu_rec
        net_reason = f"{dest_fs or 'local'} (no network cap)"
    
    # Return the most conservative recommendation with explanation
    final = min(cpu_rec, io_rec, net_rec)
    
    # Determine limiting factor
    if final == io_rec and io_rec < cpu_rec:
        limiting = "disk I/O speed"
    elif final == net_rec and net_rec < cpu_rec:
        limiting = "network filesystem"
    else:
        limiting = "CPU/file characteristics"
    
    explanation = {
        'cpu_rec': cpu_rec,
        'cpu_reason': cpu_reason,
        'io_rec': io_rec,
        'io_reason': io_reason,
        'net_rec': net_rec,
        'net_reason': net_reason,
        'limiting_factor': limiting,
    }
    
    return final, explanation

def recommend_thread_count_with_fd(cpu_count, file_stats, fd_limit, disk_speed_MBps=None, dest_fs=None):
    """Cap threads based on CPU, FD limit, disk speed, and filesystem type."""
    fd_safe = max(2, min((fd_limit - 100) // 2, 32)) if fd_limit else 8
    base_rec, explanation = recommend_thread_count(cpu_count, file_stats, disk_speed_MBps, dest_fs)
    final = min(base_rec, fd_safe)
    if final < base_rec:
        explanation['limiting_factor'] = 'file descriptor limit'
    explanation['fd_rec'] = fd_safe
    return final, explanation

def preflight_summary(source, dest):
    cpu = get_cpu_info()
    mem = get_memory_info()
    disk_src = get_disk_info(source)
    disk_dst = get_disk_info(dest)
    net = get_network_info()
    file_stats = get_file_stats(source)
    disk_speed = disk_speed_test(dest)
    min_MBps = min(disk_speed['write_MBps'], disk_speed['read_MBps'])
    est_min = estimate_duration(file_stats['total_size_GB'], min_MBps)
    dest_fs = disk_dst.get('filesystem')
    fd_limit = os.sysconf('SC_OPEN_MAX') if hasattr(os, 'sysconf') else None
    # Use smart thread recommendation considering all factors
    thread_count, thread_explanation = recommend_thread_count(
        cpu['cpu_count'], file_stats, 
        disk_speed['write_MBps'], dest_fs
    )
    fd_based_threads, _ = recommend_thread_count_with_fd(
        cpu['cpu_count'], file_stats, fd_limit,
        disk_speed['write_MBps'], dest_fs
    )
    return {
        'cpu': cpu,
        'memory': mem,
        'disk_src': disk_src,
        'disk_dst': disk_dst,
        'network': net,
        'file_stats': file_stats,
        'disk_speed': disk_speed,
        'est_min': est_min,
        'thread_count': thread_count,
        'thread_explanation': thread_explanation,
        'fd_limit': fd_limit,
        'fd_based_threads': fd_based_threads,
    }

def print_preflight_report(summary, source, dest):
    print("\n🚀  ===== Pre-flight Hardware & File System Check ===== 🚀\n")
    cpu = summary['cpu']
    mem = summary['memory']
    dest_fs = summary['disk_dst'].get('filesystem')
    print(f"🖥️  CPU: {cpu['cpu_model']} | Cores: {cpu['cpu_count']} | Freq: {cpu['cpu_freq']} MHz")
    print(f"💾 RAM: {mem['total'] // (1024**3)} GB total | {mem['available'] // (1024**3)} GB available")
    print(f"📂 Source: {source}")
    print(f"  - Size: {summary['file_stats']['total_size_GB']:.2f} GB | Files: {summary['file_stats']['total_files']}")
    print(f"  - Filenames containing '|': {summary['file_stats']['pipe_names']}")
    print(f"  - Small: {summary['file_stats']['small_files']} | Medium: {summary['file_stats']['medium_files']} | Large: {summary['file_stats']['large_files']}")
    print(f"💽 Dest: {dest}")
    print(f"  - Free: {summary['disk_dst']['free'] // (1024**3)} GB | Total: {summary['disk_dst']['total'] // (1024**3)} GB | FS: {dest_fs}")
    print(f"⚡ Disk Speed (dest): Write: {summary['disk_speed']['write_MBps']:.1f} MB/s | Read: {summary['disk_speed']['read_MBps']:.1f} MB/s")
    print(f"⏱️  Estimated Duration: {summary['est_min']:.1f} minutes (best case)")
    thread_exp = summary['thread_explanation']
    print(f"🔢 Recommended Threads: {summary['thread_count']} (limited by: {thread_exp['limiting_factor']})")
    print(f"   ├─ CPU-based: {thread_exp['cpu_rec']} ({thread_exp['cpu_reason']})")
    print(f"   ├─ I/O-based: {thread_exp['io_rec']} ({thread_exp['io_reason']})")
    print(f"   └─ FS-based:  {thread_exp['net_rec']} ({thread_exp['net_reason']})")
    if summary['file_stats']['pipe_names'] > 0 and dest_fs and any(tag in dest_fs.lower() for tag in PIPE_FS_TAGS):
        print("\n⚠️  Destination filesystem may reject '|' in filenames. Consider using --sanitize-pipes.")
    print("\n✨ Recommended Command:")
    cmd = f"python restsdk_public.py --db <path/to/index.db> --filedir {source} --dumpdir {dest} --log_file <path/to/logfile.log> --thread-count {summary['thread_count']}"
    print(f"\n📝 {cmd}\n")
    print("Copy and paste the above command, replacing <...> with your actual file paths!")
    print("\nQuestions? See the README or /docs for help. Happy transferring! 🚚✨\n")
