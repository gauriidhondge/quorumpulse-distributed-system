import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from typing import List
from common.config import HDFS_BASE_DIR, HDFS_BIN, HDFS_CALL_TIMEOUT_SECONDS

class HdfsError(RuntimeError):
    pass

def _run(args: List[str]) -> str:
    try:
        result = subprocess.run([HDFS_BIN, *args], capture_output=True, text=True, timeout=HDFS_CALL_TIMEOUT_SECONDS)
    except FileNotFoundError:
        raise HdfsError(f"'{HDFS_BIN}' is not on PATH -- is Hadoop installed and is its bin/ directory on this shell's PATH?")
    except subprocess.TimeoutExpired:
        raise HdfsError(f'hdfs command timed out after {HDFS_CALL_TIMEOUT_SECONDS}s -- is the NameNode/DataNode actually running? Check with `jps`.')
    if result.returncode != 0:
        raise HdfsError((result.stderr or result.stdout or 'unknown hdfs error').strip())
    return result.stdout

def ensure_base_dir() -> None:
    _run(['dfs', '-mkdir', '-p', HDFS_BASE_DIR])

def upload_file(local_path: str, hdfs_filename: str) -> str:
    ensure_base_dir()
    hdfs_path = f'{HDFS_BASE_DIR}/{hdfs_filename}'
    _run(['dfs', '-put', '-f', local_path, hdfs_path])
    return hdfs_path

@dataclass
class HdfsFileEntry:
    name: str
    size_bytes: int
    replication: int
    hdfs_path: str

def list_files() -> List[HdfsFileEntry]:
    ensure_base_dir()
    try:
        raw = _run(['dfs', '-ls', HDFS_BASE_DIR])
    except HdfsError:
        return []
    entries: List[HdfsFileEntry] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith('Found '):
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        perms_and_type = parts[0]
        if perms_and_type.startswith('d'):
            continue
        try:
            replication = int(parts[1]) if parts[1] != '-' else 1
            size_bytes = int(parts[4])
        except ValueError:
            continue
        hdfs_path = parts[-1]
        entries.append(HdfsFileEntry(name=os.path.basename(hdfs_path), size_bytes=size_bytes, replication=replication, hdfs_path=hdfs_path))
    return entries

def download_to_tempfile(hdfs_filename: str) -> str:
    hdfs_path = f'{HDFS_BASE_DIR}/{hdfs_filename}'
    fd, local_path = tempfile.mkstemp(prefix='hdfs_dl_')
    os.close(fd)
    os.remove(local_path)
    _run(['dfs', '-get', hdfs_path, local_path])
    return local_path
_REPORT_LINE = re.compile('^(Live datanodes|Dead datanodes|Configured Capacity|DFS Used%)\\s*:?\\s*(.*)$')

def cluster_status() -> dict:
    raw = _run(['dfsadmin', '-report'])
    summary = {}
    for line in raw.splitlines():
        match = _REPORT_LINE.match(line.strip())
        if match:
            key, value = match.groups()
            summary[key.strip()] = value.strip()
    return {'reachable': True, 'summary': summary, 'raw': raw}
