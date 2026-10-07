import uuid
from typing import Dict, List
DEFAULT_CHUNK_SIZE = 4096

def new_file_id() -> str:
    return uuid.uuid4().hex[:12]

def chunk_bytes(data: bytes, chunk_size: int=DEFAULT_CHUNK_SIZE) -> List[bytes]:
    if not data:
        return [b'']
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

def choose_replica_nodes(chunk_index: int, node_names: List[str], replication_factor: int) -> List[str]:
    n = len(node_names)
    replication_factor = min(replication_factor, n)
    start = chunk_index % n
    return [node_names[(start + i) % n] for i in range(replication_factor)]

def build_chunk_map(total_chunks: int, node_names: List[str], replication_factor: int) -> Dict[int, List[str]]:
    return {i: choose_replica_nodes(i, node_names, replication_factor) for i in range(total_chunks)}
