import os
import sqlite3
DB_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_donor_db(node_name: str) -> sqlite3.Connection:
    path = os.path.join(DB_DIR, f'donor_{node_name}.db')
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('\n        CREATE TABLE IF NOT EXISTS donors (\n            donor_id TEXT PRIMARY KEY,\n            blood_group TEXT NOT NULL,\n            units_available INTEGER NOT NULL,\n            city TEXT NOT NULL,\n            available INTEGER DEFAULT 1\n        )\n        ')
    conn.commit()
    return conn

def get_registry_db() -> sqlite3.Connection:
    path = os.path.join(DB_DIR, 'registry.db')
    conn = sqlite3.connect(path, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute("\n        CREATE TABLE IF NOT EXISTS node_registry (\n            node_name TEXT PRIMARY KEY,\n            host TEXT NOT NULL,\n            port INTEGER NOT NULL,\n            last_heartbeat REAL,\n            status TEXT DEFAULT 'online'\n        )\n        ")
    conn.execute('\n        CREATE TABLE IF NOT EXISTS coordinator_state (\n            id INTEGER PRIMARY KEY CHECK (id = 1),\n            coordinator TEXT,\n            updated_at REAL\n        )\n        ')
    conn.execute('\n        CREATE TABLE IF NOT EXISTS dfs_manifest (\n            file_id TEXT PRIMARY KEY,\n            filename TEXT NOT NULL,\n            total_chunks INTEGER NOT NULL,\n            replication_factor INTEGER NOT NULL,\n            chunk_map TEXT NOT NULL,   -- JSON: {chunk_index: [node_name, ...]}\n            uploaded_at REAL\n        )\n        ')
    conn.execute('\n        CREATE TABLE IF NOT EXISTS ledger_blocks (\n            block_index INTEGER PRIMARY KEY,\n            timestamp REAL NOT NULL,\n            data TEXT NOT NULL,       -- JSON payload (the allocation record)\n            prev_hash TEXT NOT NULL,\n            hash TEXT NOT NULL\n        )\n        ')
    conn.commit()
    return conn
