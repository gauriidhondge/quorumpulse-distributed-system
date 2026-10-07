import hashlib
import json
import time

def _compute_hash(index: int, timestamp: float, data: dict, prev_hash: str) -> str:
    payload = json.dumps({'index': index, 'timestamp': timestamp, 'data': data, 'prev_hash': prev_hash}, sort_keys=True)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()

def _get_last_block(registry_db):
    row = registry_db.execute('SELECT * FROM ledger_blocks ORDER BY block_index DESC LIMIT 1').fetchone()
    return dict(row) if row else None

def ensure_genesis_block(registry_db) -> None:
    if _get_last_block(registry_db) is not None:
        return
    genesis_data = {'note': 'genesis block — blood/resource allocation ledger initialized'}
    timestamp = time.time()
    prev_hash = '0' * 64
    block_hash = _compute_hash(0, timestamp, genesis_data, prev_hash)
    registry_db.execute('INSERT INTO ledger_blocks (block_index, timestamp, data, prev_hash, hash) VALUES (?,?,?,?,?)', (0, timestamp, json.dumps(genesis_data), prev_hash, block_hash))
    registry_db.commit()

def add_block(registry_db, data: dict) -> dict:
    ensure_genesis_block(registry_db)
    last = _get_last_block(registry_db)
    new_index = last['block_index'] + 1
    timestamp = time.time()
    prev_hash = last['hash']
    block_hash = _compute_hash(new_index, timestamp, data, prev_hash)
    registry_db.execute('INSERT INTO ledger_blocks (block_index, timestamp, data, prev_hash, hash) VALUES (?,?,?,?,?)', (new_index, timestamp, json.dumps(data), prev_hash, block_hash))
    registry_db.commit()
    return {'block_index': new_index, 'timestamp': timestamp, 'data': data, 'prev_hash': prev_hash, 'hash': block_hash}

def get_chain(registry_db) -> list:
    ensure_genesis_block(registry_db)
    rows = registry_db.execute('SELECT * FROM ledger_blocks ORDER BY block_index ASC').fetchall()
    chain = []
    for row in rows:
        block = dict(row)
        block['data'] = json.loads(block['data'])
        chain.append(block)
    return chain

def is_chain_valid(registry_db) -> dict:
    chain = get_chain(registry_db)
    for i, block in enumerate(chain):
        recomputed = _compute_hash(block['block_index'], block['timestamp'], block['data'], block['prev_hash'])
        if recomputed != block['hash']:
            return {'valid': False, 'broken_at_block': block['block_index'], 'reason': 'stored hash does not match recomputed hash — data was tampered with'}
        if i > 0 and block['prev_hash'] != chain[i - 1]['hash']:
            return {'valid': False, 'broken_at_block': block['block_index'], 'reason': "prev_hash does not match the previous block's actual hash — chain link broken"}
    return {'valid': True, 'blocks_verified': len(chain)}
