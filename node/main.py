import os
import threading
import time
import requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from common.config import NODES, HEARTBEAT_INTERVAL_SECONDS
from common.database import get_donor_db, get_registry_db
from common.queue_client import consume_urgent_request
from common.lamport_clock import LamportClock
from common.vector_clock import VectorClock
from common.election import ElectionManager
from common.mutual_exclusion import MutexManager
from common.snapshot import take_local_snapshot
from node.donor_seed import seed
from node.models import SearchRequest, SearchResponse
NODE_NAME = os.environ.get('NODE_NAME', 'pune')
PORT = int(os.environ.get('PORT', NODES.get(NODE_NAME, {}).get('port', 8001)))
HOST = NODES.get(NODE_NAME, {}).get('host', 'localhost')
app = FastAPI(title=f'Donor Node — {NODE_NAME.title()}')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
donor_db = get_donor_db(NODE_NAME)
seed(donor_db, NODE_NAME)
registry_db = get_registry_db()
lamport_clock = LamportClock()
vector_clock = VectorClock(NODE_NAME, list(NODES.keys()))
election = ElectionManager(NODE_NAME, NODES, self_url=f'http://{HOST}:{PORT}')
mutex = MutexManager(NODE_NAME, NODES, lamport_clock)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DFS_DIR = os.path.join(PROJECT_ROOT, f'dfs_storage_{NODE_NAME}')
os.makedirs(DFS_DIR, exist_ok=True)

def _search_local(req: SearchRequest) -> SearchResponse:
    row = donor_db.execute('\n        SELECT * FROM donors\n        WHERE blood_group = ? AND units_available >= ? AND available = 1\n        LIMIT 1\n        ', (req.blood_group, req.units)).fetchone()
    if row:
        return SearchResponse(found=True, donor_id=row['donor_id'], city=row['city'], units_available=row['units_available'], served_by=NODE_NAME, vector=vector_clock.value)
    return SearchResponse(found=False, served_by=NODE_NAME, vector=vector_clock.value)

def _tick_vector_for_request(req: SearchRequest) -> None:
    if req.vector:
        vector_clock.update(req.vector)
    else:
        vector_clock.tick()

@app.post('/rpc/search_donor', response_model=SearchResponse)
def search_donor(req: SearchRequest):
    lamport_clock.tick()
    _tick_vector_for_request(req)
    return _search_local(req)

@app.post('/rpc/search_donor_p2p', response_model=SearchResponse)
def search_donor_p2p(req: SearchRequest):
    lamport_clock.tick()
    _tick_vector_for_request(req)
    local = _search_local(req)
    if local.found:
        return local
    outgoing = req.model_copy(update={'vector': vector_clock.value})
    for name, info in NODES.items():
        if name == NODE_NAME:
            continue
        try:
            resp = requests.post(f"http://{info['host']}:{info['port']}/rpc/search_donor", json=outgoing.model_dump(), timeout=2)
            resp.raise_for_status()
            result = SearchResponse(**resp.json())
            if result.vector:
                vector_clock.update(result.vector)
            if result.found:
                result.vector = vector_clock.value
                return result
        except requests.exceptions.RequestException:
            continue
    return SearchResponse(found=False, served_by=NODE_NAME, vector=vector_clock.value)

@app.get('/inventory_count')
def inventory_count():
    rows = donor_db.execute('\n        SELECT blood_group, SUM(units_available) as total\n        FROM donors WHERE available = 1\n        GROUP BY blood_group\n        ').fetchall()
    return {row['blood_group']: row['total'] for row in rows}

@app.get('/health')
def health():
    return {'node': NODE_NAME, 'status': 'online', 'port': PORT}

@app.get('/clocks')
def clocks():
    return {'node': NODE_NAME, 'lamport_clock': lamport_clock.value, 'vector_clock': vector_clock.value}

@app.post('/election/message')
def election_message(payload: dict):
    return election.receive_election_message(payload.get('from'), registry_db)

@app.post('/election/coordinator')
def election_coordinator_announce(payload: dict):
    return election.receive_coordinator_announcement(payload.get('coordinator'), registry_db)

@app.post('/election/start')
def election_start():
    return election.start_election(registry_db)

@app.get('/election/status')
def election_status():
    return election.status()

@app.post('/mutex/request')
def mutex_request_endpoint(payload: dict):
    from_node = payload['from']
    from_ts = payload['timestamp']
    decision = mutex.on_request(from_node, from_ts)
    if decision == 'reply_now':
        info = NODES[from_node]
        try:
            requests.post(f"http://{info['host']}:{info['port']}/mutex/reply", json={'from': NODE_NAME}, timeout=3)
        except requests.exceptions.RequestException:
            pass
    return {'decision': decision}

@app.post('/mutex/reply')
def mutex_reply_endpoint(payload: dict):
    mutex.reply_received(payload['from'])
    return {'status': 'ack'}

@app.post('/mutex/acquire_and_reserve')
def mutex_acquire_and_reserve(payload: dict):
    donor_id = payload['donor_id']
    units = payload.get('units', 1)
    acquire_result = mutex.acquire(timeout=10)
    if acquire_result.get('status') != 'acquired':
        return {'reserved': False, 'mutex_result': acquire_result}
    try:
        row = donor_db.execute('SELECT * FROM donors WHERE donor_id = ?', (donor_id,)).fetchone()
        if not row or row['units_available'] < units:
            return {'reserved': False, 'reason': 'insufficient_units_or_not_found', 'mutex_result': acquire_result}
        donor_db.execute('UPDATE donors SET units_available = units_available - ? WHERE donor_id = ?', (units, donor_id))
        donor_db.commit()
        new_row = donor_db.execute('SELECT units_available FROM donors WHERE donor_id = ?', (donor_id,)).fetchone()
        return {'reserved': True, 'donor_id': donor_id, 'units_reserved': units, 'remaining_units': new_row['units_available'], 'mutex_result': acquire_result}
    finally:
        mutex.release()

@app.get('/mutex/status')
def mutex_status():
    return mutex.status()

@app.get('/snapshot/local')
def snapshot_local():
    return take_local_snapshot(NODE_NAME, donor_db, lamport_clock, vector_clock, mutex)

@app.get('/objects/donor/{donor_id}')
def object_get_donor(donor_id: str):
    row = donor_db.execute('SELECT * FROM donors WHERE donor_id = ?', (donor_id,)).fetchone()
    if not row:
        return {'error': 'not_found', 'donor_id': donor_id}
    return dict(row)

@app.post('/objects/donor/{donor_id}/reserve')
def object_reserve_donor(donor_id: str, payload: dict):
    units = payload.get('units', 1)
    row = donor_db.execute('SELECT * FROM donors WHERE donor_id = ?', (donor_id,)).fetchone()
    if not row or row['units_available'] < units:
        return {'success': False, 'reason': 'insufficient_units_or_not_found'}
    donor_db.execute('UPDATE donors SET units_available = units_available - ? WHERE donor_id = ?', (units, donor_id))
    donor_db.commit()
    new_row = donor_db.execute('SELECT units_available FROM donors WHERE donor_id = ?', (donor_id,)).fetchone()
    return {'success': True, 'donor_id': donor_id, 'remaining_units': new_row['units_available']}

@app.post('/objects/donor/{donor_id}/release')
def object_release_donor(donor_id: str, payload: dict):
    units = payload.get('units', 1)
    donor_db.execute('UPDATE donors SET units_available = units_available + ? WHERE donor_id = ?', (units, donor_id))
    donor_db.commit()
    new_row = donor_db.execute('SELECT units_available FROM donors WHERE donor_id = ?', (donor_id,)).fetchone()
    return {'success': True, 'donor_id': donor_id, 'remaining_units': new_row['units_available']}

@app.post('/dfs/store_chunk')
async def dfs_store_chunk(request: Request, file_id: str, chunk_index: int):
    body = await request.body()
    path = os.path.join(DFS_DIR, f'{file_id}__chunk{chunk_index}.bin')
    with open(path, 'wb') as f:
        f.write(body)
    return {'stored': True, 'node': NODE_NAME, 'file_id': file_id, 'chunk_index': chunk_index, 'bytes': len(body)}

@app.get('/dfs/fetch_chunk')
def dfs_fetch_chunk(file_id: str, chunk_index: int):
    path = os.path.join(DFS_DIR, f'{file_id}__chunk{chunk_index}.bin')
    if not os.path.exists(path):
        return Response(status_code=404)
    with open(path, 'rb') as f:
        data = f.read()
    return Response(content=data, media_type='application/octet-stream')

def heartbeat_loop():
    hb_db = get_registry_db()
    while True:
        try:
            hb_db.execute("\n                INSERT INTO node_registry (node_name, host, port, last_heartbeat, status)\n                VALUES (?, ?, ?, ?, 'online')\n                ON CONFLICT(node_name) DO UPDATE SET\n                    last_heartbeat = excluded.last_heartbeat,\n                    status = 'online'\n                ", (NODE_NAME, HOST, PORT, time.time()))
            hb_db.commit()
        except Exception as exc:
            print(f'[{NODE_NAME}] heartbeat error: {exc}')
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)

def urgent_listener_loop():
    while True:
        try:
            req_dict = consume_urgent_request(timeout=5)
            if req_dict:
                result = _search_local(SearchRequest(**req_dict))
                print(f'[{NODE_NAME}] urgent broadcast received: {req_dict} -> {result.model_dump()}')
        except Exception as exc:
            print(f'[{NODE_NAME}] queue error (is Redis running?): {exc}')
            time.sleep(3)

def coordinator_watchdog_loop():
    time.sleep(8)
    if election.coordinator is None:
        print(f'[{NODE_NAME}] bootstrapping: no known coordinator -> starting election')
        election.start_election(registry_db)
    while True:
        time.sleep(10)
        coord = election.coordinator
        if coord and coord != NODE_NAME:
            row = registry_db.execute('SELECT status FROM node_registry WHERE node_name = ?', (coord,)).fetchone()
            if row and row['status'] == 'offline':
                print(f"[{NODE_NAME}] coordinator '{coord}' appears offline -> starting election")
                election.start_election(registry_db)
threading.Thread(target=heartbeat_loop, daemon=True).start()
threading.Thread(target=urgent_listener_loop, daemon=True).start()
threading.Thread(target=coordinator_watchdog_loop, daemon=True).start()
