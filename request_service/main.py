import asyncio
import os
import tempfile
import time
import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from common.config import NODES, STALE_THRESHOLD_SECONDS, SERVERLESS_GATEWAY_URL, DFS_REPLICATION_FACTOR
from common.database import get_registry_db
from common.queue_client import publish_urgent_request
from common.object_proxy import DonorRecordProxy
from common.blockchain_ledger import add_block, get_chain, is_chain_valid
from common.deadlock_detection import run_demo_scenarios
from common.dfs import new_file_id, chunk_bytes, build_chunk_map
from common.hadoop_hdfs import HdfsError, upload_file as hdfs_upload_file, list_files as hdfs_list_files, download_to_tempfile as hdfs_download_to_tempfile, cluster_status as hdfs_cluster_status
from node.models import SearchRequest
app = FastAPI(title='Request & Matching Service')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
registry_db = get_registry_db()
DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), 'dashboard.html')

@app.get('/', response_class=HTMLResponse)
def dashboard():
    with open(DASHBOARD_PATH, 'r', encoding='utf-8') as f:
        return f.read()

@app.post('/request_blood')
def request_blood(req: SearchRequest):
    if req.urgency == 'emergency':
        try:
            publish_urgent_request(req.model_dump())
        except Exception as exc:
            print(f'[request_service] could not publish to queue (is Redis running?): {exc}')
    online_rows = registry_db.execute("SELECT node_name FROM node_registry WHERE status = 'online'").fetchall()
    online_nodes = [row['node_name'] for row in online_rows]
    coordinator = _get_known_coordinator()
    if coordinator and coordinator in online_nodes:
        candidates = [coordinator] + [n for n in online_nodes if n != coordinator]
    else:
        candidates = online_nodes or list(NODES.keys())
    for name in candidates:
        info = NODES.get(name)
        if not info:
            continue
        try:
            resp = requests.post(f"http://{info['host']}:{info['port']}/rpc/search_donor_p2p", json=req.model_dump(), timeout=3)
            resp.raise_for_status()
            result = resp.json()
            if result.get('found'):
                return result
        except requests.exceptions.RequestException:
            continue
    return {'found': False, 'message': 'No matching donor found across any reachable node.'}

@app.get('/api/dashboard_data')
def dashboard_data():
    rows = registry_db.execute('SELECT * FROM node_registry').fetchall()
    nodes_status = [dict(r) for r in rows]
    inventory = {}
    clocks = {}
    for name, info in NODES.items():
        try:
            r = requests.get(f"http://{info['host']}:{info['port']}/inventory_count", timeout=1)
            r.raise_for_status()
            inventory[name] = r.json()
        except requests.exceptions.RequestException:
            inventory[name] = {'status': 'unreachable'}
        try:
            c = requests.get(f"http://{info['host']}:{info['port']}/clocks", timeout=1)
            c.raise_for_status()
            clocks[name] = c.json()
        except requests.exceptions.RequestException:
            clocks[name] = {'status': 'unreachable'}
    return {'nodes': nodes_status, 'inventory': inventory, 'clocks': clocks, 'coordinator': _get_known_coordinator()}

@app.websocket('/ws/live_inventory')
async def live_inventory(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(dashboard_data())
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass

async def staleness_monitor():
    while True:
        await asyncio.sleep(5)
        cutoff = time.time() - STALE_THRESHOLD_SECONDS
        try:
            registry_db.execute("UPDATE node_registry SET status = 'offline' WHERE last_heartbeat < ? AND status = 'online'", (cutoff,))
            registry_db.commit()
        except Exception as exc:
            print(f'[request_service] staleness monitor error: {exc}')

@app.on_event('startup')
async def startup_event():
    asyncio.create_task(staleness_monitor())

def _get_known_coordinator() -> str | None:
    row = registry_db.execute('SELECT coordinator FROM coordinator_state WHERE id = 1').fetchone()
    return row['coordinator'] if row else None

@app.get('/election/status')
def election_status_aggregate():
    beliefs = {}
    for name, info in NODES.items():
        try:
            r = requests.get(f"http://{info['host']}:{info['port']}/election/status", timeout=2)
            r.raise_for_status()
            beliefs[name] = r.json().get('believes_coordinator_is')
        except requests.exceptions.RequestException:
            beliefs[name] = 'unreachable'
    return {'shared_registry_coordinator': _get_known_coordinator(), 'per_node_belief': beliefs}

@app.post('/election/trigger')
def election_trigger(payload: dict | None=None):
    starter = (payload or {}).get('from_node')
    online_rows = registry_db.execute("SELECT node_name FROM node_registry WHERE status = 'online'").fetchall()
    online = [r['node_name'] for r in online_rows] or list(NODES.keys())
    if not starter or starter not in online:
        starter = online[0]
    info = NODES[starter]
    try:
        r = requests.post(f"http://{info['host']}:{info['port']}/election/start", timeout=10)
        r.raise_for_status()
        return {'triggered_from': starter, 'result': r.json()}
    except requests.exceptions.RequestException as exc:
        return {'error': str(exc)}

def _find_owning_node(donor_id: str) -> str | None:
    for name, info in NODES.items():
        try:
            r = requests.get(f"http://{info['host']}:{info['port']}/objects/donor/{donor_id}", timeout=2)
            data = r.json()
            if 'error' not in data:
                return name
        except requests.exceptions.RequestException:
            continue
    return None

@app.post('/mutex/reserve')
def mutex_reserve(payload: dict):
    donor_id = payload['donor_id']
    units = payload.get('units', 1)
    node_name = payload.get('node_name') or _find_owning_node(donor_id)
    if not node_name:
        return {'reserved': False, 'reason': 'donor_id not found on any node'}
    info = NODES[node_name]
    try:
        r = requests.post(f"http://{info['host']}:{info['port']}/mutex/acquire_and_reserve", json={'donor_id': donor_id, 'units': units}, timeout=15)
        r.raise_for_status()
        result = r.json()
    except requests.exceptions.RequestException as exc:
        return {'reserved': False, 'reason': f'node unreachable: {exc}'}
    if result.get('reserved'):
        _append_ledger_and_notify(method='mutual_exclusion_protected', donor_id=donor_id, node_name=node_name, units=units)
    return result

@app.get('/mutex/status_all')
def mutex_status_all():
    statuses = {}
    for name, info in NODES.items():
        try:
            r = requests.get(f"http://{info['host']}:{info['port']}/mutex/status", timeout=2)
            r.raise_for_status()
            statuses[name] = r.json()
        except requests.exceptions.RequestException:
            statuses[name] = {'status': 'unreachable'}
    return statuses

@app.post('/objects/reserve')
def objects_reserve(payload: dict):
    donor_id = payload['donor_id']
    units = payload.get('units', 1)
    node_name = payload.get('node_name') or _find_owning_node(donor_id)
    if not node_name:
        return {'success': False, 'reason': 'donor_id not found on any node'}
    proxy = DonorRecordProxy(donor_id, node_name, NODES)
    try:
        result = proxy.reserve(units)
    except requests.exceptions.RequestException as exc:
        return {'success': False, 'reason': f'node unreachable: {exc}'}
    if result.get('success'):
        _append_ledger_and_notify(method='object_based_unprotected', donor_id=donor_id, node_name=node_name, units=units)
    return result

@app.post('/snapshot/trigger')
def snapshot_trigger():
    local_snapshots = {}
    for name, info in NODES.items():
        try:
            r = requests.get(f"http://{info['host']}:{info['port']}/snapshot/local", timeout=5)
            r.raise_for_status()
            local_snapshots[name] = r.json()
        except requests.exceptions.RequestException:
            local_snapshots[name] = {'error': 'node unreachable at snapshot time'}
    totals: dict[str, int] = {}
    for snap in local_snapshots.values():
        for donor in snap.get('donor_inventory', []):
            bg = donor['blood_group']
            totals[bg] = totals.get(bg, 0) + donor['units_available']
    return {'taken_at': time.time(), 'per_node_snapshot': local_snapshots, 'global_inventory_totals': totals}

@app.get('/deadlock/simulate')
def deadlock_simulate():
    return run_demo_scenarios()

def _append_ledger_and_notify(method: str, donor_id: str, node_name: str, units: int):
    record = {'method': method, 'donor_id': donor_id, 'node': node_name, 'units': units, 'recorded_at': time.time()}
    try:
        add_block(registry_db, record)
    except Exception as exc:
        print(f'[request_service] ledger append failed: {exc}')
    try:
        requests.post(f'{SERVERLESS_GATEWAY_URL}/invoke/notify', json={'event_type': 'donor_match_found', 'donor_id': donor_id, 'city': node_name.title(), 'blood_group': None, 'units': units, 'hospital': 'Requesting Hospital'}, timeout=5)
    except requests.exceptions.RequestException as exc:
        print(f'[request_service] serverless gateway unreachable (is it running on 8095?): {exc}')

@app.get('/ledger/chain')
def ledger_chain():
    return get_chain(registry_db)

@app.get('/ledger/validate')
def ledger_validate():
    return is_chain_valid(registry_db)

@app.post('/dfs/upload')
async def dfs_upload(file: UploadFile=File(...)):
    data = await file.read()
    file_id = new_file_id()
    chunks = chunk_bytes(data)
    node_names = list(NODES.keys())
    chunk_map = build_chunk_map(len(chunks), node_names, DFS_REPLICATION_FACTOR)
    for idx, chunk in enumerate(chunks):
        for node_name in chunk_map[idx]:
            info = NODES[node_name]
            try:
                requests.post(f"http://{info['host']}:{info['port']}/dfs/store_chunk", params={'file_id': file_id, 'chunk_index': idx}, data=chunk, timeout=10)
            except requests.exceptions.RequestException as exc:
                print(f'[request_service] failed storing chunk {idx} on {node_name}: {exc}')
    import json as _json
    registry_db.execute('\n        INSERT INTO dfs_manifest (file_id, filename, total_chunks, replication_factor, chunk_map, uploaded_at)\n        VALUES (?, ?, ?, ?, ?, ?)\n        ', (file_id, file.filename, len(chunks), DFS_REPLICATION_FACTOR, _json.dumps(chunk_map), time.time()))
    registry_db.commit()
    return {'file_id': file_id, 'filename': file.filename, 'total_chunks': len(chunks), 'replication_factor': DFS_REPLICATION_FACTOR, 'size_bytes': len(data)}

@app.get('/dfs/files')
def dfs_files():
    rows = registry_db.execute('SELECT file_id, filename, total_chunks, replication_factor, uploaded_at FROM dfs_manifest ORDER BY uploaded_at DESC').fetchall()
    return [dict(r) for r in rows]

@app.get('/dfs/download/{file_id}')
def dfs_download(file_id: str):
    import json as _json
    row = registry_db.execute('SELECT * FROM dfs_manifest WHERE file_id = ?', (file_id,)).fetchone()
    if not row:
        return Response(status_code=404, content=b'file_id not found')
    chunk_map = _json.loads(row['chunk_map'])
    total_chunks = row['total_chunks']
    reconstructed = bytearray()
    for idx in range(total_chunks):
        replicas = chunk_map[str(idx)]
        chunk_bytes_data = None
        for node_name in replicas:
            info = NODES[node_name]
            try:
                r = requests.get(f"http://{info['host']}:{info['port']}/dfs/fetch_chunk", params={'file_id': file_id, 'chunk_index': idx}, timeout=10)
                if r.status_code == 200:
                    chunk_bytes_data = r.content
                    break
            except requests.exceptions.RequestException:
                continue
        if chunk_bytes_data is None:
            return Response(status_code=500, content=f'chunk {idx} unavailable on every replica'.encode())
        reconstructed.extend(chunk_bytes_data)
    return Response(content=bytes(reconstructed), media_type='application/octet-stream', headers={'Content-Disposition': f'''attachment; filename="{row['filename']}"'''})

@app.post('/hdfs/upload')
async def hdfs_upload(file: UploadFile=File(...)):
    data = await file.read()
    fd, local_tmp_path = tempfile.mkstemp(prefix='hdfs_ul_')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
        hdfs_path = hdfs_upload_file(local_tmp_path, file.filename)
    except HdfsError as exc:
        return {'uploaded': False, 'error': str(exc), 'hint': 'Is Hadoop running? Check with `jps` — you should see NameNode and DataNode.'}
    finally:
        if os.path.exists(local_tmp_path):
            os.remove(local_tmp_path)
    return {'uploaded': True, 'filename': file.filename, 'hdfs_path': hdfs_path, 'size_bytes': len(data)}

@app.get('/hdfs/files')
def hdfs_files():
    try:
        entries = hdfs_list_files()
    except HdfsError as exc:
        return {'error': str(exc)}
    return [{'name': e.name, 'size_bytes': e.size_bytes, 'replication': e.replication, 'hdfs_path': e.hdfs_path} for e in entries]

@app.get('/hdfs/download/{filename}')
def hdfs_download(filename: str):
    try:
        local_path = hdfs_download_to_tempfile(filename)
    except HdfsError as exc:
        return Response(status_code=502, content=str(exc).encode())
    try:
        with open(local_path, 'rb') as f:
            data = f.read()
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
    return Response(content=data, media_type='application/octet-stream', headers={'Content-Disposition': f'attachment; filename="{filename}"'})

@app.get('/hdfs/status')
def hdfs_status():
    try:
        return hdfs_cluster_status()
    except HdfsError as exc:
        return {'reachable': False, 'error': str(exc), 'hint': "Run `jps` in a terminal — you should see NameNode, DataNode and SecondaryNameNode. If they're missing, start them with start-dfs.sh."}
