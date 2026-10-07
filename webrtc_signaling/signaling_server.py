from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import os
app = FastAPI(title='WebRTC Signaling Server')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
rooms: dict[str, list[WebSocket]] = {}
CONSULT_PATH = os.path.join(os.path.dirname(__file__), 'consult.html')

@app.get('/', response_class=HTMLResponse)
def consult_page():
    with open(CONSULT_PATH, 'r', encoding='utf-8') as f:
        return f.read()

@app.websocket('/signal/{room}')
async def signal(websocket: WebSocket, room: str):
    await websocket.accept()
    rooms.setdefault(room, []).append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            for peer in rooms.get(room, []):
                if peer is not websocket:
                    await peer.send_text(data)
    except WebSocketDisconnect:
        rooms.get(room, []).remove(websocket)
        if not rooms.get(room):
            rooms.pop(room, None)
