import time
from fastapi import FastAPI
from pydantic import BaseModel
from serverless.notify_function import handle_event
app = FastAPI(title='Serverless Notification Gateway (simulated FaaS)')
_last_invocation_at: float | None = None
COLD_START_WINDOW_SECONDS = 20
COLD_START_SIMULATED_DELAY = 0.6
WARM_START_SIMULATED_DELAY = 0.02

class Event(BaseModel):
    event_type: str
    donor_id: str | None = None
    city: str | None = None
    blood_group: str | None = None
    units: int | None = None
    hospital: str | None = None
    node: str | None = None

@app.post('/invoke/notify')
def invoke_notify(event: Event):
    global _last_invocation_at
    now = time.time()
    is_cold = _last_invocation_at is None or now - _last_invocation_at > COLD_START_WINDOW_SECONDS
    time.sleep(COLD_START_SIMULATED_DELAY if is_cold else WARM_START_SIMULATED_DELAY)
    _last_invocation_at = now
    result = handle_event(event.model_dump())
    result['invocation_type'] = 'cold_start' if is_cold else 'warm_start'
    result['simulated_latency_seconds'] = COLD_START_SIMULATED_DELAY if is_cold else WARM_START_SIMULATED_DELAY
    return result

@app.get('/health')
def health():
    return {'service': 'serverless-notification-gateway', 'status': 'online'}
