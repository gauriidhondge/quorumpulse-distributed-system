from datetime import datetime, timezone

def handle_event(event: dict) -> dict:
    event_type = event.get('event_type', 'unknown')
    if event_type == 'donor_match_found':
        message = f"URGENT MATCH FOUND — {event.get('blood_group', '?')} x{event.get('units', '?')} units available via donor {event.get('donor_id', '?')} in {event.get('city', '?')}. Please coordinate pickup/transport immediately."
    elif event_type == 'node_offline':
        message = f"ALERT — node '{event.get('node', '?')}' has stopped responding to heartbeats."
    else:
        message = f'Notification: {event}'
    return {'notified': True, 'channel': 'sms+email (simulated)', 'message': message, 'handled_at': datetime.now(timezone.utc).isoformat()}
