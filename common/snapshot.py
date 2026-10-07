import time

def take_local_snapshot(node_name: str, donor_db, lamport_clock, vector_clock, mutex_manager) -> dict:
    rows = donor_db.execute('SELECT donor_id, blood_group, units_available, city, available FROM donors').fetchall()
    inventory = [dict(r) for r in rows]
    return {'node': node_name, 'recorded_at': time.time(), 'donor_inventory': inventory, 'lamport_clock': lamport_clock.value, 'vector_clock': vector_clock.value, 'mutex_state': mutex_manager.status()}
