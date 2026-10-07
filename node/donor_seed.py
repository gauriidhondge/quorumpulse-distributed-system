import sqlite3
SEED_DATA = {'pune': [('PUNE-D001', 'O+', 5), ('PUNE-D002', 'A+', 3), ('PUNE-D003', 'B+', 2), ('PUNE-D004', 'AB-', 1), ('PUNE-D005', 'O-', 4)], 'mumbai': [('MUM-D001', 'O+', 2), ('MUM-D002', 'A-', 3), ('MUM-D003', 'B+', 6), ('MUM-D004', 'AB-', 8), ('MUM-D005', 'O+', 8)], 'nashik': [('NSK-D001', 'O+', 1), ('NSK-D002', 'A+', 4), ('NSK-D003', 'B-', 2), ('NSK-D004', 'AB+', 3), ('NSK-D005', 'O-', 5)]}

def seed(conn: sqlite3.Connection, node_name: str) -> None:
    rows = SEED_DATA.get(node_name, [])
    for donor_id, blood_group, units in rows:
        conn.execute('\n            INSERT OR IGNORE INTO donors\n                (donor_id, blood_group, units_available, city, available)\n            VALUES (?, ?, ?, ?, 1)\n            ', (donor_id, blood_group, units, node_name.title()))
    conn.commit()
