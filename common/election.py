import threading
import time
import requests

class ElectionManager:

    def __init__(self, node_name: str, nodes: dict, self_url: str):
        self.node_name = node_name
        self.nodes = nodes
        self.self_url = self_url
        self.coordinator: str | None = None
        self._lock = threading.Lock()
        self._election_in_progress = False

    def _priority(self, name: str) -> int:
        return self.nodes[name]['port']

    def _higher_priority_nodes(self):
        own = self._priority(self.node_name)
        return [n for n in self.nodes if n != self.node_name and self._priority(n) > own]

    def start_election(self, registry_db=None) -> dict:
        with self._lock:
            if self._election_in_progress:
                return {'status': 'already_running'}
            self._election_in_progress = True
        try:
            higher = self._higher_priority_nodes()
            any_higher_alive = False
            for name in higher:
                info = self.nodes[name]
                try:
                    resp = requests.post(f"http://{info['host']}:{info['port']}/election/message", json={'from': self.node_name}, timeout=2)
                    if resp.status_code == 200:
                        any_higher_alive = True
                except requests.exceptions.RequestException:
                    continue
            if any_higher_alive:
                return {'status': 'deferred_to_higher_node'}
            self._announce_coordinator(self.node_name, registry_db)
            return {'status': 'became_coordinator', 'coordinator': self.node_name}
        finally:
            with self._lock:
                self._election_in_progress = False

    def _announce_coordinator(self, coordinator: str, registry_db=None):
        self.coordinator = coordinator
        if registry_db is not None:
            registry_db.execute('\n                INSERT INTO coordinator_state (id, coordinator, updated_at)\n                VALUES (1, ?, ?)\n                ON CONFLICT(id) DO UPDATE SET coordinator = excluded.coordinator,\n                                               updated_at = excluded.updated_at\n                ', (coordinator, time.time()))
            registry_db.commit()
        for name, info in self.nodes.items():
            if name == self.node_name:
                continue
            try:
                requests.post(f"http://{info['host']}:{info['port']}/election/coordinator", json={'coordinator': coordinator}, timeout=2)
            except requests.exceptions.RequestException:
                continue

    def receive_election_message(self, from_node: str, registry_db=None):
        threading.Thread(target=self.start_election, args=(registry_db,), daemon=True).start()
        return {'status': 'ok_i_am_higher'}

    def receive_coordinator_announcement(self, coordinator: str, registry_db=None):
        self.coordinator = coordinator
        if registry_db is not None:
            registry_db.execute('\n                INSERT INTO coordinator_state (id, coordinator, updated_at)\n                VALUES (1, ?, ?)\n                ON CONFLICT(id) DO UPDATE SET coordinator = excluded.coordinator,\n                                               updated_at = excluded.updated_at\n                ', (coordinator, time.time()))
            registry_db.commit()
        return {'status': 'acknowledged', 'coordinator': coordinator}

    def status(self) -> dict:
        return {'node': self.node_name, 'believes_coordinator_is': self.coordinator}
