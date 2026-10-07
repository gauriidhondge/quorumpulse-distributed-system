import threading
import requests
from common.lamport_clock import LamportClock

class MutexManager:

    def __init__(self, node_name: str, nodes: dict, clock: LamportClock):
        self.node_name = node_name
        self.nodes = nodes
        self.clock = clock
        self.state = 'RELEASED'
        self.request_ts: int | None = None
        self._deferred: list[str] = []
        self._lock = threading.Lock()
        self._reply_event = threading.Event()
        self._reply_count = 0

    def _other_nodes(self):
        return [n for n in self.nodes if n != self.node_name]

    def acquire(self, timeout: float=10.0) -> dict:
        with self._lock:
            if self.state != 'RELEASED':
                return {'status': 'busy', 'detail': 'a request is already in progress on this node'}
            self.state = 'WANTED'
            self.request_ts = self.clock.tick()
            self._reply_event.clear()
            self._reply_count = 0
        others = self._other_nodes()
        if not others:
            with self._lock:
                self.state = 'HELD'
            return {'status': 'acquired', 'timestamp': self.request_ts}
        for name in others:
            info = self.nodes[name]
            try:
                requests.post(f"http://{info['host']}:{info['port']}/mutex/request", json={'from': self.node_name, 'timestamp': self.request_ts}, timeout=3)
            except requests.exceptions.RequestException:
                self._count_reply(len(others))
        acquired = self._reply_event.wait(timeout=timeout)
        with self._lock:
            if acquired or self._reply_count >= len(others):
                self.state = 'HELD'
                return {'status': 'acquired', 'timestamp': self.request_ts}
            self.state = 'RELEASED'
            return {'status': 'timeout', 'detail': 'did not receive all replies in time'}

    def _count_reply(self, needed: int):
        with self._lock:
            self._reply_count += 1
            if self._reply_count >= needed:
                self._reply_event.set()

    def on_request(self, from_node: str, from_ts: int) -> str:
        with self._lock:
            self.clock.update(from_ts)
            defer = False
            if self.state == 'HELD':
                defer = True
            elif self.state == 'WANTED':
                mine = (self.request_ts, self.node_name)
                theirs = (from_ts, from_node)
                if mine < theirs:
                    defer = True
            if defer:
                self._deferred.append(from_node)
                return 'deferred'
            return 'reply_now'

    def reply_received(self, from_node: str):
        needed = len(self._other_nodes())
        self._count_reply(needed)

    def release(self) -> dict:
        with self._lock:
            self.state = 'RELEASED'
            deferred = list(self._deferred)
            self._deferred = []
        replied_to = []
        for name in deferred:
            info = self.nodes[name]
            try:
                requests.post(f"http://{info['host']}:{info['port']}/mutex/reply", json={'from': self.node_name}, timeout=3)
                replied_to.append(name)
            except requests.exceptions.RequestException:
                continue
        return {'status': 'released', 'replied_to': replied_to}

    def status(self) -> dict:
        with self._lock:
            return {'node': self.node_name, 'state': self.state, 'request_timestamp': self.request_ts, 'pending_deferred_replies': list(self._deferred)}
