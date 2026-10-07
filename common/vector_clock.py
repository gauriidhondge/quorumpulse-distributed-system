import threading
from typing import Dict

class VectorClock:

    def __init__(self, node_name: str, all_nodes: list):
        self.node_name = node_name
        self._lock = threading.Lock()
        self._vector: Dict[str, int] = {n: 0 for n in all_nodes}

    def tick(self) -> Dict[str, int]:
        with self._lock:
            self._vector[self.node_name] += 1
            return dict(self._vector)

    def update(self, received_vector: Dict[str, int]) -> Dict[str, int]:
        with self._lock:
            for node, ts in received_vector.items():
                self._vector[node] = max(self._vector.get(node, 0), ts)
            self._vector[self.node_name] += 1
            return dict(self._vector)

    @property
    def value(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._vector)

    @staticmethod
    def happened_before(v1: Dict[str, int], v2: Dict[str, int]) -> bool:
        keys = set(v1) | set(v2)
        le_all = all((v1.get(k, 0) <= v2.get(k, 0) for k in keys))
        strictly_less = any((v1.get(k, 0) < v2.get(k, 0) for k in keys))
        return le_all and strictly_less

    @staticmethod
    def concurrent(v1: Dict[str, int], v2: Dict[str, int]) -> bool:
        return not VectorClock.happened_before(v1, v2) and (not VectorClock.happened_before(v2, v1))
