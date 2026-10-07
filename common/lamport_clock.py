import threading

class LamportClock:

    def __init__(self):
        self._lock = threading.Lock()
        self._counter = 0

    def tick(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter

    def update(self, received_timestamp: int) -> int:
        with self._lock:
            self._counter = max(self._counter, received_timestamp) + 1
            return self._counter

    @property
    def value(self) -> int:
        with self._lock:
            return self._counter
