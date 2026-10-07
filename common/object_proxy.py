import requests

class DonorRecordProxy:

    def __init__(self, donor_id: str, node_name: str, nodes: dict):
        self.donor_id = donor_id
        self.node_name = node_name
        self._info = nodes[node_name]

    def _base_url(self) -> str:
        return f"http://{self._info['host']}:{self._info['port']}/objects/donor/{self.donor_id}"

    def get_details(self) -> dict:
        resp = requests.get(self._base_url(), timeout=3)
        resp.raise_for_status()
        return resp.json()

    def reserve(self, units: int) -> dict:
        resp = requests.post(f'{self._base_url()}/reserve', json={'units': units}, timeout=3)
        resp.raise_for_status()
        return resp.json()

    def release(self, units: int) -> dict:
        resp = requests.post(f'{self._base_url()}/release', json={'units': units}, timeout=3)
        resp.raise_for_status()
        return resp.json()

    def __repr__(self) -> str:
        return f'<DonorRecordProxy donor_id={self.donor_id!r} @ node={self.node_name!r}>'
