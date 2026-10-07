from typing import Dict, List, Optional

class WaitForGraph:

    def __init__(self):
        self.edges: Dict[str, List[str]] = {}

    def add_wait(self, waiting_process: str, held_by_process: str):
        self.edges.setdefault(waiting_process, [])
        if held_by_process not in self.edges[waiting_process]:
            self.edges[waiting_process].append(held_by_process)
        self.edges.setdefault(held_by_process, [])

    def neighbours(self, process: str) -> List[str]:
        return self.edges.get(process, [])

def edge_chasing_probe(graph: WaitForGraph, initiator: str) -> dict:
    visited = set()
    queue = [initiator]
    hops = []
    while queue:
        current = queue.pop(0)
        for neighbour in graph.neighbours(current):
            hops.append({'probe_sent': f'{current} -> {neighbour}'})
            if neighbour == initiator:
                return {'deadlock_detected': True, 'cycle_closed_at': neighbour, 'probe_hops': hops, 'explanation': f"Probe initiated by '{initiator}' travelled through the wait-for graph and returned to '{initiator}' itself -- this closed cycle means every process on the path is permanently blocked waiting on the next one, which is the definition of deadlock."}
            if neighbour not in visited:
                visited.add(neighbour)
                queue.append(neighbour)
    return {'deadlock_detected': False, 'probe_hops': hops, 'explanation': f"Probe initiated by '{initiator}' explored the entire reachable wait-for graph without the probe ever returning to '{initiator}' -- no cycle exists, so there is no deadlock involving this process."}

def run_demo_scenarios() -> dict:
    cyclic = WaitForGraph()
    cyclic.add_wait('pune', 'mumbai')
    cyclic.add_wait('mumbai', 'nashik')
    cyclic.add_wait('nashik', 'pune')
    acyclic = WaitForGraph()
    acyclic.add_wait('pune', 'mumbai')
    acyclic.add_wait('mumbai', 'nashik')
    return {'scenario_1_circular_wait': edge_chasing_probe(cyclic, 'pune'), 'scenario_2_simple_chain': edge_chasing_probe(acyclic, 'pune')}
if __name__ == '__main__':
    import json
    print(json.dumps(run_demo_scenarios(), indent=2))
