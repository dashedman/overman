from enum import IntEnum
from typing import Generator, Sequence, Any
from dataclasses import dataclass
from collections import deque


INF = 1000000000


class VisitStatus(IntEnum):
    NotVisited = 0
    InProcessing = 1
    Visited = 2


@dataclass
class GraphNode:
    index: int
    # index, edge value
    edges: list[tuple[int, float]]
    value: Any


@dataclass
class Graph:
    nodes: list[GraphNode]

    def __delitem__(self, key):
        self.nodes.pop(key)
        for node in self.nodes:
            edges_to_del = []
            edges_to_decrease = []
            for index, (edge_tail, _) in enumerate(node.edges):
                if edge_tail == key:
                    edges_to_del.append(index)
                elif edge_tail > key:
                    edges_to_decrease.append(index)

            for index in edges_to_decrease:
                node.edges[index] = (node.edges[index][0] - 1, node.edges[index][1])
            for index in edges_to_del:
                node.edges.pop(index)

            if node.index > key:
                node.index -= 1

    def __len__(self):
        return len(self.nodes)


def get_cycles(graph: Graph, start: int = 0, with_start: bool = False) -> Generator[Sequence[GraphNode], None, None]:
    visited: list[VisitStatus] = [VisitStatus.NotVisited] * len(graph)
    visit_from: list[int] = [-1] * len(graph)

    def restore_cycle(head_index, tail_index) -> Sequence[GraphNode]:
        cycle = deque()
        curr_index = tail_index
        # unwinding cycle
        while curr_index != head_index:
            cycle.appendleft(graph.nodes[curr_index])
            curr_index = visit_from[curr_index]
        cycle.appendleft(graph.nodes[head_index])
        return cycle

    def bfs_search(curr_index, prev_index):
        if visited[curr_index] == VisitStatus.Visited:
            return
        if visited[curr_index] == VisitStatus.InProcessing:
            if not with_start or curr_index == start:
                yield restore_cycle(curr_index, prev_index)
            return

        visited[curr_index] = VisitStatus.InProcessing
        visit_from[curr_index] = prev_index
        for next_index, edge_val in graph.nodes[curr_index].edges:
            if next_index == prev_index:
                # detect cycle with length == 2
                continue
            yield from bfs_search(next_index, curr_index)

        visited[curr_index] = VisitStatus.NotVisited

    yield from bfs_search(start, -1)


# TODO: get_cycles_with_pair
# TODO: get_cycles_from_node_with_pair
# TODO: filter graph from nodes without cycles with base coins
def filter_from_noncycle_nodes(graph_inout: Graph, base_nodes: list[GraphNode]):
    checked = [False] * len(graph_inout)
    for cycle in get_cycles(graph_inout):
        for base_node in base_nodes:
            if base_node in cycle:
                break
        else:
            continue

        for index, val in enumerate(checked):
            checked[index] = val or graph_inout.nodes[index] in cycle

    del_count = 0
    for index, good in enumerate(checked):
        if not good:
            del graph_inout[index - del_count]
            del_count += 1


if __name__ == '__main__':

    # example
    # 8 8
    # 0 4
    # 1 4
    # 2 1
    # 5 2
    # 4 5
    # 4 6
    # 7 5
    # 6 7

    n, m = map(int, input().split(' '))
    raw_edges: list[list[tuple]] = [[] for _ in range(n)]
    for _ in range(m):
        v1, v2 = map(int, input().split(' '))
        raw_edges[v1].append((v2, 1))

    graph = Graph([
        GraphNode(index, e) for index, e in enumerate(raw_edges)
    ])
    for c in get_cycles(graph):
        print(c)
