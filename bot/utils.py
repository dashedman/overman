from copy import deepcopy
from enum import IntEnum
from typing import NewType, Generator
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


@dataclass
class Graph:
    nodes: list[GraphNode]

    def __len__(self):
        return len(self.nodes)


def get_cycles(graph: Graph) -> Generator[list[int], None, None]:
    visited: list[VisitStatus] = [VisitStatus.NotVisited] * len(graph)
    visit_from: list[int] = [-1] * len(graph)

    def restore_cycle(head_index, tail_index) -> list[int]:
        cycle = deque()
        curr_index = tail_index
        # unwinding cycle
        while curr_index != head_index:
            cycle.append(curr_index)
            curr_index = visit_from[curr_index]
        cycle.append(head_index)
        return list(reversed(cycle))

    def bfs_search(curr_index, prev_index):
        if visited[curr_index] == VisitStatus.Visited:
            return
        if visited[curr_index] == VisitStatus.InProcessing:
            yield restore_cycle(curr_index, prev_index)
            return

        visited[curr_index] = VisitStatus.InProcessing
        visit_from[curr_index] = prev_index
        for next_index, edge_val in graph.nodes[curr_index].edges:
            yield from bfs_search(next_index, curr_index)

        visited[curr_index] = VisitStatus.NotVisited

    yield from bfs_search(0, -1)


if __name__ == '__main__':

    # example
    # 8 8
    # 1 5
    # 2 5
    # 3 2
    # 6 3
    # 5 6
    # 5 7
    # 8 6
    # 7 8

    n, m = map(int, input().split(' '))
    raw_edges: list[list[tuple]] = [[] for _ in range(n)]
    for _ in range(m):
        v1, v2 = map(int, input().split(' '))
        raw_edges[v1 - 1].append((v2 - 1, 1))

    graph = Graph([
        GraphNode(index, e) for index, e in enumerate(raw_edges)
    ])
    print(graph)
    for c in get_cycles(graph):
        print(c)
