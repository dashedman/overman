from enum import IntEnum
from typing import Generator, Any, TypeVar, Iterable
from dataclasses import dataclass, field
from collections import deque

from tqdm import tqdm

INF = 1000000000


T = TypeVar('T')
S = TypeVar('S')


class VisitStatus(IntEnum):
    NotVisited = 0
    InProcessing = 1
    Visited = 2


@dataclass
class Edge:
    next_node_index: int
    val: float = field(default=1.0)
    inversed: bool = field(default=False)


@dataclass
class GraphNode:
    index: int
    # index, edge value
    edges: list[Edge]
    value: Any

    def __eq__(self, other: 'GraphNode'):
        return self.value == other.value


@dataclass
class Graph:
    nodes: list[GraphNode]
    __names_to_index: dict[str, int] = None
    __need_update: bool = True

    def __getitem__(self, item):
        return self.nodes[item]

    def __delitem__(self, key):
        self.nodes.pop(key)
        for node in self.nodes:
            edges_to_del = []
            edges_to_decrease = []
            for index, edge_tail in enumerate(node.edges):
                if edge_tail.next_node_index == key:
                    edges_to_del.append(index)
                elif edge_tail.next_node_index > key:
                    edges_to_decrease.append(index)

            for index in edges_to_decrease:
                node.edges[index].next_node_index -= 1
            for index in reversed(edges_to_del):
                node.edges.pop(index)

            if node.index > key:
                node.index -= 1

        self.__need_update = True

    def __len__(self):
        return len(self.nodes)

    def __iter__(self):
        return iter(self.nodes)

    def get_index_for_coin_name(self, coin: str):
        if self.__need_update:
            self.__names_to_index = {}
            for index, node in enumerate(self):
                self.__names_to_index[node.value] = index
            self.__need_update = False

        return self.__names_to_index[coin]

    def get_node_for_coin(self, coin: str):
        return self.nodes[self.get_index_for_coin_name(coin)]

    def restore_cycle(
            self,
            head_index,
            tail_index,
            visit_from: list[int],
            edge_from: list[Edge],
            last_edge: Edge,
    ) -> deque[tuple[GraphNode, Edge]]:
        cycle = deque()
        curr_index = tail_index
        curr_edge = last_edge
        # unwinding cycle
        while curr_index != head_index:
            cycle.appendleft((self.nodes[curr_index], curr_edge))
            curr_edge = edge_from[curr_index]
            curr_index = visit_from[curr_index]
        cycle.appendleft((self.nodes[head_index], curr_edge))
        return cycle

    def get_cycles(
            self,
            start: int = 0,
            with_start: bool = False,
            max_length: int = None,
            prevent_short_cycles: bool = True,
    ) -> Generator[deque[tuple[GraphNode, Edge]], None, None]:
        visited: list[VisitStatus] = [VisitStatus.NotVisited] * len(self)
        visit_from: list[int] = [-1] * len(self)
        edge_from: list[Edge | None] = [None] * len(self)

        def bfs_search(curr_index, prev_index, prev_edge, curr_depth):
            if visited[curr_index] == VisitStatus.Visited:
                return
            if with_start and max_length is not None and curr_depth > max_length:
                return
            if visited[curr_index] == VisitStatus.InProcessing:
                if not with_start or curr_index == start:
                    yield self.restore_cycle(curr_index, prev_index, visit_from, edge_from, prev_edge)
                return

            visited[curr_index] = VisitStatus.InProcessing
            visit_from[curr_index] = prev_index
            edge_from[curr_index] = prev_edge
            for next_edge in self.nodes[curr_index].edges:
                if prevent_short_cycles and next_edge.next_node_index == prev_index:
                    # detect cycle with length equals to 2
                    continue
                yield from bfs_search(
                    next_edge.next_node_index,
                    curr_index,
                    next_edge,
                    curr_depth + 1,
                )

            visited[curr_index] = VisitStatus.NotVisited

        yield from bfs_search(start, -1, None, 1)

    # TODO: get_cycles_with_pair
    # TODO: get_cycles_from_node_with_pair
    # TODO: filter graph from nodes without cycles with base coins
    def filter_from_noncycle_nodes(self, base_nodes: list[GraphNode]):
        checked = [False] * len(self)
        for base_node in base_nodes:
            cycle_generator = tqdm(
                self.get_cycles(start=base_node.index, with_start=True, max_length=5),
                desc=base_node.value,
                ascii=True,
                unit=' cycles',
            )
            cycle_generator: Iterable[deque[tuple[GraphNode, Edge]]]

            for cycle in cycle_generator:
                for node, edge in cycle:
                    checked[node.index] = True

        del_count = 0
        for index, good in enumerate(checked):
            if not good:
                del self[index - del_count]
                del_count += 1

    def print_pairs(self):
        for node in self:
            for edge in node.edges:
                print(node.value, self[edge.next_node_index].value, sep='>')


if __name__ == '__main__':

    # example
    # 8 8
    # 0 4 1
    # 1 4 2
    # 2 1 3
    # 5 2 4
    # 4 5 5
    # 4 6 6
    # 7 5 7
    # 6 7 8

    n, m = map(int, input().split(' '))
    raw_edges: list[list[Edge]] = [[] for _ in range(n)]
    for _ in range(m):
        v1, v2, val = map(int, input().split(' '))
        raw_edges[v1].append(Edge(v2, val))

    test_graph = Graph([
        GraphNode(index, e, str(index)) for index, e in enumerate(raw_edges)
    ])
    for c in test_graph.get_cycles():
        for cnode, pair_val in c:
            print(cnode.value, pair_val)
        print()
