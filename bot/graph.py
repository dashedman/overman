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
class PairValue:
    val: float = 1.0


@dataclass
class GraphNode:
    index: int
    # index, edge value
    edges: list[tuple[int, PairValue]]
    value: Any


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
            value_from: list[PairValue],
            last_edge_val: PairValue,
    ) -> deque[tuple[GraphNode, PairValue]]:
        cycle = deque()
        curr_index = tail_index
        curr_edge_val = last_edge_val
        # unwinding cycle
        while curr_index != head_index:
            cycle.appendleft((self.nodes[curr_index], curr_edge_val))
            curr_edge_val = value_from[curr_index]
            curr_index = visit_from[curr_index]
        cycle.appendleft((self.nodes[head_index], curr_edge_val))
        return cycle

    def get_cycles(
            self,
            start: int = 0,
            with_start: bool = False,
    ) -> Generator[deque[tuple[GraphNode, PairValue]], None, None]:
        visited: list[VisitStatus] = [VisitStatus.NotVisited] * len(self)
        visit_from: list[int] = [-1] * len(self)
        value_from: list[PairValue | None] = [None] * len(self)

        def bfs_search(curr_index, prev_index, prev_edge_val):
            if visited[curr_index] == VisitStatus.Visited:
                return
            if visited[curr_index] == VisitStatus.InProcessing:
                if not with_start or curr_index == start:
                    yield self.restore_cycle(curr_index, prev_index, visit_from, value_from, prev_edge_val)
                return

            visited[curr_index] = VisitStatus.InProcessing
            visit_from[curr_index] = prev_index
            value_from[curr_index] = prev_edge_val
            for next_index, edge_val in self.nodes[curr_index].edges:
                if next_index == prev_index:
                    # detect cycle with length equals to 2
                    continue
                yield from bfs_search(next_index, curr_index, edge_val)

            visited[curr_index] = VisitStatus.NotVisited

        yield from bfs_search(start, -1, None)

    # TODO: get_cycles_with_pair
    # TODO: get_cycles_from_node_with_pair
    # TODO: filter graph from nodes without cycles with base coins
    def filter_from_noncycle_nodes(self, base_nodes: list[GraphNode]):
        checked = [False] * len(self)
        for cycle in self.get_cycles():
            for base_node in base_nodes:
                if base_node in cycle:
                    break
            else:
                continue

            for index, val in enumerate(checked):
                checked[index] = val or self.nodes[index] in cycle

        del_count = 0
        for index, good in enumerate(checked):
            if not good:
                del self[index - del_count]
                del_count += 1


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
    raw_edges: list[list[tuple]] = [[] for _ in range(n)]
    for _ in range(m):
        v1, v2, val = map(int, input().split(' '))
        raw_edges[v1].append((v2, PairValue(val)))

    test_graph = Graph([
        GraphNode(index, e, str(index)) for index, e in enumerate(raw_edges)
    ])
    for c in test_graph.get_cycles():
        for node, pair_val in c:
            print(node.value, pair_val)
        print()
