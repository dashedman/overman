import itertools
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
    InCycle = 3
    InBranch = 4


@dataclass
class Edge:
    origin_node_index: int
    next_node_index: int
    val: float = field(default=1.0)
    inversed: bool = field(default=False)

    def copy(self):
        return Edge(self.origin_node_index,
                    self.next_node_index,
                    self.val,
                    self.inversed)


@dataclass
class GraphNode:
    index: int
    # index, edge value
    edges: list[Edge]
    value: Any

    def __eq__(self, other: 'GraphNode'):
        return self.value == other.value

    def copy(self):
        return GraphNode(
            index=self.index,
            edges=[edge.copy() for edge in self.edges],
            value=self.value,
        )


@dataclass
class Cycle:
    q: deque[tuple[GraphNode, Edge]]

    def __iter__(self):
        return iter(self.q)

    def copy(self):
        return Cycle(self.q.copy())

    def get_profit(self):
        profit = 1
        for _, edge in self.q:
            profit *= edge.val
        return profit

    def validate_cycle(self) -> bool:
        prev_edge = self.q[-1][1]
        for node, edge in self.q:
            if prev_edge is None:
                return False
            if prev_edge.next_node_index != node.index:
                return False
            prev_edge = edge
        return True

    def fast_validate(self) -> bool:
        """ Check last edge goin to first node """
        return self.q[-1][1].next_node_index == self.q[0][0].index and self.q[0][1] is not None

    def has_node(self, node: GraphNode):
        return any(node == cycle_node for cycle_node, _ in c.q)

    def has_edge(self, edge: Edge):
        return any(edge == cycle_edge for _, cycle_edge in c.q)


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

            if node.index >= key:
                node.index -= 1

            for index in edges_to_decrease:
                node.edges[index].next_node_index -= 1
            for index in sorted(edges_to_del, reverse=True):
                node.edges.pop(index)
            for edge in node.edges:
                edge.origin_node_index = node.index

        self.__need_update = True

    def __len__(self):
        return len(self.nodes)

    def __iter__(self):
        return iter(self.nodes)

    @property
    def edges(self):
        return itertools.chain.from_iterable(
            node.edges for node in self.nodes
        )

    def copy(self) -> 'Graph':
        return Graph(nodes=[node.copy() for node in self.nodes])

    def delete_nodes(self, node_indexes_to_del: Iterable[int]):
        for index in sorted(node_indexes_to_del, reverse=True):
            del self[index]

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
    ) -> Cycle:
        cycle = deque()
        curr_index = tail_index
        curr_edge = last_edge
        # unwinding cycle
        while curr_index != head_index and curr_index != -1:
            try:
                cycle.appendleft((self.nodes[curr_index], curr_edge))
            except IndexError as e:
                print()
            curr_edge = edge_from[curr_index]
            curr_index = visit_from[curr_index]
        cycle.appendleft((self.nodes[head_index], curr_edge))
        return Cycle(cycle)

    def get_cycles(
            self,
            start: int = 0,
            with_start: bool = False,
            max_length: int = None,
            prevent_short_cycles: bool = True,
    ) -> Generator[Cycle, None, None]:
        visited: list[VisitStatus] = [VisitStatus.NotVisited] * len(self)
        visit_from: list[int] = [-1] * len(self)
        edge_from: list[Edge | None] = [None] * len(self)

        def dfs_search(
                curr_index: int,
                prev_index: int,
                prev_edge: Edge | None,
                curr_depth: int
        ):
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
                yield from dfs_search(
                    next_edge.next_node_index,
                    curr_index,
                    next_edge,
                    curr_depth + 1,
                )

            visited[curr_index] = VisitStatus.NotVisited

        yield from dfs_search(start, -1, None, 0)

    # TODO: get_cycles_with_pair
    # TODO: get_cycles_from_node_with_pair
    # TODO: filter graph from nodes without cycles with base coins
    def filter_from_noncycle_nodes(self, base_nodes: list[GraphNode]):
        nodes_in_cycle = set()

        # checked_nodes = [False] * len(self)
        for base_node in base_nodes:
            nodes_in_cycle.update(
                self.get_nodes_in_cycles(base_node.index, max_length=4)
            )

            # It's old algo to filter non cycled nodes
            # But it's stabe. So we dont erase code to test with this algo
            # cycle_generator = tqdm(
            #     self.get_cycles(start=base_node.index, with_start=True, max_length=4),
            #     desc=base_node.value,
            #     ascii=True,
            #     unit=' cycles',
            # )
            # cycle_generator: Iterable[Cycle]
            #
            # for cycle in cycle_generator:
            #     for node, _ in cycle:
            #         checked_nodes[node.index] = True

        nodes_to_del = set(range(len(self))) - nodes_in_cycle
        self.delete_nodes(nodes_to_del)

    def print_pairs(self):
        for node in self:
            for edge in node.edges:
                print(node.value, self[edge.next_node_index].value, sep='>')

    def get_profit(self, start: int) -> tuple[float, Cycle]:
        visited: list[VisitStatus] = [VisitStatus.NotVisited] * len(self)
        koef_in_node: list[float] = [0] * len(self)
        visit_from: list[int] = [-1] * len(self)
        edge_from: list[Edge | None] = [None] * len(self)

        def dfs_search(
                curr_index: int,
                prev_index: int,
                prev_edge: Edge | None,
                curr_depth: int
        ):
            if prev_edge is not None:
                new_koef = koef_in_node[prev_index] * prev_edge.val
                if new_koef > koef_in_node[curr_index]:
                    koef_in_node[curr_index] = new_koef
                    visit_from[curr_index] = prev_index
                    edge_from[curr_index] = prev_edge

            if visited[curr_index] == VisitStatus.InProcessing:
                return

            visited[curr_index] = VisitStatus.InProcessing
            for next_edge in self.nodes[curr_index].edges:
                dfs_search(
                    next_edge.next_node_index,
                    curr_index,
                    next_edge,
                    curr_depth + 1,
                )

            visited[curr_index] = VisitStatus.NotVisited

        dfs_search(start, -1, None, 1)
        return koef_in_node[start], self.restore_cycle(
            start,
            visit_from[start],
            visit_from,
            edge_from,
            edge_from[start]
        )

    def get_profit_2(self, start: int) -> tuple[float, Cycle]:
        visited: list[VisitStatus] = [VisitStatus.NotVisited] * len(self)
        koef_in_node: list[float] = [0] * len(self)
        visit_from: list[int] = [-1] * len(self)
        edge_from: list[Edge | None] = [None] * len(self)

        q = deque((start,))
        # bfs
        with tqdm(total=len(self), ascii=True) as pbar:
            pbar.update()
            while q:
                pbar.update()
                curr_index = q.popleft()
                curr_node = self.nodes[curr_index]
                curr_koef = koef_in_node[curr_index] if curr_index != start else 1

                for edge in tqdm(curr_node.edges, ascii=True, leave=False):
                    new_koef = curr_koef * edge.val
                    if new_koef > koef_in_node[edge.next_node_index]:
                        koef_in_node[edge.next_node_index] = new_koef
                        visit_from[edge.next_node_index] = edge.origin_node_index
                        edge_from[edge.next_node_index] = edge
                        if visited[edge.next_node_index] is VisitStatus.NotVisited:
                            q.append(edge.next_node_index)
                            visited[edge.next_node_index] = VisitStatus.Visited

        return koef_in_node[start], self.restore_cycle(
            start,
            visit_from[start],
            visit_from,
            edge_from,
            edge_from[start]
        )

    def get_profit_3(self, start: int) -> tuple[float, Cycle | None]:
        koef_in_node: list[float] = [100000] * len(self)
        visit_from: list[int] = [-1] * len(self)
        edge_from: list[Edge | None] = [None] * len(self)
        is_start = True

        q = deque((start,))
        # bfs
        while q:
            curr_index = q.popleft()
            curr_node = self.nodes[curr_index]
            curr_koef = 1 if is_start else koef_in_node[curr_index]
            is_start = False

            for edge in curr_node.edges:
                new_koef = curr_koef * edge.val
                if new_koef == 0:
                    continue
                elif new_koef < koef_in_node[edge.next_node_index]:
                    koef_in_node[edge.next_node_index] = new_koef
                    visit_from[edge.next_node_index] = edge.origin_node_index
                    edge_from[edge.next_node_index] = edge
                    if edge.next_node_index != start:
                        q.append(edge.next_node_index)

        if visit_from[start] == -1:
            return -1, None

        return koef_in_node[start], self.restore_cycle(
            start,
            visit_from[start],
            visit_from,
            edge_from,
            edge_from[start]
        )

    def get_nodes_in_cycles(
            self,
            start: int,
            max_length: int,
    ) -> list[int]:
        visited: list[VisitStatus] = [VisitStatus.NotVisited] * len(self)
        node_depth: list[int] = [-1] * len(self)
        left_to_cycle_end: list[int] = [100000] * len(self)

        def dfs_search(
                curr_index: int,
                curr_depth: int,
        ) -> VisitStatus:

            if curr_depth > max_length:
                return VisitStatus.NotVisited
            visit_status = visited[curr_index]

            if visit_status == VisitStatus.NotVisited:
                visited[curr_index] = VisitStatus.InBranch
            elif visit_status == VisitStatus.InCycle:
                if left_to_cycle_end[curr_index] + curr_depth > max_length:
                    return VisitStatus.NotVisited
                return VisitStatus.InCycle
            elif visit_status == VisitStatus.InBranch:
                if curr_index == start and curr_depth > 2:
                    left_to_cycle_end[curr_index] = 0
                    return VisitStatus.InCycle
                return VisitStatus.NotVisited

            node_depth[curr_index] = curr_depth
            new_statuses = []
            for next_edge in self.nodes[curr_index].edges:
                next_index = next_edge.next_node_index

                new_status = dfs_search(next_index, curr_depth + 1)
                new_statuses.append((new_status, next_index))

            in_cycle_statuses = [node for status, node in new_statuses if status == VisitStatus.InCycle]
            if in_cycle_statuses:
                nearest_cycle_node = min(in_cycle_statuses, key=lambda index: left_to_cycle_end[index])
                left_to_cycle_end[curr_index] = min(
                    left_to_cycle_end[nearest_cycle_node] + 1,
                    curr_depth
                )
                # second try with nodes not inCycle
                not_cycle = {node for status, node in new_statuses if status != VisitStatus.InCycle}
                not_cycle_edges = [e for e in self.nodes[curr_index].edges if e.next_node_index in not_cycle]
                for next_edge in not_cycle_edges:
                    next_index = next_edge.next_node_index
                    _ = dfs_search(next_index, min(curr_depth + 1, left_to_cycle_end[curr_index] + 1))

                visited[curr_index] = VisitStatus.InCycle
                return VisitStatus.InCycle

            if visited[curr_index] != VisitStatus.InBranch:
                print('WARN')
            left_to_cycle_end[curr_index] = 100000
            visited[curr_index] = VisitStatus.NotVisited
            return VisitStatus.NotVisited

        dfs_search(start, 0)
        return [
            index for index, status in enumerate(visited)
            if status == VisitStatus.InCycle
        ]


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
        raw_edges[v1].append(Edge(v1, v2, val))

    test_graph = Graph([
        GraphNode(index, e, str(index)) for index, e in enumerate(raw_edges)
    ])
    for c in test_graph.get_cycles():
        for cnode, pair_val in c:
            print(cnode.value, pair_val)
        print()
