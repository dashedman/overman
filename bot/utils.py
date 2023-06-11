from copy import deepcopy
from typing import NewType
from dataclasses import dataclass


INF = 1000000000
GraphMatrix = NewType('GraphMatrix', list[list[float]])


@dataclass
class GraphResult:
    graph_length: int
    graph_volume: int
    origin: GraphMatrix
    distances: list[list[float]]
    parents: list[list[int]]

    def __str__(self):
        def str_sq_list(sq_list: list[list[float]]) -> str:
            s = ''
            for line in sq_list:
                s += ' '.join(f'{v:.1f}' for v in line) + '\n'
            return s

        return (
            f'n: {self.graph_length}, n*n: {self.graph_volume}\n'
            f'origin:\n' + str_sq_list(self.origin) +
            f'distances:\n' + str_sq_list(self.distances) +
            f'parents:\n' + str_sq_list(self.parents)
        )

    @classmethod
    def from_graph(cls, graph: GraphMatrix):
        graph_length = len(graph)
        # matrix must be squared
        assert all(graph_length == len(row) for row in graph)

        return cls(
            graph_length,
            graph_length * graph_length,
            deepcopy(graph),
            deepcopy(graph),
            [[-1] * graph_length for _ in range(graph_length)]
        )


def graph_distances(graph: GraphMatrix) -> GraphResult:
    gr_res = GraphResult.from_graph(graph)

    # floyd_warshall O(n^3)
    for i in range(gr_res.graph_length):
        for j in range(gr_res.graph_length):
            for k in range(gr_res.graph_length):
                gr_res.distances[i][j] = min(gr_res.distances[i][j], gr_res.distances[i][k] + gr_res.distances[k][j])

    return gr_res


if __name__ == '__main__':
    graph = [
        [0, 2, 3, INF, INF],
        [2, 0, -5, INF, INF],
        [3, INF, 0, -6, INF],
        [INF, INF, INF, 0, -7],
        [4, INF, INF, INF, 0],
    ]
    print(graph_distances(graph))
