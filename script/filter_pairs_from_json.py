import json
from collections import defaultdict

from bot.utils import Graph, GraphNode, get_cycles


def main():
    with open('instruments-info.json', 'r') as f:
        instr_info = json.load(fp=f)['result']['list']

    base_coins: dict[str, list[str]] = defaultdict(list)
    for pair in instr_info:
        base_coins[pair['baseCoin']].append(pair['quoteCoin'])

    node_keys = list(base_coins.keys())
    node_list = []
    for index, node_key in enumerate(node_keys):
        edges = []
        for tail in base_coins[node_key]:
            try:
                edges.append((node_keys.index(tail), 1))
            except ValueError:
                continue
        node_list.append(GraphNode(index, edges=edges, value=node_key))

    graph = Graph(node_list)
    counter = 0
    for c in get_cycles(graph):
        print(c)
        counter += 1
    print(counter)


if __name__ == '__main__':
    main()
