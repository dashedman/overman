import json
from collections import defaultdict

from bot.utils import Graph, GraphNode, get_cycles, filter_from_noncycle_nodes


def main():
    with open('instruments-info.json', 'r') as f:
        instr_info = json.load(fp=f)['result']['list']

    base_coins: dict[str, list[str]] = defaultdict(list)
    for pair in instr_info:
        if 'TEST' in pair['baseCoin'] or 'TEST' in pair['quoteCoin']:
            continue
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
    print(node_keys)

    start_coins = ['BTC', 'ETH', 'USDT', 'USDC']
    base_nodes = [node for node in node_list if node.value in start_coins]

    print('before filter', len(graph))
    filter_from_noncycle_nodes(graph, base_nodes)
    print('after filter', len(graph))

    counter = 0
    for node in graph:
        for edge in node.edges:
            print(node.value, graph[edge[0]].value, sep=' ')
            counter += 1
    print(counter)

    counter = 0
    for c in get_cycles(graph):
        print(c)
        counter += 1
    print(counter)


if __name__ == '__main__':
    main()
