import asyncio
import json
from collections import defaultdict
from typing import Iterable

from bot.graph import Graph, GraphNode


class Overman:
    graph: Graph

    def run(self):
        asyncio.run(self.serve())

    async def serve(self):
        # init graph
        self.load_graph()
        # starting to listen socket

        # edit graph
        # trade if graph gave a signal

    def load_graph(self, start_coins: Iterable[str] = ('BTC', 'ETH', 'USDT', 'USDC')):
        # Read pairs
        with open('instruments-info.json', 'r') as f:
            instr_info = json.load(fp=f)['result']['list']

        # Filter from test coins
        base_coins: dict[str, list[str]] = defaultdict(list)
        for pair in instr_info:
            if 'TEST' in pair['baseCoin'] or 'TEST' in pair['quoteCoin']:
                continue
            base_coins[pair['baseCoin']].append(pair['quoteCoin'])

        # build nodes from pairs
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

        self.graph = Graph(node_list)

        # filter to leave only cycles with base coins
        base_nodes = [node for node in node_list if node.value in start_coins]
        self.graph.filter_from_noncycle_nodes(base_nodes)
