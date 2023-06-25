import asyncio
import json
import logging
from collections import defaultdict, deque
from functools import cached_property
from typing import Iterable, Literal

import websockets

from bot import utils
from bot.graph import Graph, GraphNode, PairValue


logging.


class Overman:
    graph: Graph
    tickers: list[str]

    def __init__(self, pivot_coins: list[str], depth: Literal[1, 50], prefix: str):
        self.depth = depth
        self.prefix = prefix
        self.pivot_coins = pivot_coins

    @cached_property
    def logger(self):
        return logging.getLogger('overman')

    @cached_property
    def pivot_indexes(self):
        return [
            self.graph.get_index_for_coin_name(coin)
            for coin in self.pivot_coins
        ]

    def run(self):
        asyncio.run(self.serve())

    async def serve(self):
        # init graph
        self.logger.info('Loading graph')
        pairs = self.load_graph()
        self.logger.info('Loaded %s pairs.', len(pairs))
        self.tickers = list(map(lambda pair: pair[0] + pair[1], pairs))

        # starting to listen sockets
        ticker_chunks = utils.chunk(self.tickers)
        all_subs = []
        for ticker_ch in ticker_chunks:
            sub_chunk = []
            for tick in ticker_ch:
                sub_chunk.append(f"{self.prefix}.{self.depth}.{tick}")
            all_subs.append(sub_chunk)

        tasks = [
            asyncio.create_task(self.monitor_socket(numb, ch))
            for numb, ch in enumerate(all_subs)
        ]
        await asyncio.gather(*tasks)
        # edit graph
        # trade if graph gave a signal

    async def monitor_socket(self, chunk_numb: int, subs: list[str]):
        url = "wss://stream-testnet.bybit.com/v5/public/spot"
        async for sock in websockets.connect(url):
            try:
                if subs:
                    sub = self.prepare_sub(subs)
                    pairs = json.dumps(sub)
                    await sock.send(pairs)
                while True:
                    try:
                        order_book_raw: str = await sock.recv()
                        order_book: dict = json.loads(order_book_raw)
                        print(f'{chunk_numb}: {order_book}')
                    except Exception as e:
                        print(e)
                        break
            except websockets.ConnectionClosed:
                ...

    def update_graph(self, coins_pair: tuple[str, str], new_value: float, fee: float = 0.001):
        # update pairs
        base_node = self.graph.get_node_for_coin(coins_pair[0])
        quote_node_index = self.graph.get_index_for_coin_name(coins_pair[1])
        for edge in base_node.edges:
            quote_index, pair_value = edge
            if quote_index == quote_node_index:
                pair_value.val = new_value * (1 - fee)
                break

    def check_profit(self) -> deque[tuple[GraphNode, PairValue]] | None:
        for pivot_coin_index in self.pivot_indexes:
            for cycle in self.graph.get_cycles(start=pivot_coin_index, with_start=True):
                profit = 1
                for _, pair_val in cycle:
                    profit *= pair_val
                if profit > 1:
                    return cycle
        return None

    def prepare_sub(self, subs_chunk: list[str]):
        return {
            "req_id": "test",
            "op": "subscribe",
            "args": subs_chunk
        }

    def load_graph(self) -> list[tuple[str, str]]:
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
                    edges.append((node_keys.index(tail), PairValue()))
                except ValueError:
                    continue
            node_list.append(GraphNode(index, edges=edges, value=node_key))

        self.graph = Graph(node_list)

        # filter to leave only cycles with base coins
        pivot_nodes = [node for node in node_list if node.value in self.pivot_coins]
        self.graph.filter_from_noncycle_nodes(pivot_nodes)

        filtered_pairs = [
            (node.value, self.graph[edge_index].value)
            for node in self.graph
            for edge_index, _ in node.edges
        ]
        return filtered_pairs
