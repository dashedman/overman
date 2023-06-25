import asyncio
import json
from collections import defaultdict
from typing import Iterable, Literal

import websockets

from bot import utils
from bot.graph import Graph, GraphNode


class Overman:
    graph: Graph

    def __init__(self, tickers: list[str], depth: Literal[1, 50], prefix: str):
        self.tickers = tickers
        self.depth = depth
        self.prefix = prefix

    def run(self):
        asyncio.run(self.serve())

    async def serve(self):
        # init graph
        self.load_graph()

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

    def prepare_sub(self, subs_chunk: list[str]):
        return {
            "req_id": "test",
            "op": "subscribe",
            "args": subs_chunk
        }

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
