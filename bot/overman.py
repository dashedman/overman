import asyncio
import json
import logging
from collections import defaultdict, deque
from functools import cached_property
from pprint import pprint
from typing import Literal

import requests
import websockets

from bot import utils
from bot.graph import Graph, GraphNode, Edge
import dto


# logging.


class Overman:
    graph: Graph
    tickers_to_pairs: dict[str, tuple[str, str]]

    def __init__(
            self,
            pivot_coins: list[str],
            depth: Literal[1, 50],
            prefix: str
    ):
        self.depth = depth
        self.prefix = prefix
        self.pivot_coins = pivot_coins
        self.order_book_by_ticker: dict[str, set['dto.OrderBookPair']] \
            = defaultdict(set)
        self.token="2neAiuYvAU61ZDXANAGAsiL4-iAExhsBXZxftpOeh_5" \
                   "5i3Ysy2q2LEsEWU64mdzUOPusi34M_wGoSf7iNyEWJ6" \
                   "1c9cBSu34minkkRmbD8qeAA2RniAeBpNiYB9J6i9Gjsx" \
                   "UuhPw3BlrzazF6ghq4L-l3quUQN_RjW3ZkgQd2kJw=.d" \
                   "WwmgZmsM8pLO2ZqXYviZA=="

    @cached_property
    def logger(self):
        return logging.getLogger('overman')

    @cached_property
    def pivot_indexes(self):
        return [
            self.graph.get_index_for_coin_name(coin)
            for coin in self.pivot_coins
        ]

    def handle_raw_orderbook(
            self,
            raw_orderbook: dict[str, str | dict[str, str | list[str]]]
    ):
        """
        Example snapshot:

        {'topic': 'orderbook.50.DYDXUSDT', 'ts': 1687687095565, 'type': 'snapshot',
         'data': {'s': 'DYDXUSDT', 'b': [['2.001', '360.697'], ['1.971', '292.256'],
          ['1.97', '453.245'], ['1.968', '672.694'], ['1.967', '690.698'],
           ['1.966', '743.749'], ['1.963', '261.32'], ['1.962', '559.267']],
            'a': [['2.003', '506.542'], ['2.033', '574.77'], ['2.034', '424.14'],
             ['2.035', '292.429'], ['2.036', '627.188'], ['2.037', '640.978'],
              ['2.038', '399.721'], ['2.039', '732.748'], ['2.041', '631.048'],
               ['2.042', '669.269'], ['2.043', '517.712'], ['2.171', '30.765'],
                ['2.172', '38.137'], ['2.173', '35.01'], ['2.174', '58.356']],
                 'u': 19047, 'seq': 499518832}}

        Example delta:

        {'topic': 'orderbook.50.DYDXUSDT', 'ts': 1687687108145, 'type': 'delta',
         'data': {'s': 'DYDXUSDT', 'b': [['1.963', '261.32'], ['1.965', '0'],
          ['1.969', '0']], 'a': [['2.033', '554.77']], 'u': 19058, 'seq': 499518911}
          }
        """
        ob_type = raw_orderbook['type']
        ob_symbol: str = raw_orderbook['data']['s']
        raw_ask_data = raw_orderbook['data']['a']
        prepared_ask_data: set['dto.OrderBookPair'] = {
            dto.OrderBookPair(float(orderbook_el[0]), float(orderbook_el[1]))
            for orderbook_el in raw_ask_data
        }
        if ob_type == 'snapshot':
            self.order_book_by_ticker[ob_symbol] = prepared_ask_data
        else:
            for p_ask in prepared_ask_data:
                if p_ask in self.order_book_by_ticker[ob_symbol]:
                    self.order_book_by_ticker[ob_symbol].remove(p_ask)
                    if p_ask.count != 0:
                        self.order_book_by_ticker[ob_symbol].add(p_ask)
                else:
                    self.order_book_by_ticker[ob_symbol].add(p_ask)

        self.logger.info(
            f'{ob_type},'
            f' symbol: {ob_symbol},'
            f' data: {self.order_book_by_ticker[ob_symbol]}'
        )
        if self.order_book_by_ticker[ob_symbol]:
            min_pair = min(self.order_book_by_ticker[ob_symbol])
            self.update_graph(self.tickers_to_pairs[ob_symbol], min_pair.price)
            profit = self.check_profit()
            if profit:
                profit_koef, cycle = profit
                self.logger.info('Profit: %s, cycle: %s', profit_koef, cycle)

                next_cycle = cycle.copy()
                next_cycle.rotate(-1)
                for index, ((node, edge), (next_node, _)) in enumerate(zip(cycle, next_cycle), start=1):
                    print(index, node.value, edge.val, next_node.value)

    def run(self):
        asyncio.run(self.serve())

    async def serve(self):
        # init graph
        self.logger.info('Loading graph')
        pairs = self.load_graph()
        self.logger.info('Loaded %s pairs.', len(pairs))
        for base_coin, quote_coin in pairs:
            # need print, not logger
            print(base_coin, quote_coin, sep='>')
        self.tickers_to_pairs = {
            base_coin + '-' + quote_coin: (base_coin, quote_coin)
            for base_coin, quote_coin in pairs
        }

        # starting to listen sockets
        ticker_chunks = utils.chunk(self.tickers_to_pairs.keys(), 10000)
        all_subs = []
        for ticker_ch in ticker_chunks:
            sub_chunk = []
            for tick in ticker_ch:
                sub_chunk.append(f"{tick}")
            all_subs.append(sub_chunk)

        tasks = [
            asyncio.create_task(self.monitor_socket(ch))
            for ch in all_subs
        ]
        await asyncio.gather(*tasks)
        # edit graph
        # trade if graph gave a signal

    async def monitor_socket(self, subs: list[str]):
        url = f"wss://ws-api-spot.kucoin.com/?token={self.token}"
        async for sock in websockets.connect(url):
            try:
                if subs:
                    sub = self.prepare_sub(subs)
                    pairs = json.dumps(sub)
                    await sock.send(pairs)
                while True:
                    try:
                        orderbook_raw: str = await sock.recv()
                        orderbook: dict = json.loads(orderbook_raw)
                        orderbook_type = orderbook.get('type')
                        if orderbook_type:
                            ...
                            # self.handle_raw_orderbook(orderbook)
                        else:
                            pprint(orderbook)
                    except Exception as e:
                        self.logger.error(
                            'Catch error while monitoring socket:\n',
                            exc_info=e)
                        break
            except websockets.ConnectionClosed as e:
                self.logger.error('websocket error', exc_info=e)

    def update_graph(
            self,
            coins_pair: tuple[str, str],
            new_value: float,
            fee: float = 0.001
    ):
        # update pairs
        base_node = self.graph.get_node_for_coin(coins_pair[0])
        quote_node_index = self.graph.get_index_for_coin_name(coins_pair[1])
        for edge in base_node.edges:
            if edge.next_node_index == quote_node_index:
                edge.val = new_value * (1 - fee)
                break

    def check_profit(
            self,
    ) -> tuple[float, deque[tuple[GraphNode, Edge]]] | None:
        for pivot_coin_index in self.pivot_indexes:
            for cycle in self.graph.get_cycles(
                    start=pivot_coin_index,
                    with_start=True
            ):
                profit = 1
                for _, edge in cycle:
                    profit *= edge.val
                if profit > 1:
                    return profit, cycle
        return None

    def prepare_sub(self, subs_chunk: list[str]):
        return {
            "id": "test",
            "type": "subscribe",
            "topic": f"/spotMarket/level2Depth5:{','.join(subs_chunk)}",
            "response": True
        }

    def load_graph(self) -> list[tuple[str, str]]:
        # Read pairs
        url = "https://api.kucoin.com/api/v2/symbols"
        r = requests.get(url)

        instr_info = r.json()['data']

        # Filter from test coins
        base_coins: dict[str, set[str]] = defaultdict(set)
        inv_base_coins: dict[str, set[str]] = defaultdict(set)
        for pair in instr_info:
            if 'TEST' in pair['baseCurrency'] or 'TEST' in pair['quoteCurrency']:
                continue

            if pair['quoteCurrency'] in base_coins and pair['baseCurrency'] in inv_base_coins:
                continue
            base_coins[pair['baseCurrency']].add(pair['quoteCurrency'])
            inv_base_coins[pair['quoteCurrency']].add(pair['baseCurrency'])

        # build nodes from pairs
        node_keys = list(base_coins.keys())
        node_list = []
        for index, node_key in enumerate(node_keys):
            edges = []
            for tail in base_coins[node_key]:
                try:
                    edges.append(Edge(node_keys.index(tail), 0, False))
                except ValueError:
                    continue
            for tail in inv_base_coins[node_key]:
                try:
                    edges.append(Edge(node_keys.index(tail), 0, True))
                except ValueError:
                    continue
            node_list.append(GraphNode(index, edges=edges, value=node_key))

        self.graph = Graph(node_list)

        # filter to leave only cycles with base coins
        pivot_nodes = [
            node for node in node_list if node.value in self.pivot_coins
        ]
        self.graph.filter_from_noncycle_nodes(pivot_nodes)

        filtered_pairs = [
            (node.value, self.graph[edge.next_node_index].value)
            for node in self.graph
            for edge in node.edges
            if not edge.inversed
        ]
        return filtered_pairs
