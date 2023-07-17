import asyncio
import json
import time
from collections import defaultdict
from functools import cached_property
from pprint import pprint
from typing import Literal

import requests
import websockets
from tqdm import tqdm

import bot.logger
from bot import utils
from bot.graph import Graph, GraphNode, Edge, Cycle
import dto


class Overman:
    graph: Graph
    loop: asyncio.AbstractEventLoop
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

        self.order_book_by_ticker: dict[str, 'dto.BestOrders'] = {}
        self.__token = None
        self.is_on_trade: bool = False

    @cached_property
    def logger(self):
        return bot.logger.getLogger('overman')

    @cached_property
    def pivot_indexes(self):
        return [
            self.graph.get_index_for_coin_name(coin)
            for coin in self.pivot_coins
        ]

    @staticmethod
    def prepare_sub(subs_chunk: tuple[str]):
        return {
            "id": "test",
            "type": "subscribe",
            "topic": f"/spotMarket/level2Depth5:{','.join(subs_chunk)}",
            "response": True
        }

    @property
    def token(self):
        if self.__token is None:
            self.reload_token()
        return self.__token

    def reload_token(self):
        url = 'https://api.kucoin.com/api/v1/bullet-public'
        res = requests.post(url)
        self.__token = res.json()['data']['token']

    def run(self):
        asyncio.run(self.serve())

    async def serve(self):
        # init graph
        self.logger.info('Loading graph')
        self.loop = asyncio.get_running_loop()
        pairs = await self.load_graph()
        self.logger.info(
            'Loaded %s pairs, nodes: %s, edges: %s.',
            len(pairs), len(self.graph),
            sum(len(node.edges) for node in self.graph)
        )
        self.tickers_to_pairs = {
            base_coin + '-' + quote_coin: (base_coin, quote_coin)
            for base_coin, quote_coin in pairs
        }

        # starting to listen sockets
        # max 100 tickers per connection
        ticker_chunks = utils.chunk(self.tickers_to_pairs.keys(), 100)

        tasks = [
            asyncio.create_task(self.monitor_socket(ch))
            for ch in ticker_chunks
        ]
        tasks.append(
            asyncio.create_task(self.status_monitor())
        )
        await asyncio.gather(*tasks)
        # edit graph
        # trade if graph gave a signal

    async def monitor_socket(self, subs: tuple[str]):
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
                        if orderbook.get('code') == 401:
                            self.logger.info("Token has been expired")
                            self.reload_token()
                            self.logger.info("Token reloaded")
                        else:
                            if orderbook.get('data') is not None:
                                self.handle_raw_orderbook_data(orderbook)
                            else:
                                pprint(orderbook)
                    except Exception as e:
                        self.logger.error(
                            'Catch error while monitoring socket:\n',
                            exc_info=e)
                        break
            except websockets.ConnectionClosed as e:
                self.logger.error('websocket error', exc_info=e)

    def handle_raw_orderbook_data(
            self,
            raw_orderbook: dict[str, str | dict[str, int | list[list[str]]]]
    ):
        """
        {
             'asks': [['0.00006792', '4.9846'], ['0.00006793', '90.9062'],
            ['0.00006798', '39.9709'], ['0.00006799', '0.7342'], ['0.00006802',
            '6.8374']],
             'bids': [['0.00006781', '49.4415'], ['0.0000678',
            '2.5265'], ['0.00006771', '90.2718'], ['0.00006764', '271.9394'],
            ['0.00006758', '2.5348']],
             'timestamp': 1688157998591
         }
         "Ask" (предложение о продаже) - это цена, по которой продавец готов
         продать определенное количество акций или других ценных бумаг.

         "Bid" (предложение о покупке) - это цена, по которой покупатель готов
          купить определенное количество акций или других ценных бумаг.
        """
        ticker = raw_orderbook['topic'].split(':')[-1]
        ob_data: dict[str, list[list[str]]] = raw_orderbook['data']
        raw_ask_data = ob_data['asks']
        raw_bids_data = ob_data['bids']
        prepared_ask_data: set['dto.OrderBookPair'] = {
            dto.OrderBookPair(float(orderbook_el[0]), float(orderbook_el[1]))
            for orderbook_el in raw_ask_data
        }
        prepared_bids_data: set['dto.OrderBookPair'] = {
            dto.OrderBookPair(float(orderbook_el[0]), float(orderbook_el[1]))
            for orderbook_el in raw_bids_data
        }
        self.order_book_by_ticker[ticker] = dto.BestOrders(
            asks=prepared_ask_data,
            bids=prepared_bids_data
        )

        self.logger.debug(
            f' symbol: {ticker},'
            f' data: {self.order_book_by_ticker[ticker]}'
        )
        if self.order_book_by_ticker[ticker].is_relevant:
            self.update_graph(
                self.tickers_to_pairs[ticker],
                self.order_book_by_ticker[ticker].best_ask,
            )
            self.update_graph(
                self.tickers_to_pairs[ticker],
                self.order_book_by_ticker[ticker].best_bid,
                inverted=True,
            )
            self.trigger_trade()

    def trigger_trade(self):
        if self.is_on_trade:
            return

        self.is_on_trade = True
        graph_copy = self.graph.copy()
        asyncio.create_task(self.process_trade(graph_copy))

    async def process_trade(self, graph: Graph):

        self.logger.info('Start trade')

        start_calc = time.time()
        experimental_profit = await self.loop.run_in_executor(
            None,
            self.check_profit_experimental_3,
            graph
        )
        end_calc = time.time()
        if experimental_profit:
            profit_koef, cycle = experimental_profit
            # profit_readable = 1 / profit_koef * 100 - 100
            self.logger.info(
                'Experimental Profit: %.9f%%, validate_profit: %s, in time: %.3f, cycle: %s',
                profit_koef, cycle.get_profit(),
                end_calc - start_calc, cycle
            )

            next_cycle = cycle.copy()
            next_cycle.q.rotate(-1)
            for index, ((node, edge), (next_node, _)) in enumerate(zip(cycle, next_cycle), start=1):
                print(index, node.value, edge.val, next_node.value)

        start_calc = time.time()
        profit = await self.loop.run_in_executor(
            None,
            self.check_max_profit,
            graph,
        )
        end_calc = time.time()
        if profit:
            profit_koef, cycle = profit
            if profit_koef < 1:
                # profit_readable = 1 / profit_koef * 100 - 100
                self.logger.info(
                    'MAX Profit: %.9f%%, in time: %.3f, cycle: %s',
                    profit_koef, end_calc - start_calc, cycle
                )

                next_cycle = cycle.copy()
                next_cycle.q.rotate(-1)
                for index, ((node, edge), (next_node, _)) in enumerate(zip(cycle, next_cycle), start=1):
                    print(index, node.value, edge.val, next_node.value)

        self.logger.info('End trade')
        self.is_on_trade = False

    def update_graph(
            self,
            coins_pair: tuple[str, str],
            new_value: 'dto.OrderBookPair',
            fee: float = 0.001,
            inverted: bool = False,
    ):
        # update pairs
        if inverted:
            coins_pair = coins_pair[::-1]

        quote_node = self.graph.get_node_for_coin(coins_pair[1])
        base_node_index = self.graph.get_index_for_coin_name(coins_pair[0])
        for edge in quote_node.edges:
            if edge.next_node_index == base_node_index:

                if inverted:
                    edge.val = 1 / (new_value.price * (1 - fee))
                else:
                    edge.val = new_value.price / (1 - fee)
                break

    def check_profit(
            self,
    ) -> tuple[float, Cycle] | None:
        for pivot_coin_index in self.pivot_indexes:
            for cycle in tqdm(self.graph.get_cycles(
                    start=pivot_coin_index,
                    with_start=True,
                    max_length=5,
            ), ascii=True):
                profit = cycle.get_profit()
                if profit < 1:
                    return profit, cycle
        return None

    def check_max_profit(
            self, graph: Graph
    ) -> tuple[float, Cycle] | None:
        max_profit = 100000
        max_cycle = None
        for pivot_coin_index in self.pivot_indexes:
            for cycle in tqdm(
                    graph.get_cycles(
                    start=pivot_coin_index,
                    with_start=True,
                    max_length=4,
            ), ascii=True, disable=True):
                profit = cycle.get_profit()
                if profit != 0 and profit < max_profit:
                    max_profit = profit
                    max_cycle = cycle
        if max_cycle:
            return max_profit, max_cycle
        return None

    def check_profit_experimental(
            self,
    ) -> tuple[float, Cycle] | None:
        for pivot_coin_index in self.pivot_indexes:
            profit, cycle = self.graph.get_profit(pivot_coin_index)
            if profit < 1:
                return profit, cycle
        return None

    def check_profit_experimental_2(
            self,
    ) -> tuple[float, Cycle] | None:
        for pivot_coin_index in self.pivot_indexes:
            profit, cycle = self.graph.get_profit_2(pivot_coin_index)
            if profit < 1:
                return profit, cycle
        return None

    def check_profit_experimental_3(
            self, graph: Graph,
    ) -> tuple[float, Cycle] | None:
        for pivot_coin_index in self.pivot_indexes:
            profit, cycle = graph.get_profit_3(pivot_coin_index)
            if cycle and not cycle.validate_cycle():
                print('bad cycle')
                return None
            if profit != -1 and profit < 1:
                return profit, cycle
            elif profit != -1:
                self.logger.info('Best profit: %s for %s', profit, pivot_coin_index)
        return None

    async def load_graph(self) -> list[tuple[str, str]]:
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
        node_keys = list(set(base_coins.keys()) | set(inv_base_coins.keys()))
        node_list = []
        for index, node_key in enumerate(node_keys):
            edges = []
            for tail in base_coins[node_key]:
                try:
                    edges.append(Edge(index, node_keys.index(tail), 0, False))
                except ValueError:
                    continue
            for tail in inv_base_coins[node_key]:
                try:
                    edges.append(Edge(index, node_keys.index(tail), 0, True))
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

    async def status_monitor(self):
        while True:
            counter = 0
            index = 0
            for index, edge in enumerate(self.graph.edges, start=1):
                if edge.val != 0:
                    counter += 1
            self.logger.info(
                'Status: %s of %s edgers filled. Asyncio tasks: %s',
                counter, index, len(asyncio.all_tasks())
            )
            await asyncio.sleep(30)
