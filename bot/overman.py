import asyncio
import base64
import hashlib
import hmac
import random
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from itertools import chain
from typing import Literal, Any, NewType
from decimal import Decimal, ROUND_HALF_DOWN

import aiohttp as aiohttp
import asciichartpy
import orjson as orjson
import websockets

from graph_rs import EdgeRS, GraphNodeRS, GraphRS
from sortedcontainers import SortedDict

from tqdm import tqdm

from . import logger
from . import utils
from .config import Config
from .exceptions import RequestException, BalanceInsufficientError, OrderSizeTooSmallError, OrderCanceledError
from .graph import Graph, GraphNode, Edge, Cycle
import dto


BaseCoin = NewType('BaseCoin', str)
QuoteCoin = NewType('QuoteCoin', str)


@dataclass
class PairInfo:
    symbol: str
    name: str
    baseCurrency: str
    quoteCurrency: str
    feeCurrency: str
    market: str
    baseMinSize: str
    quoteMinSize: str
    baseMaxSize: str
    quoteMaxSize: str
    baseIncrement: str
    quoteIncrement: str
    priceIncrement: str
    priceLimitRate: str
    minFunds: str
    isMarginEnabled: bool
    enableTrading: bool

    @property
    def base_min_size(self):
        return Decimal(self.baseMinSize)

    @property
    def quote_min_size(self):
        return Decimal(self.quoteMinSize)

    @property
    def base_max_size(self):
        return Decimal(self.baseMaxSize)

    @property
    def quote_max_size(self):
        return Decimal(self.quoteMaxSize)

    @property
    def base_increment(self):
        return Decimal(self.baseIncrement)

    @property
    def quote_increment(self):
        return Decimal(self.quoteIncrement)

    @property
    def price_increment(self):
        return Decimal(self.priceIncrement)

    @property
    def quote_limit_rate(self):
        return Decimal(self.priceLimitRate)

    @property
    def min_funds(self):
        return Decimal(self.minFunds)


@dataclass(kw_only=True)
class TradeUnit:
    origin_coin: BaseCoin | QuoteCoin
    dest_coin: QuoteCoin | BaseCoin
    price: Decimal
    is_sell_phase: bool
    min_size: Decimal
    max_size: Decimal
    target_size: Decimal
    funds_info: Decimal | None = None

    def get_base_quote(self) -> tuple[BaseCoin, QuoteCoin]:
        if self.is_sell_phase:
            base_coin = self.origin_coin
            quote_coin = self.dest_coin
        else:
            base_coin = self.dest_coin
            quote_coin = self.origin_coin
        return base_coin, quote_coin

    def clone(self):
        return TradeUnit(
            origin_coin=self.origin_coin,
            dest_coin=self.dest_coin,
            price=self.price,
            is_sell_phase=self.is_sell_phase,
            min_size=self.min_size,
            max_size=self.max_size,
            target_size=self.target_size,
            funds_info=self.funds_info,
        )
    
    
TradeUnitList = list[TradeUnit]


@dataclass(kw_only=True)
class TradeCycleResult:
    cycle: Cycle
    profit_koef: float
    profit_time: float
    started: bool
    balance_difference: Decimal = Decimal(0)
    real_profit: float = 0
    trade_time: float = 0
    overhauls: int = None
    fail_reason: str = None
    need_to_log: bool = False

    def one_line_status(self) -> str:
        values_chain = ' -> '.join(
            chain((n.value for n, _ in self.cycle), (self.cycle[0][0].value,))
        )
        return (
            f'[{values_chain}] '
            f'profit: {self.profit_koef:.6f}, '
            f'ptime: {self.profit_time:.5f}, '
            f'started: {self.started}, '
            f'balance_difference: {self.balance_difference}'
        ) + (
            f', ttime: {self.trade_time:.4f}' if self.trade_time else ''
        ) + (
            f', overhauls: {self.overhauls}' if self.overhauls is not None else ''
        ) + (
            f', real profit: {self.real_profit}' if self.real_profit else ''
        ) + (
            f', fail: {self.fail_reason}' if self.fail_reason else ''
        )


@dataclass(kw_only=True)
class TradeSegmentResult:
    segment: TradeUnitList
    start_balance: Decimal
    end_balance: Decimal
    trade_time: float = 0


@dataclass(kw_only=True)
class ObserveUnit:
    trade_unit: TradeUnit
    order_book_history: list[dto.BestOrders]


@dataclass(kw_only=True)
class ObserveSequence:
    observes: list[ObserveUnit]


class Overman:
    graph: Graph | GraphRS
    loop: asyncio.AbstractEventLoop
    stat_counter: int = 0
    handle_sum: float = 0.0
    start_running: float = -1
    tickers_to_pairs: dict[str, tuple[str, str]]
    pairs_to_tickers: dict[tuple[str, str], str]
    pairs_info: dict[tuple[str, str], PairInfo]
    pair_to_fee: dict[tuple[str, str], Decimal]
    done_orders: dict[str, Any]
    canceled_orders: dict[str, Any]
    init_market_cache: dict[str, list[tuple[dict, int]]]
    hf_trade: bool

    def __init__(
            self,
            pivot_coins: list[str],
            depth: Literal[1, 50],
            prefix: str,
            hf_trade: bool = False,
    ):
        self.depth = depth
        self.prefix = prefix
        self.pivot_coins = pivot_coins
        self.hf_trade = hf_trade

        self.order_book_by_ticker: dict[str, dto.BestOrders | dto.FullOrders] = {}
        self.__token = None
        self.__private_token = None
        self._ping_interval = None
        self._ping_timeout = None
        self._private_ping_interval = None
        self._private_ping_timeout = None
        self.server_time_correction = 0
        self._ws_id = 0
        self.is_on_trade: bool = False
        self.was_traded = False

        try:
            self.config = Config.read_config('./config.yaml')
        except FileNotFoundError:
            self.config = Config.read_config('../config.yaml')

        self.last_profit = -1
        self.profit_life_start = 0

        self.current_balance: dict[str, Decimal] = {}
        self.done_orders = {}
        self.canceled_orders = {}
        self.init_market_cache = defaultdict(list)

        self.status_bar = tqdm(colour='green')

    @cached_property
    def session(self):
        return aiohttp.ClientSession(json_serialize=orjson.dumps)

    @cached_property
    def logger(self):
        return logger.setup_logger('main', with_root=True)

    @cached_property
    def result_logger(self):
        return logger.setup_logger('result')

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

    @staticmethod
    def prepare_market_sub(subs_chunk: tuple[str]):
        return {
            "id": "test3",
            "type": "subscribe",
            "topic": f"/market/level2:{','.join(subs_chunk)}",
            "response": True
        }

    @staticmethod
    def prepare_orders_topic():
        return {
            "id": "test2",
            "type": "subscribe",
            "topic": "/spotMarket/tradeOrders",
            "response": True,
            "privateChannel": True,
        }

    async def token(self):
        if self.__token is None:
            await self.reload_token()
        return self.__token

    async def private_token(self):
        if self.__private_token is None:
            await self.reload_private_token()
        return self.__private_token

    async def reload_token(self):
        data = await self.do_request('POST', '/api/v1/bullet-public')
        self.__token = data['token']
        self._ping_interval = data['instanceServers'][0]['pingInterval'] / 1000   # to sec
        self._ping_timeout = data['instanceServers'][0]['pingTimeout'] / 1000   # to sec

    async def reload_private_token(self):
        data = await self.do_request('POST', '/api/v1/bullet-private', private=True)
        self.__private_token = data['token']
        self._private_ping_interval = data['instanceServers'][0]['pingInterval'] / 1000   # to sec
        self._private_ping_timeout = data['instanceServers'][0]['pingTimeout'] / 1000   # to sec

    @staticmethod
    def next_uuid():
        return uuid.uuid4().int

    def next_ws_id(self):
        self._ws_id = (self._ws_id + 1) % 1000000000
        return self._ws_id

    def run(self):
        try:
            asyncio.run(self.serve(), debug=True)
        except KeyboardInterrupt:
            self.logger.info('Ended by keyboard interrupt')

    async def serve(self):
        # init graph
        self.logger.info('Loading graph')
        self.loop = asyncio.get_running_loop()

        await self.calibrate_server_time()
        await self.load_tickers()
        await self.load_fees()

        # starting to listen sockets
        # max 100 tickers per connection
        ticker_chunks = utils.chunk(self.tickers_to_pairs.keys(), 80)

        if True:
            tasks = [
                asyncio.create_task(self.market_monitor_socket(ch))
                for ch in ticker_chunks
            ]
            tasks.append(asyncio.create_task(self.fetch_order_books(4)))
        else:
            tasks = [
                asyncio.create_task(self.monitor_socket(ch))
                for ch in ticker_chunks
            ]
        tasks.append(
            asyncio.create_task(self.orders_socket())
        )
        tasks.append(
            asyncio.create_task(self.status_monitor())
        )

        self.result_logger.info('Start trading bot')
        self.start_running = time.perf_counter()
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            await self.session.close()
        self.result_logger.info('Finish trading bot')
        # edit graph
        # trade if graph gave a signal

    async def calibrate_server_time(self):
        start_time = int(time.time() * 1000)
        server_time = await self.do_request('GET', '/api/v1/timestamp')
        end_time = int(time.time() * 1000)

        my_time = (start_time + end_time) // 2
        self.server_time_correction = (server_time - my_time) // 2
        self.logger.info('Server timestamp %s, Local timestamp %s, Time Correction %s',
                         server_time, my_time, self.server_time_correction)

    async def load_tickers(self):
        pairs_raw = await self.do_request('GET', '/api/v2/symbols')

        pairs = await self.load_graph(pairs_raw)
        self.tickers_to_pairs: dict[str, tuple[BaseCoin, QuoteCoin]] = {
            base_coin + '-' + quote_coin: (base_coin, quote_coin)
            for quote_coin, base_coin in pairs
        }
        self.pairs_to_tickers: dict[tuple[BaseCoin, QuoteCoin], str] = {
            pair: ticker for ticker, pair in self.tickers_to_pairs.items()
        }
        self.pairs_info: dict[tuple[BaseCoin, QuoteCoin], PairInfo] = {
            pair: PairInfo(**pair_raw)
            for pair_raw in pairs_raw
            if (pair := self.tickers_to_pairs.get(pair_raw['symbol'], False))
        }

        self.logger.info(
            'Loaded %s pairs, nodes: %s, edges: %s.',
            len(pairs), len(self.graph),
            sum(len(node.edges) for node in self.graph.nodes)
        )

    async def load_fees(self):
        # loading fees
        # max 10 tickers per connection
        self.pair_to_fee = {}
        ticker_chunks = utils.chunk(self.tickers_to_pairs.keys(), 150)

        while True:
            self.logger.info('Trying to get first fees info.')
            try:
                data = await self.get_trade_fees(ticker_chunks[0][:1])
                if data:
                    break
            except Exception as e:
                self.logger.warning(f'Catch {e}. Trying again. Sleep 60 sec')
            await asyncio.sleep(60)

        for chunks in tqdm(ticker_chunks, postfix='fees loaded', ascii=True):
            data_chunks = await asyncio.gather(*(
                self.get_trade_fees(subchunk)
                for subchunk in utils.chunk(chunks, 10)
            ))
            data = chain.from_iterable(data_chunks)
            for data_unit in data:
                pair = self.tickers_to_pairs[data_unit['symbol']]
                self.pair_to_fee[pair] = Decimal(data_unit['takerFeeRate'])

    async def monitor_socket(self, subs: tuple[str]):
        url = f"wss://ws-api-spot.kucoin.com/?token={await self.token()}"
        async for sock in websockets.connect(url, ping_interval=None):
            try:
                last_ping = time.time()
                if subs:
                    sub = self.prepare_sub(subs)
                    pairs = orjson.dumps(sub).decode()
                    await sock.send(pairs)

                while True:
                    try:
                        orderbook_raw: str = await sock.recv()
                        orderbook: dict = orjson.loads(orderbook_raw)
                        if orderbook.get('code') == 401:
                            self.logger.info("Token has been expired")
                            await self.reload_token()
                            self.logger.info("Token reloaded")
                        else:
                            if orderbook.get('data') is not None:
                                self.handle_raw_orderbook_data(orderbook)
                            else:
                                # pprint(orderbook)
                                pass

                        if last_ping + self._ping_interval * 0.8 < time.time():
                            await sock.send(orjson.dumps({
                                'id': str(self.next_ws_id()),
                                'type': 'ping'
                            }).decode())
                            last_ping = time.time()
                    except websockets.ConnectionClosed as e:
                        self.logger.error('Catch error from websocket: %s', e, exc_info=e)
                    except Exception as e:
                        self.logger.error(
                            'Catch error while monitoring socket:\n',
                            exc_info=e)
                        break
            except websockets.ConnectionClosed as e:
                self.logger.error('websocket error: %s', e)

    async def market_monitor_socket(self, subs: tuple[str]):
        url = f"wss://ws-api-spot.kucoin.com/?token={await self.token()}"
        async for sock in websockets.connect(url, ping_interval=None):
            try:
                last_ping = time.time()
                if subs:
                    sub = self.prepare_market_sub(subs)
                    pairs = orjson.dumps(sub).decode()
                    await sock.send(pairs)

                while True:
                    try:
                        try:
                            async with asyncio.timeout(self._ping_interval * 0.2):
                                changes_raw: str = await sock.recv()
                        except TimeoutError:
                            pass
                        else:
                            changes: dict = orjson.loads(changes_raw)
                            if changes.get('code') == 401:
                                self.logger.info("Token has been expired")
                                await self.reload_token()
                                self.logger.info("Token reloaded")
                            else:
                                if 'data' in changes:
                                    self.handle_raw_changes_data(changes['data'])
                                else:
                                    # pprint(changes)
                                    pass

                        if last_ping + self._ping_interval * 0.6 < time.time():
                            await sock.send(orjson.dumps({
                                'id': str(self.next_ws_id()),
                                'type': 'ping'
                            }).decode())
                            last_ping = time.time()
                    except websockets.ConnectionClosed as e:
                        self.logger.error('Catch error from websocket: %s', e, exc_info=e)
                    except Exception as e:
                        self.logger.error(
                            'Catch error while monitoring socket:\n',
                            exc_info=e)
                        break
            except websockets.ConnectionClosed as e:
                self.logger.error('websocket error: %s', e)

    async def orders_socket(self):
        url = f"wss://ws-api-spot.kucoin.com/?token={await self.private_token()}"
        async for sock in websockets.connect(url, ping_interval=None):
            try:
                last_ping = time.time()

                subscribe_msg = self.prepare_orders_topic()
                subscribe_msg_raw = orjson.dumps(subscribe_msg).decode()
                await sock.send(subscribe_msg_raw)

                while True:
                    try:
                        try:
                            async with asyncio.timeout(self._private_ping_interval * 0.2):
                                order_result_raw: str = await sock.recv()
                        except TimeoutError:
                            pass
                        else:
                            order_result: dict = orjson.loads(order_result_raw)
                            if order_result.get('code') == 401:
                                self.logger.info("Token has been expired")
                                await self.reload_private_token()
                                self.logger.info("Token reloaded")
                            else:
                                if order_result.get('data') is not None:
                                    self.handle_order_event(order_result)
                                else:
                                    # pprint(orderbook)
                                    pass

                        if last_ping + self._private_ping_interval * 0.8 < time.time():
                            await sock.send(orjson.dumps({
                                'id': str(self.next_ws_id()),
                                'type': 'ping'
                            }).decode())
                            last_ping = time.time()
                    except websockets.ConnectionClosed as e:
                        self.logger.error('Catch error from websocket: %s', e, exc_info=e)
                    except Exception as e:
                        self.logger.error(
                            'Catch error while monitoring socket:\n',
                            exc_info=e)
                        break
            except websockets.ConnectionClosed as e:
                self.logger.error('websocket error: %s', e)

    async def clear_orders_buffers(self):
        now = time.time()
        five_min = 5 * 60

        # clear done buffer
        orders_to_del = []
        for order_id, order_data in self.done_orders.items():
            if order_data['receiveTimeSec'] + five_min < now:
                orders_to_del.append(order_id)

        for order_to_del in orders_to_del:
            self.logger.info('Del order from "done" buffer: %s', order_to_del)
            del self.done_orders[order_to_del]

        # clear canceled buffer
        orders_to_del = []
        for order_id, order_data in self.canceled_orders.items():
            if order_data['receiveTimeSec'] + five_min < now:
                orders_to_del.append(order_id)

        for order_to_del in orders_to_del:
            self.logger.info('Del order from "canceled" buffer: %s', order_to_del)
            del self.canceled_orders[order_to_del]

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
        self.stat_counter += 1

        ticker = raw_orderbook['topic'].split(':')[-1]
        ob_data: dict[str, list[list[str]]] = raw_orderbook['data']
        order_book = self.order_book_by_ticker[ticker] = \
            self.get_order_book_from_raw(ob_data)

        self.logger.debug(
            'symbol: %s, data: %s',
            ticker, self.order_book_by_ticker[ticker]
        )
        if order_book.is_relevant:
            pair = self.tickers_to_pairs[ticker]
            pair_info = self.pairs_info[pair]
            pair_fee = self.pair_to_fee[pair]
            min_funds = Decimal(pair_info.minFunds)
            min_size = Decimal(pair_info.baseMinSize)

            ask, bid = self.tune_to_size_n_funds(min_size, min_funds, order_book)
            self.update_graph(pair, ask, fee=pair_fee)
            self.update_graph(pair, bid, fee=pair_fee, inverted=True)
            self.trigger_trade()

    def handle_raw_changes_data(
            self,
            changes_data: dict[str, str | int | dict[str, list[list[str]]]]
    ):
        """
        {
            "changes": {
              "asks": [
                [
                  "18906", //price
                  "0.00331", //size
                  "14103845" //sequence
                ],
                ["18907.3", "0.58751503", "14103844"]
              ],
              "bids": [["18891.9", "0.15688", "14103847"]]
            },
            "sequenceEnd": 14103847,
            "sequenceStart": 14103844,
            "symbol": "BTC-USDT",
            "time": 1663747970273 //milliseconds
        }
         "Ask" (предложение о продаже) - это цена, по которой продавец готов
         продать определенное количество акций или других ценных бумаг.

         "Bid" (предложение о покупке) - это цена, по которой покупатель готов
          купить определенное количество акций или других ценных бумаг.
        """
        self.stat_counter += 1
        start_handle = time.perf_counter()

        ticker = changes_data['symbol']
        order_book = self.update_order_book_by_changes(
            ticker,
            changes_data['changes'],
            changes_data['sequenceEnd']
        )

        self.logger.debug(
            'symbol: %s, data: %s',
            ticker, order_book
        )
        if order_book and order_book.is_relevant:
            pair = self.tickers_to_pairs[ticker]
            pair_info = self.pairs_info[pair]
            pair_fee = self.pair_to_fee[pair]
            min_funds = Decimal(pair_info.minFunds)
            min_size = Decimal(pair_info.baseMinSize)

            ask, bid = self.tune_to_size_n_funds_full(min_size, min_funds, order_book)
            self.update_graph(pair, ask, fee=pair_fee)
            self.update_graph(pair, bid, fee=pair_fee, inverted=True)
            self.trigger_trade()

        self.handle_sum += time.perf_counter() - start_handle

    def handle_order_event(
            self,
            raw_order_result: dict[str, str | dict[str, int | float | str]]
    ):
        self.stat_counter += 1

        order_data = raw_order_result['data']

        match order_data['type'], order_data['status']:
            case 'filled', 'done':
                order_data['receiveTimeSec'] = time.time()
                self.done_orders[order_data['orderId']] = order_data
                self.logger.info('Add order to "done" buffer: %s', order_data['orderId'])

            case 'canceled', 'done':
                order_data['receiveTimeSec'] = time.time()
                self.canceled_orders[order_data['orderId']] = order_data
                self.logger.info('Add order to "canceled" buffer: %s', order_data['orderId'])
            case _:
                self.logger.info(
                    'Catch some order event: %s, %s, %s',
                    order_data['type'], order_data['status'], order_data['orderId']
                )

    @staticmethod
    def get_order_book_from_raw(ob_data):
        raw_ask_data = ob_data['asks']
        raw_bids_data = ob_data['bids']
        prepared_ask_data: list['dto.OrderBookPair'] = [
            dto.OrderBookPair(Decimal(orderbook_el[0]), Decimal(orderbook_el[1]))
            for orderbook_el in raw_ask_data
        ]
        prepared_bids_data: list['dto.OrderBookPair'] = [
            dto.OrderBookPair(Decimal(orderbook_el[0]), Decimal(orderbook_el[1]))
            for orderbook_el in raw_bids_data
        ]
        return dto.BestOrders(
            asks=prepared_ask_data,
            bids=prepared_bids_data
        )

    def update_order_book_by_changes(
            self,
            ticker: str,
            changes: dict[str, list[list[str]]],
            new_sequence_end: int,
    ) -> dto.FullOrders | None:
        order_book: dto.FullOrders | None = self.order_book_by_ticker.get(ticker)

        if order_book:
            self.apply_market_changes(order_book, changes, new_sequence_end)
            return order_book
        else:
            # cache data
            self.init_market_cache[ticker].append((changes, new_sequence_end))
            return None

    async def fetch_order_books(self, pause: int):
        for _ in range(pause):
            await asyncio.sleep(1)
            self.logger.info(
                'Wait for catching init_market_cache, %d, %d',
                len(self.init_market_cache), len(self.tickers_to_pairs)
            )
            if len(self.init_market_cache) >= len(self.tickers_to_pairs):
                break

        self.logger.info('Get full orders books for %d pairs', len(self.tickers_to_pairs))
        ticker_chunks = utils.chunk(self.tickers_to_pairs.keys(), 50)
        for tc in tqdm(ticker_chunks, postfix='Full Order books loaded', ascii=True):
            await asyncio.gather(*(self.get_full_order_book(ticker) for ticker in tc))

    async def get_full_order_book(self, symbol: str):
        data = await self.do_request(
            'GET', '/api/v3/market/orderbook/level2',
            params={'symbol': symbol},
            private=True
        )

        curr_sequence = int(data['sequence'])
        asks = SortedDict()
        for price_str, size_str in data['asks']:
            asks[Decimal(price_str)] = Decimal(size_str)

        bids = SortedDict()
        for price_str, size_str in data['bids']:
            bids[Decimal(price_str)] = Decimal(size_str)

        new_order_book = self.order_book_by_ticker[symbol] = dto.FullOrders(
            asks=asks, bids=bids, last_sequence=curr_sequence,
        )
        # self.logger.info('Apply cached market (%d) to %s', len(self.init_market_cache[symbol]), symbol)
        for changes, new_sequence_end in self.init_market_cache[symbol]:
            self.apply_market_changes(new_order_book, changes, new_sequence_end)
        del self.init_market_cache[symbol]

    @staticmethod
    def apply_market_changes(
            order_book: dto.FullOrders,
            changes: dict[str, list[list[str]]],
            new_sequence_end: int
    ):
        sequence_end = order_book.last_sequence

        for price_str, size_str, sequence_str in changes['asks']:
            sequence = int(sequence_str)
            if sequence <= sequence_end:
                continue

            price = Decimal(price_str)
            if price == 0:
                continue

            size = Decimal(size_str)
            if size == 0:
                try:
                    del order_book.asks[price]
                except KeyError:
                    pass
                continue

            order_book.asks[price] = size

        for price_str, size_str, sequence_str in changes['bids']:
            sequence = int(sequence_str)
            if sequence <= sequence_end:
                continue

            price = Decimal(price_str)
            if price == 0:
                continue

            size = Decimal(size_str)
            if size == 0:
                try:
                    del order_book.bids[price]
                except KeyError:
                    pass
                continue

            order_book.bids[price] = size

        order_book.last_sequence = max(new_sequence_end, sequence_end)

    @staticmethod
    def tune_to_funds(min_funds: Decimal, order_book: dto.BestOrders):
        ask_volume = 0
        bid_volume = 0
        virtual_ask = dto.OrderBookPair()
        virtual_bid = dto.OrderBookPair()

        for ask in order_book.asks:
            ask_volume += ask.price * ask.count
            virtual_ask.count += ask.count
            if ask_volume >= min_funds:
                virtual_ask.price = ask.price
                break

        for bid in order_book.bids:
            bid_volume += bid.price * bid.count
            virtual_bid.count += bid.count
            if bid_volume >= min_funds:
                virtual_bid.price = bid.price
                break
        return virtual_ask, virtual_bid

    @staticmethod
    def tune_to_size(size: Decimal, order_book: dto.BestOrders):
        virtual_ask = dto.OrderBookPair()
        virtual_bid = dto.OrderBookPair()

        for ask in order_book.asks:
            virtual_ask.count += ask.count
            if virtual_ask.count >= size:
                virtual_ask.price = ask.price
                break

        for bid in order_book.bids:
            virtual_bid.count += bid.count
            if virtual_bid.count >= size:
                virtual_bid.price = bid.price
                break
        return virtual_ask, virtual_bid

    @staticmethod
    def tune_to_size_n_funds(size: Decimal, min_funds: Decimal, order_book: dto.BestOrders):
        ask_volume = 0
        bid_volume = 0
        virtual_ask = dto.OrderBookPair()
        virtual_bid = dto.OrderBookPair()

        for ask in order_book.asks:
            ask_volume += ask.price * ask.count
            virtual_ask.count += ask.count
            if virtual_ask.count >= size and ask_volume >= min_funds:
                virtual_ask.price = ask.price
                break
        else:
            virtual_ask.price = order_book.asks[-1].price

        for bid in order_book.bids:
            bid_volume += bid.price * bid.count
            virtual_bid.count += bid.count
            if virtual_bid.count >= size and bid_volume >= min_funds:
                virtual_bid.price = bid.price
                break
        else:
            virtual_bid.price = order_book.bids[-1].price
        return virtual_ask, virtual_bid

    @staticmethod
    def tune_to_size_n_funds_full(
            size: Decimal,
            min_funds: Decimal,
            order_book: dto.FullOrders,
    ):
        ask_volume = 0
        bid_volume = 0
        virtual_ask = dto.OrderBookPair()
        virtual_bid = dto.OrderBookPair()

        for ask_price in order_book.asks:
            ask_count = order_book.asks[ask_price]
            ask_volume += ask_price * ask_count
            virtual_ask.count += ask_count
            if virtual_ask.count >= size and ask_volume >= min_funds:
                virtual_ask.price = ask_price
                break
        else:
            virtual_ask.price = order_book.asks.peekitem(-1)    # get max price, as worst

        for bid_price in reversed(order_book.bids):
            bid_count = order_book.bids[bid_price]
            bid_volume += bid_price * bid_count
            virtual_bid.count += bid_count
            if virtual_bid.count >= size and bid_volume >= min_funds:
                virtual_bid.price = bid_price
                break
        else:
            virtual_bid.price = order_book.bids.peekitem(0)     # get min price, as worst
        return virtual_ask, virtual_bid

    def trigger_trade(self):
        if self.is_on_trade:
            return

        self.is_on_trade = True
        start_time = time.perf_counter()
        graph_copy = self.graph.py_copy()
        asyncio.create_task(self.process_trade(graph_copy, time.perf_counter() - start_time))

    @staticmethod
    def get_time_line():
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def display(self, info, throttling: int = 1):
        if throttling == random.randint(1, throttling):
            self.status_bar.display(f'[{self.get_time_line()}|status] {info}')

    def display_f(self, f, throttling: int = 1):
        if throttling == random.randint(1, throttling):
            self.status_bar.display(f'[{self.get_time_line()}|status] {f()}')

    async def process_trade(self, graph: GraphRS, copy_time: float):
        try:
            start_calc = time.perf_counter()
            # experimental_profit = await self.loop.run_in_executor(
            #     None,
            #     self.check_profit_experimental_3,
            #     graph
            # )
            # experimental_profit = self.check_profit_experimental_3(graph)
            experimental_profit = graph.check_profit_experimental_3(self.pivot_indexes)
            end_calc = time.perf_counter()
            if experimental_profit:
                profit_koef, cycle, cpu_profit_time = experimental_profit
                profit_time = end_calc - start_calc

                # TIME PRINT
                # self.display(f'Current profit {profit_koef:.4f}, ct: {profit_time:.5f}')
                # if profit_koef == self.last_profit:
                #     pass
                # else:
                #     now = time.time()
                #     if self.last_profit < 1:
                #         self.result_logger.info('Profit %.4f, lifetime %.5f',
                #         self.last_profit, now - self.profit_life_start)
                #     self.last_profit = profit_koef
                #     self.profit_life_start = now

                # TRADE
                if profit_koef >= 1:
                    self.display(
                        f'Current profit {profit_koef:.4f}, '
                        f'ct: {profit_time:.6f}, cct {cpu_profit_time:.6f}, copy time: {copy_time:.6f}',
                        throttling=50,
                    )
                else:
                    self.display(
                        f'I want to trade! Current profit {profit_koef:.4f}, '
                        f'ct: {profit_time:.5f}, cct {cpu_profit_time:.5f}, copy time: {copy_time:.5f}',
                        throttling=10,
                    )
                    return
                    res = await self.trade_cycle(cycle, profit_koef, profit_time)
                    if res.need_to_log:
                        self.result_logger.info(res.one_line_status())
                    else:
                        self.display_f(res.one_line_status, throttling=10)

                    if res.started:
                        self.was_traded = True
                        await self.update_balance()
        finally:
            self.is_on_trade = False

    async def trade_cycle(self, cycle: Cycle, profit_koef: float, profit_time: float) -> TradeCycleResult:
        start_time = time.perf_counter()
        trade_timeout = 10 * 60  # sec

        first_quote_coin = cycle.q[0][0].value
        first_base_coin = cycle.q[1][0].value
        first_buy_price = cycle.q[0][1].original_price
        first_pair_info = self.pairs_info[(first_base_coin, first_quote_coin)]

        current_balance = self.current_balance.get(first_quote_coin, 0)
        prefer_start_funds = min(
            max(
                # start = MIN_SIZE * 3
                3 * first_pair_info.base_min_size * first_buy_price,
                # start = 5% of balance
                current_balance / 20
            ),
            current_balance
        )

        predicted_units = self.predict_cycle_parameters(
            cycle,
            prefer_start_funds=prefer_start_funds,
        )

        start_unit = predicted_units[0]
        if start_unit.min_size > start_unit.max_size:
            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=False,
                balance_difference=Decimal(0),
                fail_reason=f'Start min greater than start max: {start_unit.min_size} > {start_unit.max_size}',
                need_to_log=False,
            )

        real_start = start_unit.funds_info
        if real_start > current_balance:
            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=False,
                balance_difference=Decimal(0),
                fail_reason=f'Start balance greater than balance: {real_start} > {current_balance}',
                need_to_log=False,
            )

        there_is_dead_price = self.validate_dead_prices(predicted_units)
        if there_is_dead_price:
            trade_unit, order_pair = there_is_dead_price
            if trade_unit.is_sell_phase:
                msg = f'{trade_unit.price} < {order_pair.price}'
            else:
                msg = f'{trade_unit.price} > {order_pair.price}'
            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=False,
                balance_difference=Decimal(0),
                fail_reason=f'There is dead price: ' + msg,
                need_to_log=False,
            )

        for predict_unit in predicted_units:
            self.logger.info(
                '%s %s %s min %s max %s target %s funds %s',
                'sell' if predict_unit.is_sell_phase else 'buy',
                predict_unit.origin_coin,
                predict_unit.dest_coin,
                predict_unit.min_size,
                predict_unit.max_size,
                predict_unit.target_size,
                predict_unit.funds_info,
            )

        self.upkeep_cycle(predicted_units, timeout=trade_timeout)
        # PREDICT END
        # START SEGMENTATION
        curr_segment = None
        segments = []
        for predict_unit in predicted_units:

            is_buy_phase = not predict_unit.is_sell_phase
            if is_buy_phase:
                need_balance = predict_unit.target_size * predict_unit.price
            else:
                need_balance = predict_unit.target_size

            coin_balance = self.current_balance.get(predict_unit.origin_coin, 0)
            if coin_balance >= need_balance:
                curr_segment = []
                segments.append(curr_segment)
            curr_segment.append(predict_unit)

        self.logger.info(
            'Cycle with len %s splited for %s segments: %s',
            len(cycle), len(segments), ', '.join(str(len(s)) for s in segments)
        )
        for seg in segments:
            self.logger.warning(list(f'({unit.origin_coin} -> {unit.dest_coin})' for unit in seg))

        results = await asyncio.gather(
            *(
                self.trade_segment(seg, trade_timeout=trade_timeout)
                for seg in reversed(segments)
            ),
            return_exceptions=True
        )

        no_exceptions = True
        seg_results = []
        for result in reversed(results):
            if isinstance(result, BaseException):
                self.logger.error('Catch error from segment:', exc_info=result)
                no_exceptions = False
            else:
                seg_results.append(result)

        if no_exceptions:
            start_balance = seg_results[0].start_balance
            end_balance = seg_results[-1].end_balance

            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=True,
                real_profit=float(start_balance / end_balance),
                trade_time=time.perf_counter() - start_time,
                balance_difference=end_balance - start_balance,
                need_to_log=True,
            )
        else:
            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=True,
                balance_difference=Decimal(-1),
                trade_time=time.perf_counter() - start_time,
                fail_reason='Segments fault.',
                need_to_log=True,
            )

    def predict_cycle_parameters(
            self,
            cycle: Cycle,
            prefer_start_funds: Decimal = Decimal(0),
    ) -> list[TradeUnit]:
        pair_fee = Decimal('0.001')
        default_rounding = ROUND_HALF_DOWN
        # default_rounding = ROUND_FLOOR
        predict_results = []

        prev_phase_is_sell = False

        # Backward prediction
        last_min_sell_size = 0
        last_min_funds = 0
        last_max_sell_size = 100000000000
        last_max_funds = 100000000000
        for origin_node, edge, dest_node in cycle.iter_by_pairs_reversed():
            quote_coin = origin_node.value
            base_coin = dest_node.value
            if edge.inverted:
                fixed_pair = (quote_coin, base_coin)
            else:
                fixed_pair = (base_coin, quote_coin)

            pair_info = self.pairs_info[fixed_pair]
            # pair_fee = self.pair_to_fee[fixed_pair]
            is_sell_phase = edge.inverted

            min_size = Decimal(pair_info.baseMinSize)
            max_size = Decimal(pair_info.baseMaxSize)
            size_increment = Decimal(pair_info.baseIncrement)
            quote_increment = Decimal(pair_info.quoteIncrement)
            min_funds = Decimal(pair_info.minFunds)

            if is_sell_phase:
                if prev_phase_is_sell:
                    last_min_funds = last_min_sell_size
                    last_max_funds = last_max_sell_size

                last_min_sell_size = max(
                    min_size,
                    last_min_funds / (1 - pair_fee) / edge.original_price,
                    min_funds / edge.original_price
                ).quantize(size_increment, rounding=default_rounding)
                last_max_sell_size = min(
                    max_size,
                    last_max_funds / (1 - pair_fee) / edge.original_price,
                    edge.volume
                ).quantize(size_increment, rounding=default_rounding)
                predict_results.append(TradeUnit(
                    origin_coin=origin_node.value,
                    dest_coin=dest_node.value,
                    price=edge.original_price,
                    is_sell_phase=is_sell_phase,
                    min_size=last_min_sell_size,
                    max_size=last_max_sell_size,
                    target_size=Decimal(0),
                ))
            else:
                if not prev_phase_is_sell:
                    last_min_sell_size = last_min_funds
                    last_max_sell_size = last_max_funds

                last_min_buy_size = max(
                    min_size,
                    # last_sell_size / (1 - default_fee),
                    last_min_sell_size,
                    min_funds / edge.original_price
                ).quantize(size_increment, rounding=default_rounding)
                last_max_buy_size = min(
                    max_size,
                    # last_max_sell_size / (1 - default_fee) / edge.original_price,
                    # last_max_sell_size / edge.original_price,
                    last_max_sell_size,
                    edge.volume
                ).quantize(size_increment, rounding=default_rounding)
                predict_results.append(TradeUnit(
                    origin_coin=origin_node.value,
                    dest_coin=dest_node.value,
                    price=edge.original_price,
                    is_sell_phase=is_sell_phase,
                    min_size=last_min_buy_size,
                    max_size=last_max_buy_size,
                    target_size=Decimal(0),
                ))
                last_min_funds = (
                    last_min_buy_size * edge.original_price
                ).quantize(quote_increment, rounding=default_rounding)
                last_max_funds = (
                    last_max_buy_size * edge.original_price
                ).quantize(quote_increment, rounding=default_rounding)

            prev_phase_is_sell = is_sell_phase

        # Forward prediction
        predict_results.reverse()
        prev_phase_is_sell = True
        last_buy_size = Decimal(0)
        last_funds = prefer_start_funds
        for trade_unit in predict_results:
            quote_coin = trade_unit.origin_coin
            base_coin = trade_unit.dest_coin
            if trade_unit.is_sell_phase:
                fixed_pair = (quote_coin, base_coin)
            else:
                fixed_pair = (base_coin, quote_coin)
            pair_info = self.pairs_info[fixed_pair]
            # pair_fee = self.pair_to_fee[fixed_pair]

            size_increment = pair_info.base_increment
            quote_increment = pair_info.quote_increment

            if trade_unit.is_sell_phase:
                if prev_phase_is_sell:
                    last_buy_size = last_funds

                last_sell_size = min(
                    trade_unit.max_size,
                    max(
                        trade_unit.min_size,
                        # last_buy_size / trade_unit.price,
                        last_buy_size,
                    )
                ).quantize(size_increment, rounding=default_rounding)
                last_funds = (
                    last_sell_size * trade_unit.price * (1 - pair_fee)
                ).quantize(quote_increment, rounding=default_rounding)
                trade_unit.target_size = last_sell_size
                trade_unit.funds_info = last_funds
            else:
                if not prev_phase_is_sell:
                    last_funds = last_buy_size

                last_buy_size = min(
                    trade_unit.max_size,
                    max(
                        trade_unit.min_size,
                        last_funds / trade_unit.price
                    )
                ).quantize(size_increment, rounding=default_rounding)
                trade_unit.target_size = last_buy_size
                trade_unit.funds_info = last_funds

            prev_phase_is_sell = trade_unit.is_sell_phase

        # fix start's funds
        first_trade_unit = predict_results[0]

        quote_coin = first_trade_unit.origin_coin
        base_coin = first_trade_unit.dest_coin
        if first_trade_unit.is_sell_phase:
            fixed_pair = (quote_coin, base_coin)
        else:
            fixed_pair = (base_coin, quote_coin)
        pair_info = self.pairs_info[fixed_pair]

        first_trade_unit.funds_info = (
            first_trade_unit.target_size * first_trade_unit.price
        ).quantize(pair_info.quote_increment, default_rounding)

        return predict_results
    
    def validate_dead_prices(self, trade_units: TradeUnitList):
        there_is_dead_price = None
        for trade_unit in trade_units:
            pair = trade_unit.get_base_quote()
            ticker = self.pairs_to_tickers[pair]
            order_book = self.order_book_by_ticker[ticker]
            if isinstance(order_book, dto.FullOrders):
                ask, bid = self.tune_to_size_n_funds_full(
                    trade_unit.target_size,
                    trade_unit.funds_info,
                    order_book,
                )
            else:
                ask, bid = self.tune_to_size_n_funds(
                    trade_unit.target_size,
                    trade_unit.funds_info,
                    order_book,
                )

            if trade_unit.is_sell_phase:
                self.logger.debug(
                    '[%s -> %s] Sell %s for %s. Curr BID: (c: %s, p: %s)',
                    trade_unit.origin_coin, trade_unit.dest_coin,
                    trade_unit.target_size, trade_unit.price,
                    bid.count, bid.price,
                )
                if bid.price < trade_unit.price:
                    there_is_dead_price = (trade_unit, bid)
            else:
                self.logger.debug(
                    '[%s -> %s] Buy %s for %s. Curr ASK: (c: %s, p: %s)',
                    trade_unit.origin_coin, trade_unit.dest_coin,
                    trade_unit.target_size, trade_unit.price,
                    ask.count, ask.price,
                )
                if ask.price > trade_unit.price:
                    there_is_dead_price = (trade_unit, ask)
        return there_is_dead_price

    async def trade_segment(
            self,
            segment: TradeUnitList,
            trade_timeout: int,
    ) -> TradeSegmentResult:
        # prepare
        trade_unit = segment[0]
        start_balance = trade_unit.min_size if trade_unit.is_sell_phase else trade_unit.funds_info
        start_time = time.perf_counter()
        segment_str = ' -> '.join(
            chain((unit.origin_coin for unit in segment), (segment[-1].dest_coin,))
        )

        self.logger.info('Start segment: %s', segment_str)

        # segment rolling
        for trade_unit in segment:

            real_size = Decimal(0)
            attempts_for_trade = 10
            timeout_for_attempt = trade_timeout // attempts_for_trade

            # clone trade unit to calibrate some data when trade broken in a half
            trade_unit_clone = trade_unit.clone()
            pair_info = self.pairs_info[trade_unit_clone.get_base_quote()]
            for attempt in range(attempts_for_trade):
                try:
                    real_size = await self.trade_pair(
                        trade_unit_clone,
                        trade_timeout=timeout_for_attempt,
                    )
                except (BalanceInsufficientError, OrderCanceledError) as e:
                    if attempt < attempts_for_trade - 1:
                        self.logger.warning('Catch %s. Trying again (%s).', e, attempt)
                        if isinstance(e, OrderCanceledError):
                            # calibrate size
                            old_target_size = trade_unit_clone.target_size
                            trade_unit_clone.target_size -= e.size
                            self.logger.info(
                                'Calibrate target size %s -> %s (-%s, started: %s)',
                                old_target_size, trade_unit_clone.target_size, e.size, trade_unit.target_size
                            )

                            if trade_unit_clone.target_size < pair_info.base_min_size:
                                self.logger.warning('Calibrated target size below min required size. Do next trade..')
                                real_size = trade_unit.target_size - trade_unit_clone.target_size
                                break

                    else:
                        raise e
                else:
                    break

        self.logger.info('End segment: %s', segment_str)
        return TradeSegmentResult(
            segment=segment,
            start_balance=start_balance,
            end_balance=real_size * trade_unit.price,
            trade_time=time.perf_counter() - start_time
        )

    async def trade_pair(
            self,
            trade_unit: TradeUnit,
            trade_timeout: int = 0,
    ) -> Decimal:
        start_trade_time = time.perf_counter()
        self.logger.info(
            f'[%s -> %s] Start{" HF" if self.hf_trade else ""}. (price: %s; size: %s, funds: %s, %s)',
            trade_unit.origin_coin, trade_unit.dest_coin,
            trade_unit.price, trade_unit.target_size,
            trade_unit.price * trade_unit.target_size,
            'sell' if trade_unit.is_sell_phase else 'buy'
        )
        base_coin, quote_coin = trade_unit.get_base_quote()

        if self.hf_trade:
            order_result = await self.create_hf_order(
                base_coin=base_coin,
                quote_coin=quote_coin,
                price=str(trade_unit.price),
                size=str(trade_unit.target_size),
                is_sell=trade_unit.is_sell_phase,
                time_in_force='GTT',
                cancel_after=trade_timeout,
            )
            order_id = order_result['orderId']
            order_is_active = order_result['status'] == 'open'
        else:
            order_id = await self.create_order(
                base_coin=base_coin,
                quote_coin=quote_coin,
                price=str(trade_unit.price),
                size=str(trade_unit.target_size),
                is_sell=trade_unit.is_sell_phase,
                time_in_force='GTT',
                cancel_after=trade_timeout,
            )
            order_result = None
            order_is_active = True

        while order_is_active:

            if order_id in self.done_orders:
                order_result_ws = self.done_orders[order_id]
                order_result = {
                    'dealSize': order_result_ws['filledSize']
                }
                break
            elif order_id in self.canceled_orders:
                order_result_ws = self.canceled_orders[order_id]
                order_result = {
                    'dealSize': order_result_ws['filledSize'],
                    'cancelExist': True,
                }
                break

            self.display(
                f'[{trade_unit.origin_coin} -> {trade_unit.dest_coin}] '
                f'Order is active for now! Waiting timeout ({trade_timeout})...',
                throttling=100
            )
            await asyncio.sleep(0)
            continue
            #     order_result = await self.get_order(order_id, ticker=ticker)
            # # self.logger.info(f'{order_result}')
            # if order_result is None:
            #     self.logger.warning('Catch None from get_order(%s, %s)!', order_id, ticker)
            #     continue

            # order_is_active = order_result.get('active') or order_result.get('isActive')

            # if order_is_active:
            #     self.display(
            #         f'[{trade_unit.origin_coin} -> {trade_unit.dest_coin}] '
            #         f'Order is active for now! Waiting timeout ({trade_timeout})...',
            #         throttling=3
            #     )

        real_size = Decimal(order_result['dealSize'])

        if order_result.get('cancelExist', False):
            raise OrderCanceledError(
                f'[{trade_unit.origin_coin} -> {trade_unit.dest_coin}] '
                f'Order is canceled! Cycle Broken. ',
                real_size,
            )

        self.logger.info(
            '[%s -> %s] OK (size %s, in %.3f sec)',
            quote_coin, base_coin, real_size, time.perf_counter() - start_trade_time
        )
        return real_size

    async def create_order(
            self,
            base_coin: BaseCoin,
            quote_coin: QuoteCoin,
            price: str,
            size: str,
            is_sell: bool,
            time_in_force: Literal['GTC', 'GTT', 'IOC', 'FOK'] = 'FOK',
            cancel_after: int = 0,
    ) -> str:
        endpoint = '/api/v1/orders'

        data = await self._create_order_base(
            endpoint=endpoint,
            base_coin=base_coin,
            quote_coin=quote_coin,
            price=price,
            size=size,
            is_sell=is_sell,
            time_in_force=time_in_force,
            cancel_after=cancel_after,
        )
        return data['orderId']

    async def create_hf_order(
            self,
            base_coin: BaseCoin,
            quote_coin: QuoteCoin,
            price: str,
            size: str,
            is_sell: bool,
            time_in_force: Literal['GTC', 'GTT', 'IOC', 'FOK'] = 'FOK',
            cancel_after: int = 0,
    ) -> dict[str, str]:
        endpoint = '/api/v1/hf/orders/sync'

        data = await self._create_order_base(
            endpoint=endpoint,
            base_coin=base_coin,
            quote_coin=quote_coin,
            price=price,
            size=size,
            is_sell=is_sell,
            time_in_force=time_in_force,
            cancel_after=cancel_after,
        )
        return data

    async def _create_order_base(
            self,
            endpoint: str,
            base_coin: BaseCoin,
            quote_coin: QuoteCoin,
            price: str,
            size: str,
            is_sell: bool,
            time_in_force: Literal['GTC', 'GTT', 'IOC', 'FOK'] = 'FOK',
            cancel_after: int = 0,
    ):
        trade_side = 'sell' if is_sell else 'buy'

        try:
            ticker = self.pairs_to_tickers[(base_coin, quote_coin)]
        except KeyError:
            print()
            raise

        request_data = {
            'clientOid': str(self.next_uuid()),
            'side': trade_side,
            'symbol': ticker,
            'price': price,
            'size': size,
            # optional
            'type': 'limit',
            'tradeType': 'TRADE',
            'timeInForce': time_in_force,
        }

        if time_in_force == 'GTT':
            # Good Till Time
            request_data['cancelAfter'] = cancel_after  # sec

        data = await self.do_request(
            'POST',
            endpoint,
            data=request_data,
            private=True
        )
        return data

    async def market_order(
            self,
            base_coin: BaseCoin | QuoteCoin,
            quote_coin: QuoteCoin | BaseCoin,
            size: str
    ) -> str:
        endpoint = '/api/v1/orders'

        if ticker := self.pairs_to_tickers.get((base_coin, quote_coin)):
            trade_side = 'buy'
        elif ticker := self.pairs_to_tickers.get((quote_coin, base_coin)):
            trade_side = 'sell'
        else:
            raise Exception(f'Pair {base_coin} - {quote_coin} does not exist!')

        data = {
            'clientOid': str(self.next_uuid()),
            'side': trade_side,
            'symbol': ticker,
            # optional
            'type': 'market',
            'tradeType': 'TRADE',
        }
        if trade_side == 'buy':
            data['size'] = size
        else:
            data['size'] = size
        resp = await self.do_request(
            'POST',
            endpoint,
            data=data,
            private=True
        )
        return resp['orderId']

    async def do_request(
            self,
            method: Literal['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'HEAD', 'PATCH'],
            endpoint: str,
            data: Any = None,
            params: dict[str, str] = None,
            private: bool = False,
    ):
        if data is None:
            raw_data = b''
        else:
            raw_data = orjson.dumps(data)

        if params is None:
            raw_params = ''
        else:
            raw_params = '?' + '&'.join(
                f'{k}={v}' for k, v in params.items()
            )
        url = 'https://api.kucoin.com' + endpoint
        # url = 'https://openapi-sandbox.kucoin.com' + endpoint

        if private:
            timestamp = int(time.time() * 1000) # + self.server_time_correction  # convert to milliseconds
            request_sign = self.signature(
                timestamp, method,
                endpoint + raw_params,
                raw_data
            )

            headers = self.auth_headers(timestamp, request_sign.decode('ascii'))
        else:
            headers = {}

        request = self.session.request(
            method, url,
            params=params,
            data=raw_data,
            headers=headers,
        )
        async with request as resp:
            if resp.status != 200:
                self.logger.error('Catch %s HTTP code while %s: %s',
                                  resp.status, url, await resp.read())
                raise Exception('bad request')
            data_json = await resp.json(loads=orjson.loads)
            resp_code = int(data_json['code'])

            if resp_code == 200000:
                return data_json['data']
            if resp_code == 200004:
                raise BalanceInsufficientError(data_json['msg'])
            if resp_code == 400100:
                if 'Order size below the minimum requirement' in data_json['msg']:
                    raise OrderSizeTooSmallError(data_json['msg'])

            self.logger.error(
                'Catch %s API code while %s: %s',
                data_json['code'], url, data_json['msg']
            )
            raise RequestException(data_json['msg'])

    def signature(self, timestamp: int, method: str, endpoint: str, data: bytes):
        return base64.b64encode(
            hmac.new(
                self.config.api_secret.encode('ascii'),
                str(timestamp).encode('ascii') +
                method.encode('ascii') +
                endpoint.encode('ascii') +
                data,
                hashlib.sha256
            ).digest()
        )

    def auth_headers(self, timestamp: int, request_sign: str):
        return {
            'Content-Type': 'application/json',
            'KC-API-KEY': self.config.api_key,
            'KC-API-SIGN': request_sign,
            'KC-API-TIMESTAMP': str(timestamp),
            'KC-API-PASSPHRASE': self.config.api_passphrase,
        }

    async def get_order(self, order_id: str, ticker: str = ''):
        params = {}
        if self.hf_trade:
            endpoint = f'/api/v1/hf/orders/{order_id}'
            params['symbol'] = ticker
        else:
            endpoint = f'/api/v1/orders/{order_id}'
        data = await self.do_request(
            'GET',
            endpoint,
            params=params,
            private=True,
        )
        return data

    async def update_balance(self):
        accounts_info = await self.get_accounts_list()
        need_type = 'trade_hf' if self.hf_trade else 'trade'
        for acc_info in accounts_info:
            if acc_info['type'] != need_type:
                continue

            self.current_balance[acc_info['currency']] = Decimal(acc_info['available'])
        self.logger.info(
            'Current balance: %s',
            ' '.join(
                f'[{c}: {v:.4f}]'
                for c, v in self.current_balance.items()
                if v > 0
            )
        )

    async def get_accounts_list(self) -> list[dict[str, str]]:
        for _ in range(5):
            try:
                data = await self.do_request('GET', '/api/v1/accounts', private=True)
            except Exception as e:
                self.logger.error('Catch "%s" while updating balance. Trying again..', e)
                continue
            return data
        raise Exception('Cannot get accounts list')

    async def get_actual_orderbook(self, ticker: str):
        data = await self.do_request(
            'GET', '/api/v1/market/orderbook/level2_20',
            params={'symbol': ticker}
        )
        return self.get_order_book_from_raw(data)

    async def get_trade_fees(self, symbols: tuple[str]) -> list[dict[str, Any]]:
        return await self.do_request(
            'GET', '/api/v1/trade-fees',
            params={'symbols': ','.join(symbols)},
            private=True,
        )

    async def inner_transfer(
            self,
            currency: str,
            from_: str,
            to: str,
            amount: str,
    ) -> list[dict[str, Any]]:
        return await self.do_request(
            'POST', '/api/v2/accounts/inner-transfer',
            data={
                'clientOid': str(self.next_uuid()),
                'currency': currency,
                'from': from_,
                'to': to,
                'amount': amount,
            },
            private=True,
        )

    def update_graph(
            self,
            coins_pair: tuple[BaseCoin, QuoteCoin],
            new_value: 'dto.OrderBookPair',
            fee: Decimal = Decimal('0.001'),
            inverted: bool = False,
    ):
        # update pairs
        if inverted:
            coins_pair = coins_pair[::-1]
        base_coin, quote_coin = coins_pair

        quote_node = self.graph.get_node_for_coin(quote_coin)
        base_node_index = self.graph.get_index_for_coin_name(base_coin)
        for edge in quote_node.edges:
            if edge.next_node_index == base_node_index:

                edge.volume = new_value.count
                edge.original_price = new_value.price
                if inverted:
                    # base per quote
                    edge.val = 1 / float(new_value.price * (1 - fee))
                else:
                    # quote per base
                    edge.val = float(new_value.price / (1 - fee))
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
                profit, end_balance, start_balance = cycle.get_profit()
                if profit < 1:
                    return profit, cycle
        return None

    def check_max_profit(
            self, graph: Graph
    ) -> tuple[float, Cycle, float, float] | None:
        max_profit = 10000000
        max_cycle = None
        balance_info = (-1.0, -1.0)
        for pivot_coin_index in self.pivot_indexes:
            for cycle in tqdm(
                graph.get_cycles(
                    start=pivot_coin_index,
                    with_start=True,
                    max_length=4,
                ),
                ascii=True, disable=True
            ):
                profit, end_balance, start_balance = cycle.get_profit()
                if profit != 0 and profit < max_profit:
                    max_profit = profit
                    max_cycle = cycle
                    balance_info = (end_balance, start_balance)
        if max_cycle:
            return max_profit, max_cycle, balance_info[0], balance_info[1]
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
            self, graph: Graph
    ) -> tuple[float, Cycle, float] | None:

        start_calc = time.perf_counter()
        best_profit = 10000000
        best_cycle = None
        for pivot_coin_index in self.pivot_indexes:
            profit, cycle = graph.get_profit_3(pivot_coin_index)
            # profit, cycle = await graph.get_profit_3(pivot_coin_index)
            if cycle and not cycle.validate_cycle():
                continue
            if profit != -1 and profit < best_profit:
                best_profit = profit
                best_cycle = cycle

        if best_cycle is not None:
            return best_profit, best_cycle, time.perf_counter() - start_calc
        return None

    async def load_graph(self, instr_info) -> list[tuple[str, str]]:
        # Filter from test coins
        base_to_quotes: dict[str, set[str]] = defaultdict(set)
        quote_to_bases: dict[str, set[str]] = defaultdict(set)
        for pair in instr_info:
            if 'minFunds' not in pair or pair['minFunds'] is None:
                continue
            if 'TEST' in pair['baseCurrency'] or 'TEST' in pair['quoteCurrency']:
                continue

            base_to_quotes[pair['baseCurrency']].add(pair['quoteCurrency'])
            quote_to_bases[pair['quoteCurrency']].add(pair['baseCurrency'])

        # build nodes from pairs
        node_keys = list(set(base_to_quotes.keys()) | set(quote_to_bases.keys()))
        node_list = []
        for index, node_key in enumerate(node_keys):
            edges = []
            for tail in quote_to_bases[node_key]:
                try:
                    # edges.append(Edge(index, node_keys.index(tail), 0, Decimal(0.0), False))
                    edges.append(EdgeRS(index, node_keys.index(tail), 0, 0.0, False))
                except ValueError:
                    continue
            for tail in base_to_quotes[node_key]:
                try:
                    # edges.append(Edge(index, node_keys.index(tail), 0, Decimal(0.0), True))
                    edges.append(EdgeRS(index, node_keys.index(tail), 0, 0.0, True))
                except ValueError:
                    continue

            # node_list.append(GraphNode(index, edges=edges, value=node_key))
            node_list.append(GraphNodeRS(index, edges=edges, value=node_key))

        # self.graph = Graph(node_list)
        self.graph = GraphRS(node_list)

        # filter to leave only cycles with base coins
        pivot_nodes = [
            node for node in node_list if node.value in self.pivot_coins
        ]
        self.graph.filter_from_noncycle_nodes(pivot_nodes)

        filtered_pairs = [
            (node.value, self.graph[edge.next_node_index].value)
            for node in self.graph.nodes
            for edge in node.edges
            if not edge.inverted
        ]
        return filtered_pairs

    async def status_monitor(self):
        while True:
            self.logger.info(
                'Asyncio tasks: %d. %.3f rps. %.6f spr',
                len(asyncio.all_tasks()),
                self.stat_counter / (time.perf_counter() - self.start_running),
                self.handle_sum / self.stat_counter if self.stat_counter > 0 else 0
            )
            if self.stat_counter > 50000:
                self.stat_counter = 0
                self.handle_sum = 0
                self.start_running = time.perf_counter()
            await self.update_balance()
            await self.clear_orders_buffers()
            await asyncio.sleep(60)

    def upkeep_cycle(self, predicted_units: TradeUnitList, timeout: int = 10):
        observes = []
        for unit in predicted_units:
            ticker = (
                    self.pairs_to_tickers.get((unit.origin_coin, unit.dest_coin))
                    or self.pairs_to_tickers.get((unit.dest_coin, unit.origin_coin))
            )
            curr_order_book = self.order_book_by_ticker[ticker]

            observes.append(ObserveUnit(
                trade_unit=unit,
                order_book_history=[curr_order_book],
            ))

        observable = ObserveSequence(
            observes=observes,
        )
        asyncio.create_task(self.observe(observable, timeout=timeout), name='observe')

    async def observe(self, to_observe: ObserveSequence, timeout: int):
        max_chart_width = 150
        sleep_interval = timeout / max_chart_width

        for _ in range(max_chart_width):
            await asyncio.sleep(sleep_interval)
            for obs_unit in to_observe.observes:
                tu = obs_unit.trade_unit
                ticker = (
                    self.pairs_to_tickers.get((tu.origin_coin, tu.dest_coin))
                    or self.pairs_to_tickers.get((tu.dest_coin, tu.origin_coin))
                )
                curr_order_book = self.order_book_by_ticker[ticker]

                obs_unit.order_book_history.append(curr_order_book)

        self.display_cycle_chart(to_observe)

    def display_cycle_chart(self, to_observe: ObserveSequence):

        # draw chart for every trade unit
        for obs_unit in to_observe.observes:
            trade_unit = obs_unit.trade_unit
            bids_charts = list(zip(*(
                (bid.price for bid in order_book.bids[:2])
                for order_book in obs_unit.order_book_history
            )))
            asks_charts = list(zip(*(
                (ask.price for ask in order_book.asks[:2])
                for order_book in obs_unit.order_book_history
            )))
            price_line = [trade_unit.price] * len(obs_unit.order_book_history)
            plot_order_book = asciichartpy.plot(
                [
                    price_line,
                    *bids_charts,
                    *asks_charts
                ],
                {
                    'colors': [
                        asciichartpy.yellow,
                        *([asciichartpy.green] * len(bids_charts)),
                        *([asciichartpy.red] * len(asks_charts)),
                    ],
                    'height': 20,
                    'format': '{:5.8f}',
                }
            )

            # render order book history info
            order_book_history_info = ''
            for order_book in obs_unit.order_book_history:
                order_book_history_info = utils.join_multiline_strings(
                    order_book_history_info,
                    '  ',
                    f'{order_book.asks[1].price} ({order_book.asks[1].count})\n'
                    f'{order_book.asks[0].price} ({order_book.asks[0].count})\n'
                    f'{order_book.bids[0].price} ({order_book.bids[0].count})\n'
                    f'{order_book.bids[1].price} ({order_book.bids[1].count})',
                    with_filling_space=True,
                )

            self.logger.info(
                'Print chart for %s -> %s (price:%s)',
                trade_unit.origin_coin,
                trade_unit.dest_coin,
                trade_unit.price
            )
            self.logger.info('\n' + order_book_history_info)
            self.logger.info('\n' + plot_order_book)

        #
        #
        # # generate chart data
        # chart_data_unit = []
        # for tu in predicted_units:
        #     ticker = (
        #         self.pairs_to_tickers.get((tu.origin_coin, tu.dest_coin))
        #         or self.pairs_to_tickers.get((tu.dest_coin, tu.origin_coin))
        #     )
        #     order_book = self.order_book_by_ticker[ticker]
        #     for _ in order_book.:
        #     chart_data_unit.append()
        # # save chart data to history
        # chart_history = self.chart_data.get()
        # if len(chart_history) >= 5:
        #     chart_history.pop(0)
        # chart_history.append(chart_data_unit)
        # # draw chart data
        # # print chart data
        # plot_sizes = asciichartpy.plot(
        #     [
        #         min_sizes,
        #         max_sizes,
        #         target_sizes,
        #     ],
        #     {
        #         'colors': [
        #             asciichartpy.green,
        #             asciichartpy.red,
        #             asciichartpy.white,
        #         ],
        #         'height': 20
        #     }
        # )
        # plot_prices = asciichartpy.plot(
        #     [
        #         prices,
        #         acc_prices,
        #     ],
        #     {
        #         'colors': [
        #             asciichartpy.white,
        #             asciichartpy.yellow,
        #         ],
        #         'height': 20
        #     }
        # )
        # print(plot_sizes)
        # print(plot_prices)
        # print()

