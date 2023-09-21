import asyncio
import base64
import hashlib
import hmac
import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from itertools import chain
from typing import Literal, Any, NewType
from decimal import Decimal, ROUND_UP

import aiohttp as aiohttp
import orjson as orjson
import websockets
from tqdm import tqdm

import bot.logger
from bot import utils
from bot.config import Config
from bot.exceptions import RequestException, BalanceInsufficientError, OrderSizeTooSmallError
from bot.graph import Graph, GraphNode, Edge, Cycle
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


@dataclass(kw_only=True)
class TradeCycleResult:
    cycle: Cycle
    profit_koef: float
    profit_time: float
    started: bool
    balance_difference: Decimal
    real_profit: float = 0
    trade_time: float = 0
    overhauls: int = None
    fail_reason: str = None

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
            f', ttime: {self.trade_time:.3f}' if self.trade_time else ''
        ) + (
            f', overhauls: {self.overhauls}' if self.overhauls is not None else ''
        ) + (
            f', real profit: {self.real_profit}' if self.real_profit else ''
        ) + (
            f', fail: {self.fail_reason}' if self.fail_reason else ''
        )


class Overman:
    graph: Graph
    loop: asyncio.AbstractEventLoop
    tickers_to_pairs: dict[str, tuple[str, str]]
    pairs_to_tickers: dict[tuple[str, str], str]
    pairs_info: dict[tuple[str, str], PairInfo]
    pair_to_fee: dict[tuple[str, str], Decimal]

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
        self._ping_interval = None
        self._ping_timeout = None
        self._ws_id = 0
        self.is_on_trade: bool = False

        self.config = Config.read_config('../config.yaml')

        self.last_profit = -1
        self.profit_life_start = 0

        self.current_balance: dict[str, Decimal] = {}

        self.status_bar = tqdm()

    @cached_property
    def session(self):
        return aiohttp.ClientSession()

    @cached_property
    def logger(self):
        return bot.logger.setup_logger('main', with_root=True)

    @cached_property
    def result_logger(self):
        return bot.logger.setup_logger('result')

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

    async def token(self):
        if self.__token is None:
            await self.reload_token()
        return self.__token

    async def reload_token(self):
        data = await self.do_request('POST', '/api/v1/bullet-public')
        self.__token = data['token']
        self._ping_interval = data['instanceServers'][0]['pingInterval'] / 1000   # to sec
        self._ping_timeout = data['instanceServers'][0]['pingTimeout'] / 1000   # to sec

    @staticmethod
    def next_uuid():
        return uuid.uuid4().int

    def next_ws_id(self):
        self._ws_id = (self._ws_id + 1) % 1000000000
        return self._ws_id

    def run(self):
        try:
            asyncio.run(self.serve())
        except KeyboardInterrupt:
            self.logger.info('Ended by keyboard interrupt')

    async def serve(self):
        # init graph
        self.logger.info('Loading graph')
        self.loop = asyncio.get_running_loop()

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
            sum(len(node.edges) for node in self.graph)
        )

        # loading fees
        # max 10 tickers per connection
        self.pair_to_fee = {}
        ticker_chunks = utils.chunk(self.tickers_to_pairs.keys(), 10)
        for chunk in tqdm(ticker_chunks, postfix='fees loaded', ascii=True):
            data = await self.get_trade_fees(chunk)
            for data_unit in data:
                pair = self.tickers_to_pairs[data_unit['symbol']]
                self.pair_to_fee[pair] = Decimal(data_unit['takerFeeRate'])

        # starting to listen sockets
        # max 100 tickers per connection
        ticker_chunks = utils.chunk(self.tickers_to_pairs.keys(), 50)

        tasks = [
            asyncio.create_task(self.monitor_socket(ch))
            for ch in ticker_chunks
        ]
        tasks.append(
            asyncio.create_task(self.status_monitor())
        )

        self.result_logger.info('Start trading')
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            await self.session.close()
        self.result_logger.info('End trading')
        # edit graph
        # trade if graph gave a signal

    async def monitor_socket(self, subs: tuple[str]):
        url = f"wss://ws-api-spot.kucoin.com/?token={await self.token()}"
        async for sock in websockets.connect(url, ping_interval=None):
            try:
                last_ping = time.time()
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
                            await self.reload_token()
                            self.logger.info("Token reloaded")
                        else:
                            if orderbook.get('data') is not None:
                                self.handle_raw_orderbook_data(orderbook)
                            else:
                                # pprint(orderbook)
                                pass

                        if last_ping + self._ping_interval * 0.8 < time.time():
                            await sock.send(json.dumps({
                                'id': str(self.next_ws_id()),
                                'type': 'ping'
                            }))
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

        for bid in order_book.bids:
            bid_volume += bid.price * bid.count
            virtual_bid.count += bid.count
            if virtual_bid.count >= size and bid_volume >= min_funds:
                virtual_bid.price = bid.price
                break
        return virtual_ask, virtual_bid

    def trigger_trade(self):
        if self.is_on_trade:
            return

        self.is_on_trade = True
        graph_copy = self.graph.copy()
        asyncio.create_task(self.process_trade(graph_copy))

    @staticmethod
    def get_time_line():
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def display(self, info):
        self.status_bar.display(f'[{self.get_time_line()}|status] {info}\r')

    async def process_trade(self, graph: Graph):
        start_calc = time.time()
        experimental_profit = await self.loop.run_in_executor(
            None,
            self.check_profit_experimental_3,
            graph
        )
        end_calc = time.time()
        if experimental_profit:
            profit_koef, cycle = experimental_profit
            profit_time = end_calc - start_calc

            self.display(f'Current profit {profit_koef:.4f}, ct: {profit_time:.5f}')
            if profit_koef == self.last_profit:
                pass
            else:
                now = time.time()
                if self.last_profit < 1:
                    self.result_logger.info('Profit %.4f, lifetime %.5f', self.last_profit, now - self.profit_life_start)
                self.last_profit = profit_koef
                self.profit_life_start = now

            # if profit_koef >= 1:
            #     self.display(f'Current profit {profit_koef:.4f}, ct: {profit_time:.5f}')
            # else:
            #     res = await self.trade_cycle(cycle, profit_koef, profit_time)
            #     if res.balance_difference == 0:
            #         self.display(res.one_line_status())
            #     else:
            #         self.result_logger.info(res.one_line_status())
        self.is_on_trade = False

    async def trade_cycle(self, cycle: Cycle, profit_koef: float, profit_time: float) -> TradeCycleResult:
        first_quote_coin = cycle.q[0][0].value
        current_balance = self.current_balance[first_quote_coin]
        predicted_sizes = self.predict_cycle_parameters(cycle)

        _, start_min, start_max = predicted_sizes[0]
        if start_min > start_max:
            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=False,
                balance_difference=Decimal(0),
                fail_reason=f'Start min greater than start max: {start_min} > {start_max}'
            )
        start_min_balance = start_min * Decimal('1.1') * cycle[0][1].original_price
        start_max_balance = start_max * cycle[0][1].original_price
        start_amort_balance = current_balance * Decimal(0.3)

        real_start = min(
            start_max_balance,
            max(
                start_min_balance,
                start_amort_balance
            )
        )
        if real_start > current_balance:
            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=False,
                balance_difference=Decimal(0),
                fail_reason=f'Start balance greater than balance: {real_start} > {current_balance}'
            )

        for (origin_node, edge, dest_node), (is_sell_phase, size_min, size_max) in zip(cycle.iter_by_pairs(),
                                                                                       predicted_sizes):
            self.logger.info(
                '%s %s %s min %s max %s',
                'sell' if is_sell_phase else 'buy',
                origin_node.value,
                dest_node.value,
                size_min,
                size_max,
            )
        # PREDICT END

        quote_balance = real_start
        start_time = time.time()
        overhauls = 0
        base_balance = Decimal(-1)
        prev_edge_inv = True    # start value

        for cycle_data, predict_data in zip(cycle.iter_by_pairs(), predicted_sizes):
            origin_node, edge, dest_node = cycle_data
            is_sell_phase, size_min, size_max = predict_data

            # balance prediction
            is_buy_phase = not is_sell_phase
            if is_buy_phase:
                if not prev_edge_inv:
                    self.logger.info('swap %s = %s (quote_balance = base_balance)', quote_balance, base_balance)
                    quote_balance = base_balance
                # base_balance = quote_balance / edge.val
            else:
                if prev_edge_inv:
                    self.logger.info('swap %s = %s (base_balance = quote_balance)', base_balance, quote_balance)
                    base_balance = quote_balance
                # quote_balance = base_balance / edge.val

            prev_edge_inv = edge.inversed

            try:
                if is_buy_phase:
                    quote_size = quote_balance / edge.original_price
                    if size_min > quote_size:
                        self.logger.warning('Start min greater than quote size: %s > %s', size_min, quote_size)
                    real_size, real_funds, local_overhauls = await self.trade_pair(
                        origin_node, edge, dest_node,
                        trade_balance=quote_balance
                    )
                    # self.logger.info('Predict base balance difference: %s', real_base - base_balance)
                    self.logger.info('Buy result: size - %s, funds - %s', real_size, real_funds)
                    base_balance = real_size
                    overhauls += local_overhauls
                else:
                    if size_min > base_balance:
                        self.logger.warning('Start min greater than base balance: %s > %s', size_min, base_balance)
                    real_size, real_funds, local_overhauls = await self.trade_pair(
                        origin_node, edge, dest_node,
                        trade_size=base_balance
                    )
                    # self.logger.info('Predict quote balance difference: %s', real_quote - quote_balance)
                    self.logger.info('Sell result: size - %s, funds - %s', real_size, real_funds)
                    quote_balance = real_funds
                    overhauls += local_overhauls
            except OrderSizeTooSmallError:
                return TradeCycleResult(
                    cycle=cycle,
                    profit_koef=profit_koef,
                    profit_time=profit_time,
                    started=True,
                    balance_difference=-real_start,
                    real_profit=0,
                    trade_time=time.time() - start_time,
                    overhauls=overhauls,
                    fail_reason='Order size is too small. Cycle broken.'
                )

        balance_diff = quote_balance - real_start
        if quote_balance > 0:
            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=True,
                real_profit=real_start / quote_balance,
                trade_time=time.time() - start_time,
                overhauls=overhauls,
                balance_difference=balance_diff
            )
        else:
            return TradeCycleResult(
                cycle=cycle,
                profit_koef=profit_koef,
                profit_time=profit_time,
                started=False,
                balance_difference=balance_diff,
                real_profit=real_start / quote_balance,
                trade_time=time.time() - start_time,
                overhauls=overhauls,
                fail_reason='Balance difference is negative'
            )

    def predict_cycle_parameters(
            self,
            cycle: Cycle
    ) -> list[tuple[bool, Decimal, Decimal]]:
        default_fee = Decimal('0.001')
        predict_results = []

        prev_phase_is_sell = False
        last_sell_size = 0
        last_funds = 0
        last_max_sell_size = 100000000000
        last_max_funds = 100000000000
        for origin_node, edge, dest_node in cycle.iter_by_pairs_reversed():
            quote_coin = origin_node.value
            base_coin = dest_node.value
            if edge.inversed:
                fixed_pair = (quote_coin, base_coin)
            else:
                fixed_pair = (base_coin, quote_coin)
            pair_info = self.pairs_info[fixed_pair]
            is_sell_phase = edge.inversed

            min_size = Decimal(pair_info.baseMinSize)
            max_size = Decimal(pair_info.baseMaxSize)
            size_increment = Decimal(pair_info.baseIncrement)
            quote_increment = Decimal(pair_info.quoteIncrement)
            min_funds = Decimal(pair_info.minFunds)

            if is_sell_phase:
                if prev_phase_is_sell:
                    last_funds = last_sell_size
                    last_max_funds = last_max_sell_size

                last_sell_size = max(
                    min_size,
                    last_funds / (1 - default_fee) / edge.original_price,
                    min_funds / edge.original_price
                ).quantize(size_increment, rounding=ROUND_UP)
                last_max_sell_size = min(
                    max_size,
                    last_max_funds / (1 - default_fee) / edge.original_price,
                    edge.volume
                ).quantize(size_increment)
                predict_results.append((is_sell_phase, last_sell_size, last_max_sell_size))
            else:
                if not prev_phase_is_sell:
                    last_sell_size = last_funds
                    last_max_sell_size = last_max_funds

                last_buy_size = max(
                    min_size,
                    # last_sell_size / (1 - default_fee),
                    last_sell_size,
                    min_funds / edge.original_price
                ).quantize(size_increment, rounding=ROUND_UP)
                last_max_buy_size = min(
                    max_size,
                    # last_max_sell_size / (1 - default_fee) / edge.original_price,
                    last_max_sell_size / edge.original_price,
                    edge.volume
                ).quantize(size_increment)
                predict_results.append((is_sell_phase, last_buy_size, last_max_buy_size))
                last_funds = (
                    last_buy_size * edge.original_price
                ).quantize(quote_increment)
                last_max_funds = (
                    last_max_buy_size * edge.original_price
                ).quantize(quote_increment)

            prev_phase_is_sell = is_sell_phase

        predict_results.reverse()
        return predict_results

    async def trade_pair(
            self,
            node_1: GraphNode,
            edge: Edge,
            node_2: GraphNode,
            *,
            trade_size: Decimal = None,
            trade_balance: Decimal = None,
    ) -> tuple[Decimal, Decimal, int]:
        quote_coin = node_1.value
        base_coin = node_2.value
        is_buy_phase = trade_size is None

        pair_info = self.pairs_info.get((base_coin, quote_coin)) or self.pairs_info[(quote_coin, base_coin)]

        quote_increment = Decimal(pair_info.quoteIncrement)
        size_increment = Decimal(pair_info.baseIncrement)
        base_min_size = Decimal(pair_info.baseMinSize)
        # base_max_size = float(pair_info.baseMaxSize)

        # price = int(edge.original_price / price_increment) * price_increment
        price = edge.original_price
        size = (
            trade_size if trade_size else trade_balance.quantize(
                quote_increment
            ) / price
        ).quantize(size_increment)
        self.logger.info(
            'Size resolving: min - %s, ideal - %s, quote incr - %s (trade size: %s, trade balance: %s)',
            base_min_size, size, quote_increment,
            trade_size, trade_balance
        )

        self.logger.info(
            '[%s -> %s] Start. (price: %s; size: %s, funds: %s)',
            quote_coin, base_coin, price, size, price * size
        )
        if size > edge.volume:
            self.logger.warning('Size greater than size in edge: %s > %s', size, edge.volume)

        for attempt in range(100):
            try:
                order_id = await self.create_order(
                    base_coin=base_coin,
                    quote_coin=quote_coin,
                    price=str(price),
                    size=str(size),
                )
            except BalanceInsufficientError as e:
                self.logger.warning(f'Catch "{e}" error. Trying again...')
                continue

            order_is_active = True
            order_result = None
            while order_is_active:
                order_result = await self.get_order(order_id)
                self.logger.info(f'{order_result}')
                order_is_active = order_result['isActive']
                if order_is_active:
                    self.logger.info('[%s -> %s] Order is active for now! Waiting...')

            if order_result['cancelExist']:
                current_book = self.order_book_by_ticker[
                    self.pairs_to_tickers.get((base_coin, quote_coin))
                    or self.pairs_to_tickers[(quote_coin, base_coin)]
                ]
                ask, bid = self.tune_to_size(size, current_book)
                if is_buy_phase:
                    new_price = ask.price
                else:
                    new_price = bid.price
                self.logger.warning(
                    '[%s -> %s] Order is canceled! '
                    'Trying again, Changing price: %s -> %s.',
                    quote_coin, base_coin,
                    price, new_price,
                )
                price = new_price
                continue

            fee = Decimal(order_result['fee'])
            real_size = Decimal(order_result['dealSize'])
            real_funds = Decimal(order_result['dealFunds'])
            if is_buy_phase:
                pass
            else:
                self.logger.info('Real Fee: %s', fee / real_funds)
                real_funds = real_funds - fee.quantize(quote_increment)
            self.logger.info('[%s -> %s] OK', quote_coin, base_coin)
            return real_size, real_funds, attempt
        self.logger.warning(
            '[%s -> %s] Order is canceled! Cycle broken. Skip.',
            quote_coin, base_coin
        )
        return Decimal(0), Decimal(0), 0

    async def create_order(
            self,
            base_coin: BaseCoin | QuoteCoin,
            quote_coin: QuoteCoin | BaseCoin,
            price: str,
            size: str
    ) -> str:
        endpoint = '/api/v1/orders'

        if ticker := self.pairs_to_tickers.get((base_coin, quote_coin)):
            trade_side = 'buy'
        elif ticker := self.pairs_to_tickers.get((quote_coin, base_coin)):
            trade_side = 'sell'
        else:
            raise Exception(f'Pair {base_coin} - {quote_coin} does not exist!')

        data = await self.do_request(
            'POST',
            endpoint,
            data={
                'clientOid': str(self.next_uuid()),
                'side': trade_side,
                'symbol': ticker,
                'price': price,
                'size': size,
                # optional
                'type': 'limit',
                'tradeType': 'TRADE',
                'timeInForce': 'FOK',
                # 'timeInForce': 'IOC',
            },
            private=True
        )
        return data['orderId']

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
            timestamp = int(time.time() * 1000)  # convert to milliseconds
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
            data_json = await resp.json()
            match data_json['code']:
                case '200000':
                    return data_json['data']
                case '200004':
                    raise BalanceInsufficientError(data_json['msg'])
                case '400100':
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

    async def get_order(self, order_id: str):
        data = await self.do_request(
            'GET', f'/api/v1/orders/{order_id}',
            private=True
        )
        return data

    async def update_balance(self):
        accounts_info = await self.get_accounts_list()
        for acc_info in accounts_info:
            if acc_info['type'] != 'trade':
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
        data = await self.do_request('GET', '/api/v1/accounts', private=True)
        return data

    async def get_actual_orderbook(self, ticker: str):
        data = await self.do_request(
            'GET', '/api/v1/market/orderbook/level2_20',
            params={'symbol': ticker}
        )
        return self.get_order_book_from_raw(data)

    async def get_trade_fees(self, symbols: tuple[str]):
        return await self.do_request(
            'GET', '/api/v1/trade-fees',
            params={'symbols': ','.join(symbols)},
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
            self, graph: Graph,
    ) -> tuple[float, Cycle] | None:
        best_profit = 10000000
        best_cycle = None
        for pivot_coin_index in self.pivot_indexes:
            profit, cycle = graph.get_profit_3(pivot_coin_index)
            if cycle and not cycle.validate_cycle():
                continue
            if profit != -1 and profit < best_profit:
                best_profit = profit
                best_cycle = cycle

        if best_cycle is not None:
            return best_profit, best_cycle
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

            if pair['quoteCurrency'] in base_to_quotes and pair['baseCurrency'] in quote_to_bases:
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
                    edges.append(Edge(index, node_keys.index(tail), 0, 0.0, False))
                except ValueError:
                    continue
            for tail in base_to_quotes[node_key]:
                try:
                    edges.append(Edge(index, node_keys.index(tail), 0, 0.0, True))
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
            await self.update_balance()
            await asyncio.sleep(60)
