import asyncio
import json
import random
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_DOWN
from functools import cached_property
from itertools import chain

import asciichartpy
import orjson as orjson
import websockets
from graph_rs import EdgeRS, GraphNodeRS, GraphRS
from tqdm import tqdm

import dto
from . import utils
from .exceptions import BalanceInsufficientError, OrderCanceledError
from .graph import Graph, Cycle
from .overman import Overman, TradeUnitList, TradeUnit, BaseCoin, QuoteCoin, PairInfo


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


class Arba(Overman):
    graph: Graph | GraphRS
    stat_counter: int = 0
    stat_counter_2: int = 0
    handle_sum: float = 0.0
    hf_trade: bool

    def __init__(
            self,
            pivot_coins: list[str],
            depth: int,
            prefix: str,
            hf_trade: bool = False,
            only_show: bool = True
    ):
        super().__init__()

        self.depth = depth
        self.prefix = prefix
        self.pivot_coins = pivot_coins
        self.hf_trade = hf_trade
        self.only_show = only_show

        self.order_book_by_ticker: dict[str, dto.BestOrders | dto.FullOrders] = {}
        self.is_on_trade: bool = False
        self.was_traded = False
        self.last_proposal_pair = None

        self.profit_life_start = 0

    @cached_property
    def pivot_indexes(self):
        return [
            self.graph.get_index_for_coin_name(coin)
            for coin in self.pivot_coins
        ]

    async def serve(self):
        # init graph
        self.logger.info('Loading graph')
        self.loop = asyncio.get_running_loop()

        await self.calibrate_server_time()
        await self.load_tickers()
        await self.load_fees()

        # starting to listen sockets
        # max 100 tickers per connection
        tickers = list(self.tickers_to_pairs.keys())
        random.shuffle(tickers)
        ticker_chunks = utils.chunk(tickers, 20)

        if True:
            tasks = [
                asyncio.create_task(self.market_monitor_socket(ch))
                for ch in ticker_chunks
            ]
            tasks.append(asyncio.create_task(self.fetch_order_books(1)))
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
        tasks.append(
            asyncio.create_task(self.display_engine())
        )

        self.result_logger.info('Start trading bot (Only show: %s)', self.only_show)
        self.start_running = time.perf_counter()
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            await self.session.close()
        self.result_logger.info('Finish trading bot')
        # edit graph
        # trade if graph gave a signal

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

    async def get_trade_fees(self, symbols: tuple[str]) -> list[dict[str, Any]]:
        return await self.do_request(
            'GET', '/api/v1/trade-fees',
            params={'symbols': ','.join(symbols)},
            private=True,
        )

    async def monitor_socket(self, subs: tuple[str]):
        url = f"wss://ws-api-spot.kucoin.com/?token={await self.token()}"
        async for sock in websockets.connect(url, ping_interval=None):
            try:
                last_ping = time.time()
                if subs:
                    sub = self.prepare_sub(subs)
                    pairs = json.dumps(sub)
                    # pairs = orjson.dumps(sub).decode()
                    await sock.send(pairs)

                while True:
                    try:
                        orderbook_raw: str = await sock.recv()
                        orderbook: dict = json.loads(orderbook_raw)
                        # orderbook: dict = orjson.loads(orderbook_raw)
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
                            # await sock.send(orjson.dumps({
                            #     'id': str(self.next_ws_id()),
                            #     'type': 'ping'
                            # }).decode())
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

    async def market_monitor_socket(self, subs: tuple[str]):
        url = f"wss://ws-api-spot.kucoin.com/?token={await self.token()}"
        async for sock in websockets.connect(url, ping_interval=None):
            # asyncio.create_task(self.deffer_fetch_order_books(subs, 5))
            self.logger.info('Starting WebSoket for %s', subs)

            try:
                last_ping = time.time()
                if subs:
                    sub = self.prepare_market_sub(subs)
                    # pairs = json.dumps(sub)
                    pairs = orjson.dumps(sub).decode()
                    await sock.send(pairs)

                while True:
                    try:
                        changes_raw: str = await sock.recv()

                        # changes: dict = json.loads(changes_raw)
                        self.stat_counter_2 += 1
                        changes: dict = orjson.loads(changes_raw)
                        if changes.get('code') == 401:
                            self.logger.info("Token has been expired")
                            await self.reload_token()
                            self.logger.info("Token reloaded")
                        else:
                            if 'data' in changes:
                                self.handle_raw_changes_data(changes['data'])
                                # pass
                            else:
                                # pprint(changes)
                                pass

                        if last_ping + self._ping_interval * 0.8 < time.time():
                            # await sock.send(json.dumps({
                            #     'id': str(self.next_ws_id()),
                            #     'type': 'ping'
                            # }))
                            asyncio.create_task(
                                sock.send(orjson.dumps({
                                    'id': str(self.next_ws_id()),
                                    'type': 'ping'
                                }).decode())
                            )
                            last_ping = time.time()
                    except websockets.ConnectionClosed as e:
                        self.logger.error('Catch error from websocket: %s, for tickers %s', e, subs)
                        break
                    except Exception as e:
                        self.logger.error(
                            'Catch error while monitoring socket:\n',
                            exc_info=e)
                        break
            except websockets.ConnectionClosed as e:
                self.logger.error('websocket error: %s', e)

            asyncio.create_task(self.deffer_fetch_order_books(subs))

    async def orders_socket(self):
        url = f"wss://ws-api-spot.kucoin.com/?token={await self.private_token()}"
        async for sock in websockets.connect(url, ping_interval=None):
            try:
                last_ping = time.time()

                subscribe_msg = self.prepare_orders_topic()
                # subscribe_msg_raw = orjson.dumps(subscribe_msg).decode()
                subscribe_msg_raw = json.dumps(subscribe_msg)
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
                            # order_result: dict = json.loads(order_result_raw)
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
                            # await sock.send(json.dumps({
                            #     'id': str(self.next_ws_id()),
                            #     'type': 'ping'
                            # }))
                            last_ping = time.time()
                    except websockets.ConnectionClosed as e:
                        self.logger.error('Catch error from websocket: %s', e)
                        break
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
            min_funds = Decimal(pair_info.min_funds)
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
            min_funds = Decimal(pair_info.min_funds)
            min_size = Decimal(pair_info.baseMinSize)

            ask, bid = self.tune_to_size_n_funds_full(min_size, min_funds, order_book)
            self.graph.update_graph_edge(pair, ask, fee=pair_fee)
            self.graph.update_graph_edge(pair, bid, fee=pair_fee, inverted=True)
            self.trigger_trade()

        self.handle_sum += time.perf_counter() - start_handle

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

    def trigger_trade(self):
        if self.is_on_trade:
            return

        self.is_on_trade = True
        start_time = time.perf_counter()
        graph_copy = self.graph.py_copy()
        asyncio.create_task(self.process_trade(graph_copy, time.perf_counter() - start_time))

    async def process_trade(self, graph: GraphRS, copy_time: float):
        try:
            start_calc = time.perf_counter()
            # experimental_profit = await self.loop.run_in_executor(
            #     None,
            #     self.check_profit_experimental_3,
            #     graph
            # )
            # experimental_profit = self.check_profit_experimental_3(graph)
            experimental_profit = graph.check_profit_experimental_3(self.pivot_indexes, self.depth)
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
                PROFIT_EDGE = 0.9996
                PROPOSAL_WAIT = 0.5
                if profit_koef >= PROFIT_EDGE:
                    if self.last_proposal_pair:
                        self.logger.warning(
                            'Outtime proposal: %.2f sec, %s',
                            (end_calc - self.last_proposal_pair[1]) - PROPOSAL_WAIT,
                            self.last_proposal_pair[0]
                        )
                        self.last_proposal_pair = None

                    self.display(
                        'Current profit %.4f, '
                        'ct: %.6f, cct %.6f, copy time: %.6f',
                        profit_koef, profit_time, cpu_profit_time, copy_time
                    )
                else:
                    cycle_key = ''.join(f'{node.value} -> ' for node, _ in cycle) + cycle.q[0][0].value
                    if self.last_proposal_pair is None:
                        self.logger.info(
                            'Proposal: %s (wait for %.2f sec)',
                            cycle_key, PROPOSAL_WAIT
                        )
                        self.last_proposal_pair = (cycle_key, end_calc)

                    elif self.last_proposal_pair[0] != cycle_key:
                        self.logger.warning(
                            'Mismatch proposal: %.2f sec, %s',
                            (end_calc - self.last_proposal_pair[1]) - PROPOSAL_WAIT,
                            cycle_key
                        )
                        self.last_proposal_pair = None

                    elif end_calc - self.last_proposal_pair[1] < PROPOSAL_WAIT:
                        self.display(
                            '(Proposal wait) Current profit %.4f, '
                            'ct: %.6f, cct %.6f, copy time: %.6f',
                            profit_koef, profit_time, cpu_profit_time, copy_time
                        )

                    elif self.only_show:
                        self.logger.info(
                            'Detected trade (%s) '
                            'Current profit %.4f, ct: %.5f, cct %.5f, copy time: %.5f',
                            cycle_key,
                            profit_koef, profit_time, cpu_profit_time, copy_time
                        )
                        await asyncio.sleep(PROPOSAL_WAIT)
                    else:
                        # self.display(
                        #     'I want to trade! Current profit %.4f, '
                        #     'ct: %.5f, cct %.5f, copy time: %.5f',
                        #     profit_koef, profit_time, cpu_profit_time, copy_time
                        # )
                        # return
                        res = await self.trade_cycle(cycle, profit_koef, profit_time)
                        if res.need_to_log:
                            self.result_logger.info(res.one_line_status())
                        else:
                            self.display_f(res.one_line_status)

                            if res.started:
                                self.was_traded = True
                                self.last_proposal_pair = None
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

        there_is_dead_price = await self.validate_dead_prices(predicted_units)
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

        # self.upkeep_cycle(predicted_units, timeout=trade_timeout)
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

    async def validate_dead_prices(self, trade_units: TradeUnitList):
        there_is_dead_price = None
        await asyncio.sleep(0.5)
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
                f'[%s -> %s] '
                f'Order is active for now! Waiting timeout (%s)...',
                trade_unit.origin_coin, trade_unit.dest_coin, trade_timeout
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

    def update_graph(
            self,
            coins_pair: tuple[BaseCoin, QuoteCoin],
            new_value: 'dto.OrderBookPair',
            *,
            fee: Decimal,
            inverted: bool = False,
    ):
        # update pairs
        if inverted:
            coins_pair = coins_pair[::-1]
        base_coin, quote_coin = coins_pair

        quote_node = self.graph.get_node_for_coin(quote_coin)
        base_node_index = self.graph.get_index_for_coin_name(base_coin)
        for edge in quote_node.edges:
            if edge.next_node_index == base_node_index and edge.inverted == inverted:

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
        self.graph.filter_from_noncycle_nodes(pivot_nodes, self.depth)

        filtered_pairs = [
            (node.value, self.graph[edge.next_node_index].value)
            for node in self.graph.nodes
            for edge in node.edges
            if not edge.inverted
        ]
        return filtered_pairs

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

