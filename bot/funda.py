import asyncio
import math
import time
from collections import deque, defaultdict
from contextlib import asynccontextmanager
from datetime import timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Literal, Callable, Awaitable, Iterable

import cachetools
import orjson
import websockets
from pydantic import Field
from tqdm import tqdm
from typing_extensions import deprecated
from websockets.asyncio.client import ClientConnection

from . import utils
from .exceptions import RequestException
from .overman import Overman, PairInfo, BaseCoin, QuoteCoin, SymbolFee


class TradeSide(Enum):
    BUY = 'buy'
    SELL = 'sell'


class PositionSide(Enum):
    LONG = 'LONG'
    SHORT = 'SHORT'
    BOTH = 'BOTH'

    def trade_side_to_open(self):
        match self:
            case PositionSide.LONG:
                return TradeSide.BUY
            case PositionSide.SHORT:
                return TradeSide.SELL
            case _:
                raise NotImplementedError('Undefined')

    def trade_side_to_close(self):
        match self:
            case PositionSide.LONG:
                return TradeSide.SELL
            case PositionSide.SHORT:
                return TradeSide.BUY
            case _:
                raise NotImplementedError('Undefined')


class MarginMode(Enum):
    ISOLATED = 'ISOLATED'
    CROSS = 'CROSS'


class OrderType(Enum):
    LIMIT = 'limit'
    MARKET = 'market'



class FutPairInfo(PairInfo):
    root_symbol:  str
    type:  str
    first_open_date:  int
    expire_date:  int | None
    settle_date:  int | None
    settle_currency: str
    max_order_qty: int
    market_max_order_qty: int
    max_price: float
    lot_size: int
    tick_size: Decimal
    index_price_tick_size: float
    multiplier: float
    initial_margin: float
    maintain_margin: float
    max_risk_limit: int
    min_risk_limit: int
    risk_step: int
    maker_fee_rate: float
    taker_fee_rate: float
    taker_fix_fee: float
    maker_fix_fee: float
    settlement_fee: None
    is_deleverage: bool
    is_quanto: bool
    is_inverse: bool
    mark_method: str
    fair_method: str | None
    funding_base_symbol: str | None
    funding_quote_symbol: str | None
    funding_rate_symbol: str | None
    index_symbol: str
    settlement_symbol: str | None
    status: str
    predicted_funding_fee_rate: None
    funding_rate_granularity: int | None
    funding_rate_cap: float | None
    funding_rate_floor: float | None
    period: int | None
    open_interest: str
    turnover_of_24h: float = Field(validation_alias='turnoverOf24h')
    volume_of_24h: float = Field(validation_alias='volumeOf24h')
    mark_price: float
    index_price: float
    last_trade_price: float
    next_funding_rate_time: int | None
    next_funding_rate_date_time: int | None
    max_leverage: int
    source_exchanges: list
    premiums_symbol1m: str
    premiums_symbol8h: str
    funding_base_symbol1m: str | None
    funding_quote_symbol1m: str | None
    low_price: float
    high_price: float
    price_chg_pct: float
    price_chg: float
    k: float
    m: float
    f: float
    mmr_limit: float
    mmr_lev_constant: float
    support_cross: bool
    buy_limit: float
    sell_limit: float
    adjust_k: float | None
    adjust_m: float | None
    adjust_mmr_lev_constant: float | None
    adjust_active_time: int | None
    cross_risk_limit: float
    market_stage: str
    pre_market_to_perp_date: None
    funding_fee_rate: float | None

    @property
    def to_next_settlement(self):
        return timedelta(milliseconds=self.next_funding_rate_time)

    @property
    def minimal_size(self):
        return math.ceil(self.lot_size * self.multiplier)


WebsocketMsg = dict[str, Any]


def sort_by_profit(symbols: Iterable[FutPairInfo]) -> list[FutPairInfo]:
    return sorted(symbols, key=lambda sym: abs(sym.funding_fee_rate), reverse=True)


class Funda(Overman):

    def __init__(self):
        super().__init__()
        self.funds_catcher_tasks = dict[str, asyncio.Task]()

        self.positions_futures = dict[str, asyncio.Future[WebsocketMsg]]()
        self.balance_futures = defaultdict[str, list[asyncio.Future[WebsocketMsg]]](list)

        self.need_to_subscribe = deque[str]()
        self.need_to_unsubscribe = deque[str]()

        self.subscribed_topics = dict[tuple[str, bool], Callable[[Any], Awaitable[None]]]()
        self.topic_router = dict[str, Callable[[Any], Awaitable[None]]]()

        self.wait_socket = asyncio.Event()
        self.current_sock: ClientConnection | None = None

        self.symbol_to_fee = cachetools.TTLCache[str, SymbolFee](maxsize=1000, ttl=60 * 60 * 10)
        self.get_fee_lock = defaultdict(asyncio.Lock)

    async def get_fut_symbols(self, symbol: str | None = None):
        if symbol is None:
            symbols = await self.do_fut_request('GET', '/api/v1/contracts/active')
            return [FutPairInfo(**sym) for sym in symbols]
        else:
            symbol = await self.do_fut_request('GET', f'/api/v1/contracts/{symbol}')
            return FutPairInfo(**symbol)


    async def get_funding_fee_symbols(self, sort_best: bool = False):
        fut_symbols = await self.get_fut_symbols()
        symbols = [
            fs
            for fs in fut_symbols
            if fs.funding_fee_rate is not None and fs.expire_date is None
        ]
        if sort_best:
            return sort_by_profit(symbols=symbols)
        return symbols

    async def load_tickers(self):
        pairs_infos = await self.get_funding_fee_symbols()

        self.tickers_to_pairs: dict[str, tuple[BaseCoin, QuoteCoin]] = {
            p.symbol: (p.base_currency, p.quote_currency)
            for p in pairs_infos
        }
        self.pairs_to_tickers: dict[tuple[BaseCoin, QuoteCoin], str] = {
            pair: ticker for ticker, pair in self.tickers_to_pairs.items()
        }
        self.pairs_info: dict[tuple[BaseCoin, QuoteCoin], PairInfo] = {
            pair: pair_info
            for pair_info in pairs_infos
            if (pair := self.tickers_to_pairs.get(pair_info.symbol))
        }

        self.logger.info('Loaded %s pairs', len(pairs_infos))

    async def get_trade_fees(self, symbol: str) -> dict[str, Any]:
        return await self.do_fut_request(
            'GET', '/api/v1/trade-fees',
            params={'symbol': symbol},
            private=True,
        )

    @deprecated('use lazy load get_fee_for_pair()')
    async def load_fees(self):
        # loading fees
        # max 10 tickers per connection
        keys_iter = iter(self.tickers_to_pairs.keys())
        first_ticker = next(keys_iter)
        ticker_chunks = utils.chunk(keys_iter, 10)

        while True:
            self.logger.info('Trying to get first fees info.')
            try:
                data_unit = await self.get_trade_fees(first_ticker)
                if data_unit:
                    pair = self.tickers_to_pairs[data_unit['symbol']]
                    self.pair_to_fee[pair] = SymbolFee(**data_unit)
                    break
            except Exception as e:
                self.logger.warning(f'Catch {e}. Trying again. Sleep 60 sec')
            await asyncio.sleep(60)

        for chunk in tqdm(ticker_chunks, postfix='fees loaded', ascii=True):
            data = await asyncio.gather(*(
                self.get_trade_fees(ticker)
                for ticker in chunk
            ))
            for data_unit in data:
                pair = self.tickers_to_pairs[data_unit['symbol']]
                self.pair_to_fee[pair] = SymbolFee(**data_unit)

    async def get_fee_for_symbol(self, symbol: str):
        async with self.get_fee_lock[symbol]:
            if symbol in self.symbol_to_fee:
                return self.symbol_to_fee[symbol]

            fee_raw = await self.get_trade_fees(symbol=symbol)
            fee = SymbolFee(**fee_raw)
            self.symbol_to_fee[symbol] = fee
            return fee

    async def get_funding_rate(self, symbol: str):
        return await self.do_fut_request(
            'GET',
            f'/api/v1/funding-rate/{symbol}/current',
        )

    async def serve(self):
        await self.calibrate_server_time()
        await self.load_tickers()
        # await self.load_fees()
        await self.update_balance()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.funding_checker())
            tg.create_task(self.listen_socket())

    def tasks_in_process(self):
        return not all(t.done() for t in self.funds_catcher_tasks.values())

    async def funding_checker(self):
        while True:
            if not self.tasks_in_process():
                await self.check_coming_funding()
            await asyncio.sleep(3 * 60)

    async def check_coming_funding(self):
        symbols = await self.get_funding_fee_symbols()
        self.logger.info('Funding symbols: %s', len(symbols))

        # temporary limitation
        only_usdt = {sym for sym in symbols if sym.quote_currency == 'USDT'}
        self.logger.info('Only USDT symbols: %s', len(only_usdt))

        not_usdt = {sym for sym in symbols if sym.quote_currency != 'USDT'}
        another_currencies = {nu.quote_currency for nu in not_usdt}
        self.logger.info('Another currencies: %s', another_currencies)
        self.logger.info(
            'Top 3 non-usdt profits: %s',
            {
                f'{s.funding_fee_rate * 100:.2f}': f'{s.quote_currency}/{s.base_currency}'
                for s in sort_by_profit(not_usdt)[:3]
            }
        )

        # semaphore = asyncio.Semaphore(50)
        # async def fee_loader(symbol: FutPairInfo):
        #     async with semaphore:
        #         await self.get_fee_for_symbol(symbol.symbol)
        #
        # for sym in symbols:
        #     asyncio.create_task(fee_loader(sym))

        profitable_funding = set()
        pos_lost_avg = 0
        for symbol in symbols:
            position_lost_koef = 0.0025 + 0 * 2 * symbol.taker_fee_rate / (1 - symbol.taker_fee_rate)
            # position_lost_koef *= 0
            pos_lost_avg += position_lost_koef
            if position_lost_koef < abs(symbol.funding_fee_rate):
                profitable_funding.add(symbol)
        pos_lost_avg /= len(symbols)
        self.logger.info(
            'Profitable symbols (edge: %.2f) (%s): \n%s',
            pos_lost_avg * 100,
            len(profitable_funding),
            '\n'.join(
                f'\t + {s.funding_fee_rate * 100}% {s.symbol} in {s.to_next_settlement}'
                for s in sort_by_profit(profitable_funding)[:5]
            )
        )

        funding_by_time_window = defaultdict(set)
        for sym in symbols:
            minutes, secs = divmod(int(sym.to_next_settlement.total_seconds()), 60)
            time_window = timedelta(minutes=minutes)
            funding_by_time_window[time_window].add(sym)
        funding_windows = sorted(funding_by_time_window)
        self.logger.info('Funding windows: %s', ', '.join(str(w) for w in funding_windows))

        nearest_window = funding_windows[0]
        nearest_window_funding = funding_by_time_window[nearest_window]
        self.logger.info(
            'Nearest profits symbols (%s): %s',
            len(nearest_window_funding),
            {
                f'{s.funding_fee_rate * 100}%': s.symbol
                for s in sort_by_profit(nearest_window_funding)[:3]
            }
        )

        ACTIVE_FUNDS_RATIO = 0.9
        FUNDING_TASKS = min(3, len(nearest_window_funding))
        funds_for_task = float(self.current_balance['USDT']) * ACTIVE_FUNDS_RATIO / FUNDING_TASKS

        could_be_processed = set()
        for sym in symbols:
            # print(sym.symbol, sym.index_price * sym.minimal_size, funds_for_task)
            if sym.index_price * sym.minimal_size < funds_for_task:
                could_be_processed.add(sym)
        self.logger.info('Could be processed symbols: %s', len(could_be_processed))

        profitable_in_window = nearest_window_funding & profitable_funding
        self.logger.info('Profitable in window: %s', len(profitable_in_window))

        all_in_one = sort_by_profit(profitable_in_window & only_usdt & could_be_processed)
        self.logger.info('All in one: %s', len(all_in_one))

        if nearest_window > timedelta(minutes=7):
            self.logger.info(
                'Nearest window will be in %s: %s',
                nearest_window,
                ', '.join(
                    f'{s.funding_fee_rate * 100:.2f}%: {s.symbol}'
                    for s in all_in_one[:FUNDING_TASKS]
                ) or 'X'
            )
            return

        to_process = all_in_one[:FUNDING_TASKS]
        for sym in to_process:
            size_of_currency = float(funds_for_task) / sym.index_price
            lots_number = max(1, int(size_of_currency / sym.minimal_size))
            # lots_number = 1
            self.start_funding_process(symbol=sym, lots_number=lots_number)

    def start_funding_process(self, symbol: FutPairInfo, lots_number: int):
        task = asyncio.create_task(self.process_funding(symbol, lots_number))
        self.funds_catcher_tasks[symbol.symbol] = task
        task.add_done_callback(lambda _: self.funds_catcher_tasks.pop(symbol.symbol))

    async def process_funding(self, symbol: FutPairInfo, lots_number: int):
        order_fut = asyncio.Future()
        funding_fut = asyncio.Future()

        try:
            processing_logger = self.logger.getChild(f'Funding:{symbol.symbol}')

            await self.switch_margin_mode(symbol=symbol.symbol, mode='ISOLATED')
            wait_to_minute = symbol.to_next_settlement.total_seconds() - 10
            if wait_to_minute < 0:
                processing_logger.warning('Too late funding: %s', symbol.to_next_settlement)
                return
            elif wait_to_minute > 0:
                processing_logger.info('Sleeping before start: %s sec', wait_to_minute)
                await asyncio.sleep(wait_to_minute)

            # sym_info_list = []
            # for _ in range(10):
            #     request_start = time.time()
            #     sym_info = await self.get_fut_symbols(symbol=symbol.symbol)
            #     request_end = time.time()
            #     request_mid = (request_start + request_end) / 2
            #     sym_info_list.append((request_mid, sym_info))
            #     await asyncio.sleep(4)
            #
            # first_index_price = None
            # first_lt_price = None
            # for rt, sym_info in sym_info_list:
            #
            #     if first_index_price is None:
            #         first_index_price = sym_info.index_price
            #         first_lt_price = sym_info.last_trade_price
            #
            #     processing_logger.info(
            #         '%s) Funding rate %.3f%% in %s, diff: idx %.3f%%, lt %.3f%%',
            #         datetime.fromtimestamp(timestamp=rt),
            #         sym_info.funding_fee_rate * 100,
            #         sym_info.to_next_settlement,
            #         (sym_info.index_price / first_index_price - 1) * 100,
            #         (sym_info.last_trade_price / first_lt_price - 1) * 100,
            #     )
            #
            #     # processing_logger.info(
            #     #     '%s) Funding rate %.3f%% in %s, index price: %s, mark price: %s, last trade price: %s, diff: idx %.3f%%, lt %.3f%%',
            #     #     datetime.fromtimestamp(timestamp=rt),
            #     #     sym_info.funding_fee_rate * 100,
            #     #     sym_info.to_next_settlement,
            #     #     sym_info.index_price,
            #     #     sym_info.mark_price,
            #     #     sym_info.last_trade_price,
            #     #     (sym_info.index_price / first_index_price - 1) * 100,
            #     #     (sym_info.last_trade_price / first_lt_price - 1) * 100,
            #     # )
            # return

            processing_logger.info(
                'Starting for %.2f%%, lots: %s',
                symbol.funding_fee_rate * 100, lots_number
            )

            # create position, x1 to avoid liquidation
            processing_logger.info('Creating position..')
            side, opposite_side = (PositionSide.SHORT, PositionSide.LONG) if symbol.funding_fee_rate > 0 else (PositionSide.LONG, PositionSide.SHORT)
            opened_order_id = await self.create_position(symbol=symbol.symbol, side=side, size=lots_number)

            e = None
            for _ in range(5):
                try:
                    opened_order = await self.get_order(opened_order_id)
                except RequestException as e:
                    if 'error.getOrder.orderNotExist' in str(e):
                        continue
                    raise e
                break
            else:
                raise e

            # create profitable limit order
            # need_profit = 0.01
            # profit_coefficient = 1 + (need_profit if side is PositionSide.LONG else -need_profit)
            # profit_price = (Decimal(opened_order['avgDealPrice']) * Decimal(profit_coefficient)).quantize(symbol.tick_size)
            # processing_logger.info(
            #     'Creating profit limit order (profit: %.2f%%, price: %s)',
            #     need_profit * 100, profit_price
            # )
            # profit_order_id = await self.close_position(symbol=symbol.symbol, price=profit_price, side=side)
            #
            # self.done_orders[profit_order_id] = order_fut
            self.balance_futures[symbol.quote_currency].append(funding_fut)

            # wait while fund will be settled or order closed by tp
            processing_logger.info('Waiting for settle..')
            await asyncio.wait((order_fut, funding_fut), timeout=60, return_when=asyncio.FIRST_COMPLETED)

            if order_fut.done():
                # profit order done, do not wait for funds
                processing_logger.info('Profit order closed!')
                funding_fut.cancel()
                return

            elif funding_fut.done():

                processing_logger.info('Funding fee settlement taken!')
            else:
                self.logger.warning('Settlement timeout')

            # cancel order if need
            order_fut.cancel()
            # del self.done_orders[profit_order_id]
            # await self.cancel_order(profit_order_id)

            # close position by market
            await self.close_position(symbol=symbol.symbol, side=side)

            # update balance
            await self.update_balance(currency=symbol.base_currency)
        finally:
            order_fut.cancel()
            funding_fut.cancel()

    async def create_position(
            self,
            symbol: str,
            size: int,
            side: PositionSide
    ) -> str:
        # create order
        data = await self.create_fut_order(
            symbol=symbol,
            size=size,
            side=side,
        )
        return data['orderId']

    async def close_position(
            self,
            symbol: str,
            side: PositionSide,
            price: Decimal | None = None
    ):
        close_type: Literal['immediate', 'limit'] = 'immediate' if price is None else 'limit'
        order = await self.create_fut_order(symbol=symbol, side=side, close=close_type, price=price)
        return order['orderId']

    async def create_fut_order(
            self,
            symbol: str,
            side: PositionSide,
            size: int | None = None,
            price: Decimal | None = None,
            close: Literal['immediate', 'limit'] | None = None,
    ) -> dict[str, str]:
        endpoint = '/api/v1/orders'

        request_data = {
            'clientOid': str(self.next_uuid()),
            'symbol': symbol,
            'marginMode': MarginMode.ISOLATED.value,
            'positionSide': side.value,
        }
        if close is not None:
            # close current position
            request_data['closeOrder'] = True
            if close == 'immediate':
                request_data['type'] = 'market'
            elif close == 'limit':
                assert price is not None
                request_data['type'] = 'limit'
                request_data['price'] = str(price)
            else:
                raise Exception(f'Undefined close case: {close}')
        else:
            # open new position
            assert size is not None
            assert side is not None

            trade_side = side.trade_side_to_open()
            request_data |= {
                # opt
                'side': trade_side.value,
                'leverage': 1,
                'type': 'market',
                'size': size,
            }

        data = await self.do_fut_request(
            'POST',
            endpoint,
            data=request_data,
            private=True
        )
        return data

    async def cancel_order(self, order_id: str):
        endpoint = f'/api/v1/orders/{order_id}'
        await self.do_fut_request(
            'DELETE',
            endpoint,
            private=True
        )

    async def get_order(self, order_id: str):
        endpoint = f'/api/v1/orders/{order_id}'
        data = await self.do_fut_request(
            'GET',
            endpoint,
            private=True,
        )
        return data

    async def update_balance(self, currency: str | None = None):
        if currency:
            currencies = (currency,)
        else:
            currencies = tuple({quote for _, quote in self.pairs_info})

        for currency in currencies:
            account_info = await self.get_account_info(currency=currency)
            self.current_balance[currency] = Decimal(account_info['availableBalance'])
        self.logger.info(
            'Current balance: %s',
            ' '.join(
                f'[{c}: {v:.4f}]'
                for c, v in self.current_balance.items()
                if v > 0
            )
        )

    async def get_account_info(self, currency: str):
        return await self.do_fut_request(
            method='GET',
            endpoint='/api/v1/account-overview',
            params={'currency': currency},
            private=True
        )

    async def switch_margin_mode(self, symbol: str, mode: Literal['ISOLATED', 'CROSS']):
        print(await self.do_fut_request(
            method='POST',
            endpoint='/api/v2/position/changeMarginMode',
            data={
                'symbol': symbol,
                'marginMode': mode,
            },
            private=True,
        ))

    @asynccontextmanager
    async def with_subscribe(self, topic: str, is_private: bool, callback: Callable[[Any], Awaitable[None]]):
        await self.subscribe_topic(topic, is_private, callback)
        yield
        await self.unsubscribe_topic(topic, is_private)

    async def subscribe_topic(self, topic: str, is_private: bool, callback: Callable[[Any], Awaitable[None]]):
        if topic in self.topic_router:
            # already subscribed
            return
        await self.wait_socket.wait()
        self.logger.info('Subscribe to %s', topic)
        subscribe_msg = {
            "topic": topic,
            "privateChannel": is_private,
            "id": self.next_ws_id(),
            "type": "subscribe",
            "response": True,
        }
        self.topic_router[topic] = callback
        self.subscribed_topics[(topic, is_private)] = callback

        subscribe_msg_raw = orjson.dumps(subscribe_msg).decode()
        await self.current_sock.send(subscribe_msg_raw)

    async def unsubscribe_topic(self, topic: str, is_private: bool):
        await self.wait_socket.wait()

        self.logger.info('Unsubscribe from %s', topic)
        unsubscribe_msg = {
            "topic": topic,
            "privateChannel": is_private,
            "id": self.next_ws_id(),
            "type": "unsubscribe",
            "response": True,
        }
        del self.topic_router[topic]
        del self.subscribed_topics[(topic, is_private)]

        unsubscribe_msg_raw = orjson.dumps(unsubscribe_msg).decode()
        await self.current_sock.send(unsubscribe_msg_raw)

    async def insure_subscribe_topics(self):
        base_topics = [
            ("/contractMarket/tradeOrders", True, self.process_order_msg),
            ("/contractAccount/wallet", True, self.process_balance_msg),
        ]
        for base_topic, is_private, callback in base_topics:
            key = (base_topic, is_private)
            if key not in self.subscribed_topics:
                self.subscribed_topics[key] = callback

        for (topic, is_private), callback in self.subscribed_topics.items():
            await self.subscribe_topic(topic, is_private, callback)

    async def listen_socket(self):
        url = f"wss://ws-api-futures.kucoin.com/?token={await self.private_token()}"
        async for sock in websockets.connect(url, ping_interval=None):
            self.current_sock = sock
            self.topic_router.clear()
            self.wait_socket.set()
            try:
                last_ping = time.time()

                await self.insure_subscribe_topics()

                while True:
                    try:
                        try:
                            async with asyncio.timeout(self._private_ping_interval * 0.2):
                                message_raw: str = await sock.recv()
                        except TimeoutError:
                            pass
                        else:
                            message: dict = orjson.loads(message_raw)
                            if message.get('code') == 401:
                                self.logger.info("Token has been expired")
                                await self.reload_private_token()
                                self.logger.info("Token reloaded")
                            else:
                                if 'data' in message:
                                    asyncio.create_task(self.route_ws_message(message))
                                else:
                                    pass

                        if last_ping + self._private_ping_interval * 0.8 < time.time():
                            await sock.send(orjson.dumps({
                                'id': str(self.next_ws_id()),
                                'type': 'ping'
                            }).decode())
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
            finally:
                self.current_sock = None
                self.wait_socket = asyncio.Event()

    async def route_ws_message(self, message: dict[str, Any]):
        topic = message['topic']
        callback = self.topic_router.get(topic)
        if callback is None:
            self.logger.warning('Cannot process topic: %s', topic)
        else:
            await callback(message)

    async def process_order_msg(self, msg: dict[str, Any]):
        order = msg['data']
        order_id = order['orderId']
        order_status = order['status']
        match order_status:
            case 'done':
                if done_fut := self.done_orders.get(order_id):
                    done_fut.set_result(order)
                    del self.done_orders[order_id]
            case _:
                pass

    async def process_balance_msg(self, msg):
        event_data = msg['data']
        funding_fee = Decimal(event_data['isolatedFundingFeeMargin'])
        currency = event_data['currency']
        if funding_fee != 0:
            self.logger.info('Caught funding balance change! %s: %s', currency, funding_fee)
            futures = self.balance_futures[currency]
            for fut in futures:
                fut.set_result(msg)
            futures.clear()
        else:
            self.logger.info('BALANCE MSG: %s', msg)


    #
    # async def process_position_msg(self, position):
    #     symbol = position['symbol']
    #     order_status = order['status']
    #     match order_status:
    #         case 'done':
    #             if done_fut := self.done_orders.get(order_id):
    #                 done_fut.set_result(order)
    #         case _:
    #             pass
