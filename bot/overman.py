import asyncio
import base64
import hashlib
import hmac
import sys
import time
import uuid
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from functools import cached_property
from typing import Literal, Any, NewType, Callable

import aiohttp as aiohttp
import orjson as orjson
from pydantic import BaseModel
from pydantic.alias_generators import to_camel
from sortedcontainers import SortedDict
from tqdm import tqdm

import dto
from . import logger
from . import utils
from .config import Config
from .exceptions import RequestException, BalanceInsufficientError, OrderSizeTooSmallError

BaseCoin = NewType('BaseCoin', str)
QuoteCoin = NewType('QuoteCoin', str)


class PairInfo(BaseModel):
    class Config:
        alias_generator = to_camel
        extra = 'forbid'

    symbol: str
    base_currency: str
    quote_currency: str


class SpotPairInfo(PairInfo):
    name: str
    fee_currency: str
    fee_category: int
    market: str
    base_min_size: str
    quote_min_size: str
    base_max_size: str
    quote_max_size: str
    base_increment: str
    quote_increment: str
    price_increment: str
    price_limit_rate: str
    min_funds: str
    is_margin_enabled: bool
    enable_trading: bool
    maker_fee_coefficient: str
    taker_fee_coefficient: str
    st: bool
    callauction_is_enabled: bool
    callauction_price_floor: str
    callauction_price_ceiling: str
    callauction_first_stage_start_time: str
    callauction_second_stage_start_time: str
    callauction_third_stage_start_time: str
    trading_start_time: str



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


class Overman:
    loop: asyncio.AbstractEventLoop
    start_running: float = -1
    tickers_to_pairs: dict[str, tuple[str, str]]
    pairs_to_tickers: dict[tuple[str, str], str]
    pairs_info: dict[tuple[str, str], PairInfo]
    pair_to_fee: dict[tuple[str, str], Decimal]
    done_orders: dict[str, Any]
    canceled_orders: dict[str, Any]
    init_market_cache: dict[str, list[tuple[dict, int]]]

    def __init__(self):
        self.order_book_by_ticker: dict[str, dto.BestOrders | dto.FullOrders] = {}
        self.__token = None
        self.__private_token = None
        self._ping_interval = None
        self._ping_timeout = None
        self._private_ping_interval = None
        self._private_ping_timeout = None
        self.server_time_correction = 0
        self._ws_id = 0

        try:
            self.config = Config.read_config('./config.yaml')
        except FileNotFoundError:
            self.config = Config.read_config('../config.yaml')

        self.last_profit = -1

        self.current_balance: dict[str, Decimal] = {}
        self.done_orders = {}
        self.canceled_orders = {}
        self.init_market_cache = defaultdict(list)

        self.status_bar = tqdm()
        self.display_body: tuple[str, tuple] | Callable[[], str] = ('', ())
        self.max_display = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    @cached_property
    def session(self):
        return aiohttp.ClientSession(json_serialize=orjson.dumps)

    @cached_property
    def logger(self):
        return logger.setup_logger(self.__class__.__name__ + '_main', with_root=True)

    @cached_property
    def result_logger(self):
        return logger.setup_logger(self.__class__.__name__ + '_result')

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
            if sys.platform in ('win32', 'cygwin', 'cli'):
                from winloop import run
            else:
                # if we're on apple or linux do this instead
                from uvloop import run
            run(self.serve(), debug=True)
            # asyncio.run(self.serve(), debug=True)
        except KeyboardInterrupt:
            self.logger.info('Ended by keyboard interrupt')

    def get_background_tasks(self):
        return [
            self.display_engine(),
        ]

    @abstractmethod
    async def serve(self):
        pass

    async def calibrate_server_time(self):
        start_time = int(time.time() * 1000)
        server_time = await self.do_request('GET', '/api/v1/timestamp')
        end_time = int(time.time() * 1000)

        my_time = (start_time + end_time) // 2
        self.server_time_correction = (server_time - my_time) // 2
        self.logger.info('Server timestamp %s, Local timestamp %s, Time Correction %s',
                         server_time, my_time, self.server_time_correction)

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
        self.logger.info('Got full orders books for %d pairs', len(self.tickers_to_pairs))

    async def deffer_fetch_order_books(self, tickers: list[str]):
        for ticker in tickers:
            if ticker in self.init_market_cache:
                del self.init_market_cache[ticker]
            if ticker in self.order_book_by_ticker:
                del self.order_book_by_ticker[ticker]

        await asyncio.sleep(3)

        self.logger.info('Restore full orders books for %s pairs', tickers)
        await asyncio.gather(*(self.get_full_order_book(ticker) for ticker in tickers))

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

    @staticmethod
    def get_time_line():
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def display(self, info: str, *args):
        self.display_body = (info, args)

    def display_f(self, f: Callable):
        self.display_body = f

    async def display_engine(self):
        while True:
            await asyncio.sleep(1)
            if isinstance(self.display_body, tuple):
                to_display = self.display_body[0] % self.display_body[1]
            else:
                self.display_body: Callable[[], None]
                to_display = self.display_body()

            to_display = f'[{self.get_time_line()}|status] {to_display}'
            self.status_bar.display(to_display + (' ' * (self.max_display - len(to_display))) + '\r')
            self.max_display = len(to_display)

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

    async def do_spot_request(
            self,
            method: Literal['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'HEAD', 'PATCH'],
            endpoint: str,
            data: Any = None,
            params: dict[str, str] = None,
            private: bool = False,
    ):
        api_url = 'https://api.kucoin.com'
        return await self.do_request(
            method, endpoint, data, params, private, api_url
        )

    async def do_fut_request(
            self,
            method: Literal['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'HEAD', 'PATCH'],
            endpoint: str,
            data: Any = None,
            params: dict[str, str] = None,
            private: bool = False,
    ):
        api_url = 'https://api-futures.kucoin.com'
        return await self.do_request(
            method, endpoint, data, params, private, api_url
        )

    async def do_request(
            self,
            method: Literal['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'HEAD', 'PATCH'],
            endpoint: str,
            data: Any = None,
            params: dict[str, str] = None,
            private: bool = False,
            api_url = 'https://api.kucoin.com'
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
        url = api_url.rstrip('/') + '/' + endpoint.lstrip('/')

        for _ in range(3):
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
                    # raise Exception('bad request')
                data_json = await resp.json(loads=orjson.loads)
                resp_code = int(data_json['code'])

                if resp_code == 200000:
                    return data_json['data']
                if resp_code == 200004:
                    raise BalanceInsufficientError(data_json['msg'])
                if resp_code == 400002:
                    continue
                if resp_code == 400100:
                    if 'Order size below the minimum requirement' in data_json['msg']:
                        raise OrderSizeTooSmallError(data_json['msg'])

                self.logger.error(
                    'Catch %s API code while %s: %s',
                    data_json['code'], url, data_json['msg']
                )
                raise RequestException(data_json['msg'])
        raise Exception(f'Cannot do request cause {data_json}')

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

    async def status_monitor(self):
        while True:
            self.logger.info(
                'Asyncio tasks: %d. %.3f rps. %.6f spr. %d req count',
                len(asyncio.all_tasks()),
                self.stat_counter / (time.perf_counter() - self.start_running),
                self.handle_sum / self.stat_counter if self.stat_counter > 0 else 0,
                self.stat_counter_2
            )
            self.stat_counter_2 = 0
            if self.stat_counter > 50000:
                self.stat_counter = 0
                self.handle_sum = 0
                self.start_running = time.perf_counter()
            await self.update_balance()
            await self.clear_orders_buffers()
            await asyncio.sleep(60)
