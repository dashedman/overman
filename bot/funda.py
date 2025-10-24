import asyncio
from decimal import Decimal
from itertools import chain
from typing import Any

from pydantic import Field
from tqdm import tqdm

from . import utils
from .overman import Overman, PairInfo, BaseCoin, QuoteCoin


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
    tick_size: float
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



class Funda(Overman):
    async def get_fut_symbols(self):
        symbols = await self.do_fut_request('GET', '/api/v1/contracts/active')
        return [FutPairInfo(**sym) for sym in symbols]

    async def get_funding_fee_symbols(self, sort_best: bool = False):
        fut_symbols = await self.get_fut_symbols()
        symbols = [
            fs
            for fs in fut_symbols
            if fs.funding_fee_rate is not None and fs.expire_date is None
        ]
        if sort_best:
            return sorted(symbols, key=lambda sym: abs(sym.funding_fee_rate), reverse=True)
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

    async def get_trade_fees(self, symbols: tuple[str]) -> list[dict[str, Any]]:
        return await self.do_fut_request(
            'GET', '/api/v1/trade-fees',
            params={'symbols': ','.join(symbols)},
            private=True,
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

    async def serve(self):
        await self.load_tickers()
        await self.load_fees()

        print()
