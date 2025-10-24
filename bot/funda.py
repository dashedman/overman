from pydantic import Field

from .overman import Overman, PairInfo


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
