import asyncio
from decimal import Decimal

from tqdm import tqdm

from bot.exceptions import RequestException
from bot.overman import Overman


async def collect_currency(target_curency: str):
    app = Overman(depth=50, prefix='orderbook', pivot_coins=['BTC', 'ETH', 'USDT', 'USDC'])
    await app.load_tickers()
    await app.load_fees()

    await app.update_balance()
    if target_curency not in app.current_balance:
        raise Exception('Invalid currency')
    for curr, val in tqdm(app.current_balance.items()):
        if curr == target_curency:
            pass

        pair_info = app.pairs_info.get((curr, target_curency)) or app.pairs_info.get((target_curency, curr))
        if pair_info is None:
            app.logger.error(f'{(curr, target_curency)} is no pair!')
            continue

        base_increment = Decimal(pair_info.baseIncrement)
        app.logger.info('[%s -> %s] %s %% %s', curr, target_curency, val, base_increment)
        try:
            await app.market_order(
                base_coin=curr, # noqa
                quote_coin=target_curency, # noqa
                size=str(val.quantize(base_increment)),
            )
        except RequestException as e:
            app.logger.error('catch', exc_info=e)
        await asyncio.sleep(0.1)
    await app.update_balance()


if __name__ == '__main__':
    asyncio.run(collect_currency('USDT'))
