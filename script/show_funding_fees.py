import asyncio
from datetime import datetime, UTC, timedelta
from pprint import pprint

from bot.overman import Overman


async def main():
    async with Overman(depth=50, prefix='orderbook', pivot_coins=[]) as app:
        symbols = await app.do_fut_request('GET', '/api/v1/contracts/active')

        symbols = [
            s
            for s in symbols
            if s['fundingFeeRate'] is not None and s['expireDate'] is None
        ]

        best_funds = sorted(symbols, key=lambda sym: abs(sym['fundingFeeRate'] or 0), reverse=True)
        for sym in best_funds[:20]:
            title = sym['symbol']
            rate = sym['fundingFeeRate'] * 100
            countdown = timedelta(milliseconds=sym['nextFundingRateTime']).total_seconds() // 60
            url = f'https://www.kucoin.com/trade/futures/{sym['symbol']}'
            print(f'{title}: {rate:.2f}% in {countdown} min - {url}')


if __name__ == '__main__':
    asyncio.run(main())