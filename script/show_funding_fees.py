import asyncio
from datetime import datetime, UTC, timedelta
from pprint import pprint

from bot.overman import Overman


def get_avg_rate(symbols):
    return sum(abs(sym['fundingFeeRate']) for sym in symbols) / len(symbols)


async def main():
    async with Overman(depth=50, prefix='orderbook', pivot_coins=[]) as app:
        symbols = await app.do_fut_request('GET', '/api/v1/contracts/active')

        symbols = [
            s
            for s in symbols
            if s['fundingFeeRate'] is not None and s['expireDate'] is None
        ]

        best_funds = sorted(symbols, key=lambda sym: abs(sym['fundingFeeRate'] or 0), reverse=True)

        print('Check history you can here: https://www.kucoin.com/order/futures/funds-history')
        print(f'TOP 3 AVG RAtE: {get_avg_rate(best_funds[:3]) * 100:.2f}%')
        print(f'TOP 5 AVG RAtE: {get_avg_rate(best_funds[:5]) * 100:.2f}%')
        print(f'TOP 10 AVG RAtE: {get_avg_rate(best_funds[:10]) * 100:.2f}%')
        print(f'TOP 20 AVG RAtE: {get_avg_rate(best_funds[:20]) * 100:.2f}%')
        for sym in best_funds[:20]:
            title = sym['symbol']
            rate = sym['fundingFeeRate'] * 100
            countdown = timedelta(milliseconds=sym['nextFundingRateTime']).total_seconds() // 60
            url = f'https://www.kucoin.com/trade/futures/{sym['symbol']}'
            print(f'{title}: {rate:.2f}% in {countdown} min - {url}')


if __name__ == '__main__':
    asyncio.run(main())