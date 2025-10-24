import asyncio
from datetime import timedelta

from bot.funda import Funda, FutPairInfo


def get_avg_rate(symbols: list[FutPairInfo]):
    return sum(abs(sym.funding_fee_rate) for sym in symbols) / len(symbols)


async def main():
    async with Funda() as app:
        best_funds = await app.get_funding_fee_symbols(sort_best=True)

        print('Check history you can here: https://www.kucoin.com/order/futures/funds-history')
        print(f'TOP 3 AVG RATE: {get_avg_rate(best_funds[:3]) * 100:.2f}%')
        print(f'TOP 5 AVG RATE: {get_avg_rate(best_funds[:5]) * 100:.2f}%')
        print(f'TOP 10 AVG RATE: {get_avg_rate(best_funds[:10]) * 100:.2f}%')
        print(f'TOP 20 AVG RATE: {get_avg_rate(best_funds[:20]) * 100:.2f}%')
        print(f'Minimal fee: {best_funds[-1].funding_fee_rate * 100}%')
        for sym in best_funds[:20]:
            title = sym.symbol
            rate = sym.funding_fee_rate * 100
            countdown = timedelta(milliseconds=sym.next_funding_rate_time).total_seconds() // 60
            url = f'https://www.kucoin.com/trade/futures/{sym.symbol}'
            print(f'{title}: {rate:.2f}% in {countdown} min - {url}')


if __name__ == '__main__':
    asyncio.run(main())