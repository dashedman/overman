import asyncio

from bot.overman import Overman


async def main():
    app = Overman(depth=50, prefix='orderbook', pivot_coins=[])
    await app.inner_transfer('USDT', 'trade', 'trade_hf', '10')


if __name__ == '__main__':
    asyncio.run(main())
