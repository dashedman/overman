import logging

from bot.overman import Overman

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    app = Overman(depth=50, prefix='orderbook', pivot_coins=['USDT', 'USDC'])
    app.run()
