from bot.overman import Overman


if __name__ == '__main__':
    app = Overman(depth=50, prefix='orderbook', pivot_coins=['USDT', 'USDC'], hf_trade=True)
    app.run()
