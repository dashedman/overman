from bot.overman import Overman


if __name__ == '__main__':
    app = Overman(depth=3, prefix='orderbook', pivot_coins=['USDT', 'USDC'], hf_trade=True, only_show=False)
    app.run()
