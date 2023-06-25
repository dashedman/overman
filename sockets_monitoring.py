import asyncio
import json
from itertools import islice

import websockets


def chunk(raw_list, size=10):
    raw_list = iter(raw_list)
    return list(iter(lambda: tuple(islice(raw_list, size)), ()))


def prepare_sub(subs_chunk: list[str]):
    return {
        "req_id": "test",
        "op": "subscribe",
        "args": subs_chunk
    }


async def monitor_socket(chunk_numb: int, subs: list[str]):
    url = "wss://stream-testnet.bybit.com/v5/public/spot"
    async for sock in websockets.connect(url):
        try:
            if subs:
                sub = prepare_sub(subs)
                pairs = json.dumps(sub)
                await sock.send(pairs)
            while True:
                try:
                    order_book_raw: str = await sock.recv()
                    order_book: dict = json.loads(order_book_raw)
                    print(f'{chunk_numb}: {order_book}')
                except Exception as e:
                    print(e)
                    break
        except websockets.ConnectionClosed:
            ...


async def main(tickers: list[str]):

    ticker_chunks = chunk(tickers)
    depth = 50
    prefix = 'orderbook'

    all_subs = []

    for ticker_ch in ticker_chunks:
        sub_chunk = []
        for tick in ticker_ch:
            sub_chunk.append(f"{prefix}.{depth}.{tick}")
        all_subs.append(sub_chunk)

    tasks = [
        asyncio.create_task(
            monitor_socket(numb, ch)) for numb, ch in enumerate(all_subs)]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    tickers = [
        "BTCUSDT", "BTCETH", "BTCUSDC", "BTCBIT", "ETHUSDT", "ETHBTC", "ETHUSDC",
        "ETHBIT", "ETHDYDX", "EOSUSDT", "EOSETH", "EOSBIT", "EOSDYDX", "NEOBTC",
        "NEODYDX", "NEOEOS", "NEOETH", "NEOBIT", "NEODOGE", "NEOADA", "XRPUSDT",
        "XRPBTC", "XRPEOS", "XRPBIT", "XRPDYDX", "XRPUSDC", "XRPDAI", "DOGEUSDT",
        "DOGEDYDX", "DOGEBRL", "BITUSDT", "BITETH", "BITUSDC", "BITBTG", "ADAUSDT",
        "ADAUSDC", "DYDXUSDT", "DYDXETH", "DYDXXRP", "DYDXNEO", "DYDXDOGE",
        "USDCUSDT", "USDTBIT", "USDTDAI", "USDTETH", "USDTBTG", "DAIUSDT", "BTGETH",
        "BTGUSDC", "BTGBIT", "BRLUSDT"
    ]
    asyncio.run(main(tickers))
