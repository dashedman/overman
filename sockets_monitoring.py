import asyncio
import json
from collections import defaultdict
from itertools import islice

import websockets
import dto


def chunk(raw_list, size=10):
    raw_list = iter(raw_list)
    return list(iter(lambda: tuple(islice(raw_list, size)), ()))


def prepare_sub(subs_chunk: list[str]=None):
    return {
        "id": "test",
        "type": "subscribe",
        "topic": "/market/level2:BTC-USDT",
        "response": True
    }


order_book_by_ticker: dict[str, set['dto.OrderBookPair']] = defaultdict(set)


def handle_raw_orderbook(
        raw_orderbook: dict[str, str | dict[str, str | list[str]]]):
    """
    Example snapshot:

    {'topic': 'orderbook.50.DYDXUSDT', 'ts': 1687687095565, 'type': 'snapshot',
     'data': {'s': 'DYDXUSDT', 'b': [['2.001', '360.697'], ['1.971', '292.256'],
      ['1.97', '453.245'], ['1.968', '672.694'], ['1.967', '690.698'],
       ['1.966', '743.749'], ['1.963', '261.32'], ['1.962', '559.267']],
        'a': [['2.003', '506.542'], ['2.033', '574.77'], ['2.034', '424.14'],
         ['2.035', '292.429'], ['2.036', '627.188'], ['2.037', '640.978'],
          ['2.038', '399.721'], ['2.039', '732.748'], ['2.041', '631.048'],
           ['2.042', '669.269'], ['2.043', '517.712'], ['2.171', '30.765'],
            ['2.172', '38.137'], ['2.173', '35.01'], ['2.174', '58.356']],
             'u': 19047, 'seq': 499518832}}

    Example delta:

    {'topic': 'orderbook.50.DYDXUSDT', 'ts': 1687687108145, 'type': 'delta',
     'data': {'s': 'DYDXUSDT', 'b': [['1.963', '261.32'], ['1.965', '0'],
      ['1.969', '0']], 'a': [['2.033', '554.77']], 'u': 19058, 'seq': 499518911}
      }
    """
    ob_type = raw_orderbook['type']
    ob_symbol: str = raw_orderbook['data']['s']
    raw_ask_data = raw_orderbook['data']['a']
    prepared_ask_data: set['dto.OrderBookPair'] = {
        dto.OrderBookPair(float(orderbook_el[0]), float(orderbook_el[1]))
        for orderbook_el in raw_ask_data
    }
    if ob_type == 'snapshot':
        order_book_by_ticker[ob_symbol] = prepared_ask_data
    else:
        for p_ask in prepared_ask_data:
            if p_ask in order_book_by_ticker[ob_symbol]:
                order_book_by_ticker[ob_symbol].remove(p_ask)
                if p_ask.count != 0:
                    order_book_by_ticker[ob_symbol].add(p_ask)
            else:
                order_book_by_ticker[ob_symbol].add(p_ask)

    print(f'{ob_type},'
          f' symbol: {ob_symbol}, data: {order_book_by_ticker[ob_symbol]}')


async def monitor_socket(subs: list[str]=None):
    url = "wss://ws-api-spot.kucoin.com/?token=2neAiuYvAU61ZDXANAGAsiL4-iAExhsBXZxftpOeh_55i3Ysy2q2LEsEWU64mdzUOPusi34M_wGoSf7iNyEWJ61c9cBSu34minkkRmbD8qeAA2RniAeBpNiYB9J6i9GjsxUuhPw3BlrzazF6ghq4L-l3quUQN_RjW3ZkgQd2kJw=.dWwmgZmsM8pLO2ZqXYviZA=="
    async for sock in websockets.connect(url):
        try:
            # if subs:
            sub = prepare_sub()
            pairs = json.dumps(sub)
            await sock.send(pairs)
            while True:
                try:
                    orderbook_raw: str = await sock.recv()
                    orderbook: dict = json.loads(orderbook_raw)
                    print(1)
                    # orderbook_type = orderbook.get('type')
                    # if orderbook_type:
                    #     handle_raw_orderbook(orderbook)
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



    await asyncio.gather(asyncio.create_task(monitor_socket()))

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
