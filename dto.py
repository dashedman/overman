from decimal import Decimal
from dataclasses import dataclass, field
from typing import Collection

from sortedcontainers import SortedList, SortedDict


@dataclass
class OrderBookPair:
    price: Decimal = field(default=Decimal(0.0))
    count: Decimal = field(default=Decimal(0.0))

    def __hash__(self):
        return hash(self.price)

    def __eq__(self, other):
        return self.price == other.price

    def __lt__(self, other):
        return self.price < other.price


@dataclass
class AbstractOrders:
    asks: Collection['OrderBookPair']     # sell orders
    bids: Collection['OrderBookPair']     # buy orders

    @property
    def best_ask(self):
        return min(self.asks) if self.asks else None

    @property
    def best_bid(self):
        return max(self.bids) if self.bids else None

    @property
    def bids_empty(self):
        return not self.bids

    @property
    def asks_empty(self):
        return not self.asks

    @property
    def is_relevant(self):
        return not (self.asks_empty or self.bids_empty)

    def ensure_sorted(self):
        assert self.asks == sorted(self.asks)
        assert self.bids == sorted(self.bids, reverse=True)


@dataclass
class BestOrders(AbstractOrders):
    asks: list[OrderBookPair] = field(default_factory=list)     # sell orders
    bids: list[OrderBookPair] = field(default_factory=list)     # buy orders


Price = Decimal
Size = Decimal


@dataclass
class FullOrders(AbstractOrders):
    asks: SortedDict[Price, Size] = field(default_factory=SortedDict)     # sell orders
    bids: SortedDict[Price, Size] = field(default_factory=SortedDict)     # buy orders
    last_sequence: int = field(default_factory=int)

