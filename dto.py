from dataclasses import dataclass, field


@dataclass
class OrderBookPair:
    price: float
    count: float

    def __hash__(self):
        return hash(self.price)

    def __eq__(self, other):
        return self.price == other.price

    def __lt__(self, other):
        return self.price < other.price


@dataclass
class BestOrders:
    asks: set['OrderBookPair'] = field(default=set)
    bids: set['OrderBookPair'] = field(default=set)

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
