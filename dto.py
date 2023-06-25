from dataclasses import dataclass


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
