# stock.py - Represent stocks as objects


class Stock:
    def __init__(self, symbol, shares, price):
        self.symbol = symbol
        self.shares = shares
        self.price = price

    def cost(self):
        return self.shares * self.price
