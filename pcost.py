# pcost.py - Calculates the cost of a portfolio of stocks from a datafile

from typing import TextIO


def calc_cost(file_handle: TextIO) -> float:
    """Calculates the cost of a portfolio of stocks in a datafile"""
    cost = 0.0
    for line in file_handle.readlines():
        try:
            symbol, shares, price = line.split(" ")
            cost += int(shares) * float(price)
        except ValueError as ve:
            print(f"Unable to calculate cost of {symbol} from {shares} * {price}")
    return cost


if __name__ == "__main__":
    filename = "Data/portfolio.dat"
    with open(filename) as f:
        cost = calc_cost(f)
    print(f"Portfolio cost is ${cost:>,.2f}")
