# pcost.py - Calculates the cost of a portfolio of stocks from a datafile

from typing import TextIO
import re


def calc_cost(file_handle: TextIO) -> float:
    """Read the datafile and calculates the cost of a portfolio"""
    cost = 0.0
    for line in file_handle.readlines():
        try:
            # use one of the delimiters contained inside the braces
            symbol, shares, price = re.split(r"[ ,]+", line)
            cost += int(shares) * float(price)
        except ValueError as ve:
            print(
                f"Unable to calculate cost for stock: {symbol}, shares: {shares}, price: {price}"
            )
    return cost


def portfolio_cost(filename: str) -> float:
    """Opens the portfolio datafile and uses a helper function to calcuate its cost"""
    cost = 0.0
    try:
        with open(filename) as f:
            cost = calc_cost(f)
    except FileNotFoundError:
        print(f"{filename} not found.")
    finally:
        print(f"Portfolio cost is ${cost:>,.2f}")
        return cost


if __name__ == "__main__":
    filename = "Data/portfolio.dat"
    filename_comma_sep = "Data/portfolio2.dat"
    filename_with_errors = "Data/portfolio3.dat"
    portfolio_cost(filename)
