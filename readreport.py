# readreport.py

import gzip
import csv
from typing import TypedDict


class TypedStock(TypedDict):
    name: str
    shares: int
    price: float


def read_portfolio(
    filename: str,
) -> list[TypedStock]:
    """Reads a file of stock data and returns list of TypedStock objects."""
    portfolio = []
    with gzip.open(filename, "rt") as f:
        rows = csv.reader(f)
        headers = next(rows)
        for row in rows:
            record = TypedStock(name=row[0], shares=int(row[1]), price=float(row[2]))
            portfolio.append(record)
    return portfolio


if __name__ == "__main__":
    for stock in read_portfolio("Data/portfolio.csv.gz"):
        print(stock)
