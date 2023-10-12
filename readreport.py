# readreport.py

import gzip
import csv


def read_portfolio(filename: str) -> list[tuple[str, int, float]]:
    """Reads a file of stock data and returns list of dicts with name, shares and price."""
    portfolio = []
    with gzip.open(filename, "rt") as f:
        rows = csv.reader(f)
        headers = next(rows)
        for row in rows:
            record = {"name": row[0], "shares": int(row[1]), "price": float(row[2])}
            portfolio.append(record)
    return portfolio


if __name__ == "__main__":
    for stock in read_portfolio("Data/portfolio.csv.gz"):
        print(stock)
