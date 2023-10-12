# read_bus_rides.py

import csv


def read_rides(filename: str, func: Callable) -> DataCollection:
    """Reads in the CTA bus data, using a function to build the data container.

    Args:
        filename: The relative path to the data file
        func:     The function to construct the data container

    Return:
        A list of data structures or class instances for a row of data.
    """

    records = []
    with open(filename) as f:
        rows = csv.reader(f)
        headings = next(rows)
        for row in rows:
            record = func(row)
            records.append(record)
    return records
