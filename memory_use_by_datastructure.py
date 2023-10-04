# readrides.py - Finds which data structures are the most memory efficient.
# Uses multiprocessing to construct a large list of various data structures.
#
# Results
# ----------------------------------------------------------------------------------
# SLOTS     : Memory: 134,210,117 bytes  Instances: 577,563  Object Size: 233 bytes
# TUPLE     : Memory: 138,830,457 bytes  Instances: 577,563  Object Size: 241 bytes
# NAMEDTUPLE: Memory: 143,451,193 bytes  Instances: 577,563  Object Size: 249 bytes
# CLASS     : Memory: 185,035,469 bytes  Instances: 577,563  Object Size: 321 bytes
# DATACLASS : Memory: 185,035,480 bytes  Instances: 577,563  Object Size: 321 bytes
# DICTIONARY: Memory: 231,240,417 bytes  Instances: 577,563  Object Size: 401 bytes
# ----------------------------------------------------------------------------------


import csv
import math
from collections import namedtuple
from multiprocessing import Process, Manager, Queue
from dataclasses import dataclass


class Row:
    def __init__(self, row) -> None:
        self.route = row[0]
        self.date = row[1]
        self.daytype = row[2]
        self.rides = row[3]


class SlotsRow:
    __slots__ = ["route", "date", "daytype", "rides"]

    def __init__(self, row) -> None:
        self.route = row[0]
        self.date = row[1]
        self.daytype = row[2]
        self.rides = row[3]


@dataclass
class DataclassRow:
    route: str
    date: str
    daytype: str
    rides: str


NamedtupleRow = namedtuple("Row", ["route", "date", "daytype", "rides"])


def build_tuple(row):
    route = row[0]
    date = row[1]
    daytype = row[2]
    rides = row[3]
    return (route, date, daytype, rides)


def build_dictionary(row):
    return {"route": row[0], "date": row[1], "daytype": row[2], "rides": row[3]}


def build_namedtuple(row):
    return NamedtupleRow(row[0], row[1], row[2], row[3])


def build_class(row):
    return Row(row)


def build_slots(row):
    return SlotsRow(row)


def build_dataclass(row):
    return DataclassRow(row[0], row[1], row[2], row[3])


def read_rides(filename, func):
    records = []
    with open(filename) as f:
        rows = csv.reader(f)
        headings = next(rows)
        for row in rows:
            record = func(row)
            records.append(record)
    return records


def measure_memory(filename, func, queue):
    import tracemalloc

    tracemalloc.start()
    rows = read_rides(filename, func)
    current, peak = tracemalloc.get_traced_memory()
    queue.put(
        (func, current, peak, len(rows), math.ceil(current / len(rows))), block=False
    )


if __name__ == "__main__":
    functions = [
        build_dictionary,
        build_namedtuple,
        build_tuple,
        build_class,
        build_slots,
        build_dataclass,
    ]

    procs, results = [], []
    que = Queue()  # Used for inter-process communication
    for func in functions:
        proc = Process(target=measure_memory, args=("Data/ctabus.csv", func, que))
        procs.append(proc)
        proc.start()

    for proc in procs:
        result = que.get()
        results.append(result)
        proc.join()

    results = sorted(results, key=lambda x: x[1])
    for func, current, peak, rows, bytes_per_object in results:
        print(
            f"{func.__name__[6:].upper():<10}: Memory: {current:,} bytes  Instances: {rows:,}  Object Size: {bytes_per_object:,} bytes"
        )
