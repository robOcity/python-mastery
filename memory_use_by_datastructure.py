# readrides.py - Finds which data structures are the most memory efficient.
# Uses multiprocessing to construct a large list of various data structures.
#
# Results
# ------------------------------------------------------------------------------
# BUILD_SLOTS          bytes: Current 134,209,781 bytes,  Peak 134,240,131 bytes
# BUILD_TUPLE          bytes: Current 138,830,049 bytes,  Peak 138,860,399 bytes
# BUILD_NAMEDTUPLE     bytes: Current 143,450,785 bytes,  Peak 143,481,135 bytes
# BUILD_DATACLASS      bytes: Current 185,035,144 bytes,  Peak 185,065,494 bytes
# BUILD_CLASS          bytes: Current 185,035,133 bytes,  Peak 185,065,483 bytes
# BUILD_DICTIONARY     bytes: Current 231,238,905 bytes,  Peak 231,269,255 bytes
# ------------------------------------------------------------------------------


import csv
from collections import namedtuple
from multiprocessing import Process
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


def measure_memory(filename, func):
    import tracemalloc

    tracemalloc.start()
    rows = read_rides(filename, func)
    current, peak = tracemalloc.get_traced_memory()
    print(
        f"{func.__name__[6:].upper():<10}: Current {current:,} bytes,  Peak {peak:,} bytes"
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
    procs = []

    # instantiate processes
    for func in functions:
        proc = Process(target=measure_memory, args=("Data/ctabus.csv", func))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()
