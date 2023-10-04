"""readrides.py - Finds which data structures are the most memory efficient.
Using city bus route data from the Chicago Transit Authority (CTA) containing 
over half-a-million rows (trips) to build a large collection of objects. 
The tracemalloc package determines how much the memory is being consumed.  
To make accurate measurements of memory consumption, the multiprocessing package is 
used to start a separate process for creation of each different type of object collection.

Results:
------------------------------------------------------------------------------------------
SLOTS     :  Memory: 128.0 MB  Peak: 128.0 MB  Instances: 577,563  Object Size: 233 bytes
TUPLE     :  Memory: 132.4 MB  Peak: 132.4 MB  Instances: 577,563  Object Size: 241 bytes
NAMEDTUPLE:  Memory: 136.8 MB  Peak: 136.8 MB  Instances: 577,563  Object Size: 249 bytes
DATACLASS :  Memory: 176.5 MB  Peak: 176.5 MB  Instances: 577,563  Object Size: 321 bytes
CLASS     :  Memory: 176.5 MB  Peak: 176.5 MB  Instances: 577,563  Object Size: 321 bytes
DICTIONARY:  Memory: 220.5 MB  Peak: 220.6 MB  Instances: 577,563  Object Size: 401 bytes
------------------------------------------------------------------------------------------
"""


import csv
import math
from collections import namedtuple
from multiprocessing import Process, Queue
from dataclasses import dataclass
from typing import List, Tuple, Dict, NamedTuple, Union, Callable


class Row:
    """A row (tuple) of CTA bus data using a plain-python class.

    Attributes:
        route:   Column 1. The bus route ID number
        date:    Column 2. A date string (MM/DD/YYYY)
        daytype: Column 3. (U=Sunday/Holiday, A=Saturday, W=Weekday)
        rides:   Column 4. Total number of rides that day (integer)
    """

    def __init__(self, row) -> None:
        self.route = row[0]
        self.date = row[1]
        self.daytype = row[2]
        self.rides = row[3]


class SlotsRow:
    """A row (tuple) of CTA bus data using a python class with slots.

    Attributes:
        route:   Column 1. The bus route ID number
        date:    Column 2. A date string (MM/DD/YYYY)
        daytype: Column 3. (U=Sunday/Holiday, A=Saturday, W=Weekday)
        rides:   Column 4. Total number of rides that day (integer)
    """

    __slots__ = ["route", "date", "daytype", "rides"]

    def __init__(self, row) -> None:
        self.route = row[0]
        self.date = row[1]
        self.daytype = row[2]
        self.rides = row[3]


@dataclass
class DataclassRow:
    """A row (tuple) of CTA bus data stored in a dataclass.

    Attributes:
        route:   Column 1. The bus route ID number
        date:    Column 2. A date string (MM/DD/YYYY)
        daytype: Column 3. (U=Sunday/Holiday, A=Saturday, W=Weekday)
        rides:   Column 4. Total number of rides that day (integer)
    """

    route: str
    date: str
    daytype: str
    rides: str


DataCollection = List[Union[Dict, Tuple, NamedTuple, Row, SlotsRow, DataclassRow]]


def build_tuple(row: List[str]) -> Tuple[str, str, str, str]:
    """Constructs a tuple containing row of bus data.
    Args:
        A row (tuple) of CTA bus data.

    Return:
        A tuple("route", "date", "daytype", "rides").
    """
    route = row[0]
    date = row[1]
    daytype = row[2]
    rides = row[3]
    return (route, date, daytype, rides)


def build_dictionary(row: List[str]) -> Dict[str, str]:
    """Constructs a dictionary containing row of bus data.
    Args:
        A row (tuple) of CTA bus data.

    Return:
        A dict containing the route, date, daytype, and rides.
    """
    return {"route": row[0], "date": row[1], "daytype": row[2], "rides": row[3]}


NamedtupleRow = namedtuple("NamedtupleRow", ["route", "date", "daytype", "rides"])


def build_namedtuple(row: List[str]) -> NamedtupleRow:
    """Constructs a namedtuple containing row of bus data.
    Args:
        A row (tuple) of CTA bus data.

    Return:
        A namedtuple containing the route, date, daytype, and rides.
    """
    return NamedtupleRow(row[0], row[1], row[2], row[3])


def build_class(row: List[str]) -> Row:
    """Constructs a class instance representing a row of bus data.
    Args:
        A row (tuple) of CTA bus data.

    Return:
        A class containing the route, date, daytype, and rides attributes.
    """
    return Row(row)


def build_slots(row: List[str]) -> SlotsRow:
    """Constructs a memory efficient class instance representing a row of bus data.
    Args:
        A row (tuple) of CTA bus data.

    Return:
        A memory efficient class containing the route, date, daytype, and rides attributes.
    """
    return SlotsRow(row)


def build_dataclass(row: List[str]) -> DataclassRow:
    """Constructs a dataclass instance representing a row of bus data.
    Args:
        A row (tuple) of CTA bus data.

    Return:
        A dataclass containing the route, date, daytype, and rides attributes.
    """
    return DataclassRow(row[0], row[1], row[2], row[3])


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


def measure_memory(filename: str, func: Callable, queue: Queue) -> None:
    """Quantifies the current and peak memory needed to store a large collection of objects.

    Args:
        filename: The CTA bus datafile to ingest
        func:     Creates the data structure or class instance to hold the data
        queue:    Used to communicate memory consumption values"""
    import tracemalloc

    tracemalloc.start()
    rows = read_rides(filename, func)
    current, peak = tracemalloc.get_traced_memory()
    MEGA_BYTE_SIZE = 2**20
    # IPC communication from spawned process
    queue.put(
        (
            func,
            round(current / MEGA_BYTE_SIZE, 1),
            round(peak / MEGA_BYTE_SIZE, 1),
            len(rows),
            math.ceil(current / len(rows)),
        ),
        block=False,
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

    procs: List[Process] = []
    results = []
    que: Queue = Queue()  # Used for IPC communications
    for func in functions:
        proc = Process(target=measure_memory, args=("Data/ctabus.csv", func, que))
        procs.append(proc)
        proc.start()

    for proc in procs:
        result = que.get()  # Get data from the spawned processes via IPC
        results.append(result)
        proc.join()

    results = sorted(results, key=lambda x: x[1])
    for func, current, peak, rows, bytes_per_object in results:
        print(
            f"{func.__name__[6:].upper():<10}:  Memory: {current} MB  Peak: {peak} MB  Instances: {rows:,}  Object Size: {bytes_per_object:,} bytes"
        )
