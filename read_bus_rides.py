# read_bus_rides.py

import csv
from collections import namedtuple
from dataclasses import dataclass
from typing import Callable, TypeVar, List, Tuple, Dict, NamedTuple, Union, Callable


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


T = TypeVar("T")


def read_rides(filename: str, func: Callable[..., T]) -> List[T]:
    """Reads in the CTA bus data, using a function to build the data container.

    Args:
        filename: The relative path to the data file
        func:     The function to construct the data container

    Return:
        A list of data structures or class instance containing a row of data.
    """

    records = []
    with open(filename) as f:
        rows = csv.reader(f)
        headings = next(rows)
        for row in rows:
            record = func(row)
            records.append(record)
    return records
