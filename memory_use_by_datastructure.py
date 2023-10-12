"""memory_use_by_datastructure.py - Finds which data structures are the most memory efficient.
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
from multiprocessing import Process, Queue
from typing import Callable, List
from read_bus_rides import (
    Row,
    DataclassRow,
    SlotsRow,
    read_rides,
    build_class,
    build_dataclass,
    build_dictionary,
    build_namedtuple,
    build_slots,
    build_tuple,
)


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
