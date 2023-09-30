# art.py

import sys
import random

chars = "\|/"


def draw(rows, columns):
    for row in rows:
        print("".join(random.choice(chars) for _ in range(columns)))


if __name__ == "__main__":
    if len(sys.args) != 3:
        raise SystemExit("Usage: art.py rows columns")
    draw(sys.argv[1], sys.argv[2])
