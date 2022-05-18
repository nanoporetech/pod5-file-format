#!/usr/bin/python3

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("--output-dir", type=Path, default="/benchmark/outputs/")

    time_args = []
    remaining = []
    for i, a in enumerate(sys.argv):
        if a == "--":
            time_args = sys.argv[:i]
            remaining = sys.argv[i + 1 :]

    args = parser.parse_args(time_args[1:])

    t_start = time.time()
    print(f"####    Timing command: {' '.join(remaining)}, start: {t_start}")
    p = subprocess.run(remaining, check=True)
    t_end = time.time()

    duration = t_end - t_start
    print(f"####    Command took {duration:.1f} secs, end: {t_start}")

    with open(args.output_dir / f"{args.name}_results.json", "w") as f:
        f.write(json.dumps({"duration_secs": duration}, indent=2))
