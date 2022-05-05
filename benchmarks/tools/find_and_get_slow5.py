#!/usr/bin/python3

import argparse
import multiprocessing as mp
from pathlib import Path
from queue import Empty

import numpy
import pandas as pd

import pyslow5


def process_read(get_columns, read, read_ids, extracted_columns):
    read_ids.append(read["read_id"])

    for c in get_columns:
        if not c in extracted_columns:
            extracted_columns[c] = []
        col = extracted_columns[c]
        if c == "sample_count":
            col.append(read["len_raw_signal"])
        elif c == "samples":
            col.append(numpy.sum(read["signal"]))
        else:
            col.append(read[c])


def do_file_work(filename, get_columns, result_q):
    read_ids = []
    extracted_columns = {"read_id": read_ids}

    not_aux_columns = ["sample_count", "samples"]
    aux_columns = list(filter(lambda x: x not in not_aux_columns, get_columns))

    file = pyslow5.Open(str(filename), "r")
    for read in file.seq_reads(pA=False, aux=aux_columns):
        process_read(get_columns, read, read_ids, extracted_columns)

    result_q.put(pd.DataFrame(extracted_columns))


def do_search_work(files, select_read_ids, get_columns, result_q):
    read_ids = []
    extracted_columns = {"read_id": read_ids}

    not_aux_columns = ["sample_count", "samples"]
    aux_columns = list(filter(lambda x: x not in not_aux_columns, get_columns))

    for file in files:
        file = pyslow5.Open(str(file), "r")

        for read in file.get_read_list(select_read_ids, pA=False, aux=aux_columns):
            process_read(get_columns, read, read_ids, extracted_columns)

    result_q.put(pd.DataFrame(extracted_columns))


def run(input_dir, output, select_read_ids=None, get_columns=[]):
    output.mkdir(parents=True, exist_ok=True)

    mp.set_start_method("spawn")

    if select_read_ids is not None:
        print(f"Selecting {len(select_read_ids)} specific read ids")
    if get_columns is not None:
        print(f"Selecting columns: {get_columns}")

    result_queue = mp.Queue()
    runners = 10

    print(f"Search for input files in {input_dir}")
    files = list(input_dir.glob("*.blow5"))
    print(f"Searching for read ids in {[str(f) for f in files]}")

    processes = []
    if select_read_ids is not None:
        approx_chunk_size = max(1, len(select_read_ids) // runners)
        start_index = 0
        while start_index < len(select_read_ids):
            select_ids = select_read_ids[start_index : start_index + approx_chunk_size]
            p = mp.Process(
                target=do_search_work,
                args=(files, select_ids, get_columns, result_queue),
            )
            p.start()
            processes.append(p)
            start_index += len(select_ids)
    else:
        for filename in files:
            p = mp.Process(
                target=do_file_work, args=(filename, get_columns, result_queue)
            )
            p.start()
            processes.append(p)

    print("Wait for processes...")
    items = []
    while len(items) < len(processes):
        try:
            item = result_queue.get(timeout=0.5)
            items.append(item)
        except Empty:
            continue

    for p in processes:
        p.join()

    df = pd.concat(items)
    print(f"Selected {len(df)} items")
    df.to_csv(output / "read_ids.csv", index=False)

    """
    read_ids = []
    extracted_columns = {"read_id": read_ids}
    for file in input_dir.glob("*.blow5"):
        print(f"Searching for read ids in {file}")

        file = pyslow5.Open(str(file), "r")

        if select_read_ids is not None:
            iterable = file.get_read_list(select_read_ids, pA=False, aux=aux_columns)
        else:
            iterable = file.seq_reads(pA=False, aux=aux_columns)

    """


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--select-ids",
        type=str,
        help="CSV file with a read_id column, listing ids to find in input files",
    )
    parser.add_argument(
        "--get-column",
        default=[],
        nargs="+",
        type=str,
        help="Add columns that should be extacted",
    )

    args = parser.parse_args()

    select_read_ids = None
    if args.select_ids:
        select_read_ids = pd.read_csv(args.select_ids)["read_id"]

    run(
        args.input,
        args.output,
        select_read_ids=select_read_ids,
        get_columns=args.get_column,
    )


if __name__ == "__main__":
    main()
