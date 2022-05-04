#!/usr/bin/python3

import argparse
import multiprocessing as mp
from pathlib import Path
import pickle
from queue import Empty
from uuid import UUID

import numpy
import pandas as pd

import mkr_format


def select_reads(file, selection):
    if selection is not None:
        return file.select_reads(UUID(s) for s in selection)
    else:
        return file.reads()


def process_read(get_columns, read, read_ids, extracted_columns):
    read_ids.append(read.read_id)

    for c in get_columns:
        if not c in extracted_columns:
            extracted_columns[c] = []
        col = extracted_columns[c]
        if c == "samples":
            col.append(numpy.sum(read.signal))
        else:
            col.append(getattr(read, c))


def do_batch_bulk_work(
    filename, batches, select_read_ids, get_columns, c_api, result_q
):
    read_ids = []
    extracted_columns = {"read_id": read_ids}

    file = mkr_format.open_combined_file(filename, use_c_api=c_api)
    for batch in batches:
        for read in file.get_batch(batch).reads():
            process_read(get_columns, read, read_ids, extracted_columns)

    result_q.put(pd.DataFrame(extracted_columns))


def do_batch_search_work(
    filename, batches, select_read_ids_pickled, get_columns, c_api, result_q
):
    read_ids = []
    extracted_columns = {"read_id": read_ids}

    select_read_ids = pickle.loads(select_read_ids_pickled)

    file = mkr_format.open_combined_file(filename, use_c_api=c_api)
    for batch in batches:
        for read in filter(
            lambda x: x.read_id in select_read_ids, file.get_batch(batch).reads()
        ):
            process_read(get_columns, read, read_ids, extracted_columns)

    result_q.put(pd.DataFrame(extracted_columns))


def run(input_dir, output, select_read_ids=None, get_columns=[], c_api=False):
    output.mkdir(parents=True, exist_ok=True)

    mp.set_start_method("spawn")

    if select_read_ids is not None:
        print(f"Selecting {len(select_read_ids)} specific read ids")
    if get_columns is not None:
        print(f"Selecting columns: {get_columns}")

    result_queue = mp.Queue()
    runners = 10

    print(f"Search for input files in {input_dir}")
    files = list(input_dir.glob("*.mkr"))
    print(f"Searching for read ids in {[str(f) for f in files]}")

    fn_to_call = do_batch_bulk_work
    if select_read_ids is not None:
        fn_to_call = do_batch_search_work

    select_read_ids = pickle.dumps(
        set(UUID(s) for s in select_read_ids) if select_read_ids is not None else None
    )

    processes = []
    for filename in files:
        file = mkr_format.open_combined_file(filename, use_c_api=c_api)
        batches = list(range(file.batch_count))
        approx_chunk_size = max(1, len(batches) // runners)
        start_index = 0
        while start_index < len(batches):
            select_batches = batches[start_index : start_index + approx_chunk_size]
            p = mp.Process(
                target=fn_to_call,
                args=(
                    filename,
                    select_batches,
                    select_read_ids,
                    get_columns,
                    c_api,
                    result_queue,
                ),
            )
            p.start()
            processes.append(p)
            start_index += len(select_batches)

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
    parser.add_argument(
        "--c-api", action="store_true", help="Use C API rather than PyArrow"
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
        c_api=args.c_api,
    )


if __name__ == "__main__":
    main()
