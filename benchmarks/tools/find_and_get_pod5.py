#!/usr/bin/env python3

import argparse
import multiprocessing as mp
import tempfile
from collections import namedtuple
from pathlib import Path
from queue import Empty

import numpy
import pandas as pd

import pod5 as p5

SelectReadIdsData = namedtuple(
    "SelectReadIdsData", ["path", "slice_start", "slice_end", "shape"]
)


def load_mapped_ids(select_read_ids_data):
    """Load a set of read ids from a mmapped file on disk"""
    select_read_ids_all = numpy.memmap(
        select_read_ids_data.path,
        dtype=numpy.uint8,
        mode="r+",
        shape=select_read_ids_data.shape,
    )
    return select_read_ids_all[
        select_read_ids_data.slice_start : select_read_ids_data.slice_end
    ]


def do_batch_work(filename, batches, column, mode, result_q):
    """
    Per process worker to do loading of data from a set of batches
    """

    read_ids = []
    vals = []
    extracted_columns = {"read_id": read_ids, column: vals}

    if column == "samples":
        file = p5.Reader(filename)
        for batch in file.read_batches(batch_selection=batches, preload={"samples"}):
            read_ids.extend(p5.format_read_ids(batch.read_id_column))

            for read in batch.reads():
                vals.append(numpy.sum(read.signal))
    else:
        print(f"Unknown column {column}")
    result_q.put(pd.DataFrame(extracted_columns))


def do_search_work(files, select_read_ids_data, column, mode, result_q):
    """
    Per process worker to do loading of data from a number of read ids
    """
    select_read_ids = load_mapped_ids(select_read_ids_data)

    read_ids = []
    vals = []
    extracted_columns = {"read_id": read_ids, column: vals}

    if column == "samples":
        for filename in files:
            file = p5.Reader(filename)
            for batch in file.read_batches(select_read_ids, preload={"samples"}):
                read_ids.extend(p5.format_read_ids(batch.read_id_column))
                vals.extend([numpy.sum(s) for s in batch.cached_samples_column])
    else:
        print(f"Unknown column {column}")
    result_q.put(pd.DataFrame(extracted_columns))


def run_multiprocess(files, output, select_read_ids=None, column=None, mode=None):
    """
    Do work across a number of python multiprocesses
    """
    mp.set_start_method("spawn")

    if select_read_ids is not None:
        print("Placing select read id data on disk for mmapping:")
        numpy_select_read_ids = p5.pack_read_ids(select_read_ids)

        # Copy data to memory-map
        fp = tempfile.NamedTemporaryFile()
        fp.close()
        mapped_select_read_ids = numpy.memmap(
            fp.name, dtype=numpy.uint8, mode="w+", shape=numpy_select_read_ids.shape
        )
        numpy.copyto(mapped_select_read_ids, numpy_select_read_ids)
        select_read_ids_mmap_path = Path(fp.name)

    result_queue = mp.Queue()
    runners = 10

    processes = []
    if select_read_ids is not None:
        approx_chunk_size = max(1, len(select_read_ids) // runners)
        start_index = 0
        while start_index < len(select_read_ids):
            select_read_ids_data = SelectReadIdsData(
                select_read_ids_mmap_path,
                start_index,
                start_index + approx_chunk_size,
                numpy_select_read_ids.shape,
            )

            p = mp.Process(
                target=do_search_work,
                args=(files, select_read_ids_data, column, mode, result_queue),
            )
            p.start()
            processes.append(p)
            start_index += approx_chunk_size
    else:
        for filename in files:
            file = p5.Reader(filename)
            batches = list(range(file.batch_count))
            approx_chunk_size = max(1, len(batches) // runners)
            start_index = 0
            while start_index < len(batches):
                select_batches = batches[start_index : start_index + approx_chunk_size]
                p = mp.Process(
                    target=do_batch_work,
                    args=(filename, select_batches, column, mode, result_queue),
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

    return pd.concat(items)

    if select_read_ids is not None:
        select_read_ids_mmap_path.unlink()


def run_get_read_ids(files):
    """
    Load all read ids from the file.
    """
    read_ids = []
    for filename in files:
        file = p5.Reader(filename)
        for batch in file.read_batches():
            read_ids.extend(p5.format_read_ids(batch.read_id_column))
    return pd.DataFrame({"read_id": read_ids})


def run_select(files, select_read_ids, column):
    """
    Load column from a specific set of read ids
    """
    read_ids = []
    vals = []
    extracted_columns = {"read_id": read_ids, column: vals}

    for filename in files:
        file = p5.Reader(filename)
        if column == "sample_count":
            for batch in file.read_batches(select_read_ids, preload={"sample_count"}):
                read_id_selection = batch.read_id_column
                read_ids.extend(p5.format_read_ids(read_id_selection))
                vals.extend(batch.cached_sample_count_column)
        else:
            col_name = f"{column}_column"
            for batch in file.read_batches(select_read_ids):
                read_id_selection = batch.read_id_column
                read_ids.extend(p5.format_read_ids(read_id_selection))

                read_number_selection = getattr(batch, col_name)
                vals.extend(read_number_selection)

    return pd.DataFrame(extracted_columns)


def run_batched(files, column):
    """
    Load column from a all reads
    """
    read_ids = []
    vals = []
    extracted_columns = {"read_id": read_ids, column: vals}

    for filename in files:
        file = p5.Reader(filename)
        if column == "sample_count":
            for batch in file.read_batches(preload={"sample_count"}):
                read_ids.extend(p5.format_read_ids(batch.read_id_column))
                vals.extend(batch.cached_sample_count_column)
        else:
            col_name = f"{column}_column"
            for batch in file.read_batches():
                read_ids.extend(p5.format_read_ids(batch.read_id_column))
                vals.extend(getattr(batch, col_name).to_numpy())

    return pd.DataFrame(extracted_columns)


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
        default=None,
        type=str,
        help="Add column that should be extacted",
    )
    args = parser.parse_args()

    select_read_ids = None
    if args.select_ids:
        select_read_ids = pd.read_csv(args.select_ids)["read_id"]

    if select_read_ids is not None:
        print(f"Selecting {len(select_read_ids)} specific read ids")
    if args.get_column is not None:
        print(f"Selecting column: {args.get_column}")

    mode = None

    print(f"Search for input files in {args.input}")
    files = list(args.input.glob("*.pod5"))
    print(f"Searching in {[str(f) for f in files]}")

    # Run benchmark using most appropriate method:
    if args.get_column is None:
        df = run_get_read_ids(files)
    elif args.get_column == "samples":
        # Because we the "samples" column to be the sum
        # of all samples in input data, it is quicker to use
        # python multiprocessing to split the summing work:
        df = run_multiprocess(
            files,
            args.output,
            select_read_ids=select_read_ids,
            column=args.get_column,
            mode=mode,
        )
    elif args.select_ids:
        df = run_select(
            files,
            select_read_ids=select_read_ids,
            column=args.get_column,
        )
    else:
        df = run_batched(
            files,
            column=args.get_column,
        )

    print(f"Selected {len(df)} items")
    args.output.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output / "read_ids.csv", index=False)


if __name__ == "__main__":
    main()
