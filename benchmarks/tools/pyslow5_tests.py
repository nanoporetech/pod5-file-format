#!/usr/bin/env python3

import argparse
import multiprocessing as mp
from pathlib import Path
from queue import Empty

import numpy
import pandas as pd
import pyslow5


def random_access(s5_file, read_list, col, result_q):
    file = pyslow5.Open(str(s5_file), "r")
    print("processing ", s5_file)
    read_ids = []
    extracted_columns = {"read_id": read_ids}
    extracted_columns[col] = []
    vals = extracted_columns[col]
    if col == "samples":
        for read in file.get_read_list_multi(read_list, threads=10, batchsize=5000):
            read_ids.append(read["read_id"])
            vals.append(numpy.sum(read["signal"]))
    elif col == "sample_count":
        for read in file.get_read_list_multi(read_list, threads=10, batchsize=5000):
            read_ids.append(read["read_id"])
            vals.append(read["len_raw_signal"])
    else:
        for read in file.get_read_list_multi(
            read_list, threads=10, batchsize=5000, pA=False, aux=col
        ):
            read_ids.append(read["read_id"])
            vals.append(read[col])
    result_q.put(pd.DataFrame(extracted_columns))


def run(s5_file, benchmark, select_read_ids, col):
    if benchmark == "get_all_read_ids":
        read_ids = []
        extracted_columns = {"read_id": read_ids}
        file = pyslow5.Open(str(s5_file), "r")
        print("processing ", s5_file)
        read_ids, num_reads = file.get_read_ids()
        extracted_columns = {"read_id": read_ids}

    elif benchmark == "sample_values":
        mp.set_start_method("spawn")
        result_queue = mp.Queue()
        runners = 10
        processes = []
        approx_chunk_size = max(1, len(select_read_ids) // runners)
        select_ids = []
        for i in range(0, len(select_read_ids), approx_chunk_size):
            for j in range(i, min(len(select_read_ids), i + approx_chunk_size)):
                select_ids.append(select_read_ids[j])
            p = mp.Process(
                target=random_access, args=(s5_file, select_ids, col, result_queue)
            )
            p.start()
            processes.append(p)
            select_ids = []

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
        return df

    elif benchmark == "all_values":
        read_ids = []
        extracted_columns = {"read_id": read_ids}
        file = pyslow5.Open(str(s5_file), "r")
        print("processing ", s5_file)
        extracted_columns[col] = []
        vals = extracted_columns[col]
        if col == "samples":
            for read in file.seq_reads_multi(threads=10, batchsize=5000):
                read_ids.append(read["read_id"])
                vals.append(numpy.sum(read["signal"]))
        elif col == "sample_count":
            for read in file.seq_reads_multi(threads=10, batchsize=5000):
                read_ids.append(read["read_id"])
                vals.append(read["len_raw_signal"])
        else:
            for read in file.seq_reads_multi(
                threads=10, batchsize=5000, pA=False, aux=col
            ):
                read_ids.append(read["read_id"])
                vals.append(read[col])

    return pd.DataFrame(extracted_columns)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "benchmark",
        type=str,
        choices=["get_all_read_ids", "sample_values", "all_values"],
        help="which benchmark to run",
    )
    parser.add_argument(
        "--select-ids",
        type=str,
        help="CSV file with a read_id column, listing ids to find in input files",
    )
    parser.add_argument(
        "--get-column",
        default=None,
        type=str,
        help="Add columns that should be extacted",
    )

    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    select_read_ids = None
    select_reads = []
    if args.select_ids:
        select_read_ids = pd.read_csv(args.select_ids)["read_id"]
        for i in select_read_ids:
            select_reads.append(i)

    print(f"Num of select_reads: {len(select_reads)}")

    df = run(
        args.input,
        args.benchmark,
        select_read_ids=select_reads,
        col=args.get_column,
    )
    print(f"Selected {len(df)} items")
    df.to_csv(args.output / "read_ids.csv", index=False)


if __name__ == "__main__":
    main()
