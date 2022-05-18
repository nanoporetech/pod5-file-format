#!/usr/bin/python3

import argparse
from collections import namedtuple
import multiprocessing as mp
from pathlib import Path
from queue import Empty
from uuid import UUID
import tempfile

import numpy
import pandas as pd

import pod5_format

SelectReadIdsData = namedtuple(
    "SelectReadIdsData", ["path", "slice_start", "slice_end", "shape"]
)


def process_batch(get_columns, batch, extracted_columns):
    new_read_ids = batch.read_id_column.to_pandas().map(lambda x: UUID(bytes=x))
    extracted_columns["read_id"] = pd.concat(
        [extracted_columns.get("read_id", None), new_read_ids]
    )

    for c in get_columns:
        if c == "samples":
            row = []
            for read in batch.reads():
                row.append(numpy.sum(read.signal))
            extracted_columns[c] = pd.concat(
                [extracted_columns.get(c, None), pd.Series(row)]
            )
        elif c == "read_number":
            extracted_columns[c] = pd.concat(
                [extracted_columns.get(c, None), batch.read_number_column.to_pandas()]
            )
        else:
            assert False


def do_batch_work(filename, batches, get_columns, result_q):
    extracted_columns = {}

    file = pod5_format.open_combined_file(filename)
    for batch_id in batches:
        process_batch(get_columns, file.get_batch(batch_id), extracted_columns)

    result_q.put(pd.DataFrame(extracted_columns))


def do_search_work(files, select_read_ids_data, get_columns, result_q):
    select_read_ids_all = numpy.memmap(
        select_read_ids_data.path,
        dtype=numpy.uint8,
        mode="r+",
        shape=select_read_ids_data.shape,
    )
    select_read_ids = select_read_ids_all[
        select_read_ids_data.slice_start : select_read_ids_data.slice_end
    ]
    extracted_columns = {}
    for file in files:
        file = pod5_format.open_combined_file(file)

        for batch in file.select_reads_in_batches(select_read_ids):
            process_batch(get_columns, batch, extracted_columns)

    result_q.put(pd.DataFrame(extracted_columns))


def run(input_dir, output, select_read_ids=None, get_columns=[]):
    output.mkdir(parents=True, exist_ok=True)

    mp.set_start_method("spawn")

    if select_read_ids is not None:
        print(f"Selecting {len(select_read_ids)} specific read ids")
    if get_columns is not None:
        print(f"Selecting columns: {get_columns}")

    if select_read_ids is not None:
        print("Placing select read id data on disk for mmapping:")
        numpy_select_read_ids = numpy.array(
            [
                numpy.frombuffer(UUID(x).bytes, dtype=numpy.uint8)
                for x in select_read_ids
            ],
            dtype=numpy.uint8,
        )

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

    print(f"Search for input files in {input_dir}")
    files = list(input_dir.glob("*.pod5"))
    print(f"Searching for read ids in {[str(f) for f in files]}")

    file = pod5_format.open_combined_file(files[0])

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
                args=(files, select_read_ids_data, get_columns, result_queue),
            )
            p.start()
            processes.append(p)
            start_index += approx_chunk_size
    else:
        for filename in files:
            file = pod5_format.open_combined_file(filename)
            batches = list(range(file.batch_count))
            approx_chunk_size = max(1, len(batches) // runners)
            start_index = 0
            while start_index < len(batches):
                select_batches = batches[start_index : start_index + approx_chunk_size]
                p = mp.Process(
                    target=do_batch_work,
                    args=(filename, select_batches, get_columns, result_queue),
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

    if select_read_ids is not None:
        select_read_ids_mmap_path.unlink()


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
    )


if __name__ == "__main__":
    main()
