"""
Tools to assist repacking pod5 data into other pod5 files
"""
from . import reader_pyarrow
from . import writer

import pod5_format.pod5_format_pybind


class Repacker:
    """Wrapper class around native pod5 tools to repack data"""

    def __init__(self):
        self._repacker = pod5_format.pod5_format_pybind.Repacker()

    @property
    def is_complete(self):
        """Find if the requested repack operations are complete"""
        return self._repacker.is_complete

    @property
    def reads_sample_bytes_completed(self):
        """Find the number of bytes for sample data repacked"""
        return self._repacker.reads_sample_bytes_completed

    @property
    def batches_requested(self):
        """Find the number of batches requested to be read from source files"""
        return self._repacker.batches_requested

    @property
    def batches_completed(self):
        """Find the number of batches completed writing to dest files"""
        return self._repacker.batches_completed

    @property
    def reads_completed(self):
        """Find the number of reads written to files"""
        return self._repacker.reads_completed

    @property
    def pending_batch_writes(self):
        """Find the number of batches in flight, awaiting writing"""
        return self._repacker.pending_batch_writes

    def add_output(self, output_file: writer.FileWriter):
        """
        Add an output file to the repacker, so it can have read data repacked into it.

        Once a user has added an output, it can be pased as an output to add_selected_reads_to_output or add_reads_to_output
        """
        return self._repacker.add_output(output_file._writer)

    def add_selected_reads_to_output(
        self, output_ref, reader: reader_pyarrow.FileReader, selected_read_ids
    ):
        """
        Add specific read ids in a source file to an output file.

        Passing an already wrapped output, a file, and a list of read ids will pack the passed read ids into the destination file.
        """
        successful_finds, per_batch_counts, all_batch_rows = reader._plan_traversal(
            selected_read_ids
        )

        if successful_finds != len(selected_read_ids):
            raise Exception(
                f"Failed to find {len(selected_read_ids) - successful_finds} requested reads in the source file"
            )

        self._repacker.add_selected_reads_to_output(
            output_ref, reader._reader, per_batch_counts, all_batch_rows
        )

    def add_reads_to_output(self, output_ref, reader: reader_pyarrow.FileReader):
        """
        Add all read ids in a source file to an output file.
        """
        self._repacker.add_reads_to_output(output_ref, reader._reader)

    def finish(self):
        return self._repacker.finish()
