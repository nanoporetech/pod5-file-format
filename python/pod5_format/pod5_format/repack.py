"""
Tools to assist repacking pod5 data into other pod5 files
"""
import time

import pod5_format as p5
import pod5_format.pod5_format_pybind as p5b

# The default interval in seconds to check for completion
DEFAULT_INTERVAL = 15


class Repacker:
    """Wrapper class around native pod5 tools to repack data"""

    def __init__(self):
        self._repacker = p5b.Repacker()

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

    def add_output(self, output_file: p5.Writer):
        """
        Add an output file to the repacker, so it can have read data repacked into it.

        Once a user has added an output, it can be passed as an output
        to add_selected_reads_to_output or add_reads_to_output
        """
        return self._repacker.add_output(output_file._writer)

    def add_selected_reads_to_output(
        self, output_ref, reader: p5.Reader, selected_read_ids
    ):
        """
        Add specific read ids in a source file to an output file.

        Passing an already wrapped output, a file, and a list of read ids will pack the passed read ids into the destination file.
        """
        successful_finds, per_batch_counts, all_batch_rows = reader._plan_traversal(
            selected_read_ids
        )

        if successful_finds != len(selected_read_ids):
            raise RuntimeError(
                f"Failed to find {len(selected_read_ids) - successful_finds} "
                "requested reads in the source file"
            )

        self._repacker.add_selected_reads_to_output(
            output_ref, reader._handles._file_reader, per_batch_counts, all_batch_rows
        )

    def add_all_reads_to_output(self, output_ref, reader: p5.Reader):
        """
        Add all read ids in a source file to an output file.
        """
        self._repacker.add_all_reads_to_output(output_ref, reader._handles._file_reader)

    def wait(
        self,
        interval: float = DEFAULT_INTERVAL,
        status_updates: bool = True,
        finish: bool = True,
    ) -> None:
        """
        Wait for the repacker (blocking) until it is done by checking is_complete every
        interval seconds. Optionally report status_updates to stdout and call
        finish when done.
        """
        if interval <= 0:
            print(f"Invalid interval {interval}sec. Using {DEFAULT_INTERVAL}sec")
            interval = DEFAULT_INTERVAL

        last_time = time.time()
        last_bytes_complete = 0

        while not self.is_complete:
            time.sleep(interval)

            if not status_updates:
                continue

            # Compute the bytes completed since last check
            bytes_completed = self.reads_sample_bytes_completed
            bytes_delta = bytes_completed - last_bytes_complete
            last_bytes_complete = bytes_completed

            # Update the time stamp
            time_now = time.time()
            time_delta = time_now - last_time
            last_time = time_now

            # Compute write rate and completion percentage
            mb_per_sec = (bytes_delta / (1000 * 1000)) / time_delta
            pct_complete = 100 * (self.batches_completed / self.batches_requested)

            print(
                f"{pct_complete:.1f} % complete, " f"{mb_per_sec:.1f} MB/s. ",
                f"Batches complete: {self.batches_completed}, "
                f"requested: {self.batches_requested}, "
                f"pending: {self.pending_batch_writes}, ",
            )

        if finish:
            self.finish()

    def finish(self):
        return self._repacker.finish()
