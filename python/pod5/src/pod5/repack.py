"""
Tools to assist repacking pod5 data into other pod5 files
"""
import time
from typing import Collection, Generator, Optional
from venv import logger

import lib_pod5 as p5b

import pod5 as p5
from pod5.tools.utils import PBAR_DEFAULTS, logged_all
from tqdm.auto import tqdm

# The default interval in seconds to check for completion
DEFAULT_INTERVAL = 0.5


class Repacker:
    """Wrapper class around native pod5 tools to repack data"""

    def __init__(self):
        self._repacker = p5b.Repacker()
        self._reads_requested = 0

    @property
    def is_complete(self) -> bool:
        """Find if the requested repack operations are complete"""
        # is_complete can be initialised to true before work starts
        # this gives a short time to allow the value to be set
        if self._repacker.is_complete:
            for _ in range(100):
                time.sleep(0.02)
                if not self._repacker.is_complete:
                    break

        return self._repacker.is_complete

    @property
    def reads_sample_bytes_completed(self) -> int:
        """Find the number of bytes for sample data repacked"""
        return self._repacker.reads_sample_bytes_completed

    @property
    def batches_requested(self) -> int:
        """Find the number of batches requested to be read from source files"""
        return self._repacker.batches_requested

    @property
    def batches_completed(self) -> int:
        """Find the number of batches completed writing to dest files"""
        return self._repacker.batches_completed

    @property
    def reads_completed(self) -> int:
        """Find the number of reads written to files"""
        return self._repacker.reads_completed

    @property
    def reads_requested(self) -> int:
        """Find the number of requested reads to be written"""
        return self._reads_requested

    @property
    def pending_batch_writes(self) -> int:
        """Find the number of batches in flight, awaiting writing"""
        return self._repacker.pending_batch_writes

    def add_output(self, output_file: p5.Writer) -> p5b.Pod5RepackerOutput:
        """
        Add an output file writer to the repacker, so it can have read data repacked
        into it.

        Once a user has added an output, it can be passed as an output
        to :py:meth:`add_selected_reads_to_output` or :py:meth:`add_reads_to_output`

        Parameters
        ----------
        output_file: :py:class:`writer.Writer`
            The output file writer to use

        Returns
        -------
        repacker_object: p5b.Pod5RepackerOutput
            Use this as "output_ref" in calls to :py:meth:`add_selected_reads_to_output`
            or :py:meth:`add_reads_to_output`
        """
        assert output_file._writer is not None
        return self._repacker.add_output(output_file._writer)

    def add_selected_reads_to_output(
        self,
        output_ref: p5b.Pod5RepackerOutput,
        reader: p5.Reader,
        selected_read_ids: Collection[str],
    ):
        """
        Copy the selected read_ids from the given :py:class:`Reader` into the
        Repacker output reference which was returned by :py:meth:`add_output`

        Parameters
        ----------
        output_ref : lib_pod5.pod5_format_pybind.Pod5RepackerOutput
            The repacker handle reference returned from :py:meth:`add_output`
        reader : :py:class:`Reader`
            The Pod5 file reader to copy reads from
        selected_read_ids: Collection[str]
            A Collection of read_ids as strings

        Raises
        ------
        RuntimeError
            If any of the selected_read_ids were not found in the source file
        """

        successful_finds, per_batch_counts, all_batch_rows = reader._plan_traversal(
            selected_read_ids
        )

        if successful_finds != len(selected_read_ids):
            raise RuntimeError(
                f"Failed to find {len(selected_read_ids) - successful_finds} "
                "requested reads in the source file"
            )

        self._reads_requested += successful_finds
        self._repacker.add_selected_reads_to_output(
            output_ref, reader.inner_file_reader, per_batch_counts, all_batch_rows
        )

    def add_all_reads_to_output(
        self, output_ref: p5b.Pod5RepackerOutput, reader: p5.Reader
    ) -> None:
        """
        Copy the every read from the given :py:class:`Reader` into the
        Repacker output reference which was returned by :py:meth:`add_output`

        Parameters
        ----------
        output_ref : lib_pod5.pod5_format_pybind.Pod5RepackerOutput
            The repacker handle reference returned from :py:meth:`add_output`
        reader : :py:class:`Reader`
            The Pod5 file reader to copy reads from
        """
        self._reads_requested += reader.num_reads
        self._repacker.add_all_reads_to_output(output_ref, reader.inner_file_reader)

    @logged_all
    def wait(
        self,
        finish: bool = True,
        interval: float = DEFAULT_INTERVAL,
        desc: str = "",
        total_reads: Optional[int] = None,
        offset: int = 0,
    ) -> int:
        """
        Wait for the repacker (blocking) until it is done checking every `interval`
        seconds. Shows a progress bar at the current process index with desc string
        as the description.

        Parameters
        ----------
        finish : bool
            Flag to toggle an optional final call to :py:meth:`finish` to
            close the repacker and free resources
        interval : float
            The interval (in seconds) between checks to :py:meth:`is_complete`
        desc : str
            Progressbar description string
        total_reads : int
            Overwrites the total number of reads expected
        offset : int
            Sets the progress bar position offset

        Returns
        -------
        num_reads_completed: int
            The number of reads written
        """
        logger.info(f"pos: {offset}")

        if total_reads is not None:
            total = total_reads
        else:
            total = self.reads_requested

        pbar = tqdm(
            total=total,
            desc=desc,
            leave=False,
            unit="Read",
            position=offset,
            **PBAR_DEFAULTS,
        )

        last_reads = 0
        while not self.is_complete:
            time.sleep(interval)

            # Update pbar - total / reads_requested might change if user adds more
            if total_reads is None:
                pbar.total = self.reads_requested
            pbar.update(self.reads_completed - last_reads)
            last_reads = self.reads_completed

        if finish:
            self.finish()

        return last_reads

    def waiter(
        self,
        interval: float = DEFAULT_INTERVAL,
    ) -> Generator[int, None, None]:
        """
        Wait for the repacker (blocking) until it is done checking every `interval`
        seconds. Yields number of reads completed .

        Parameters
        ----------
        interval : float
            The interval (in seconds) between checks to :py:meth:`is_complete`

        Returns
        -------
        num_reads_completed: int
            The number of reads written
        """
        # Sleep to ensure `is_complete` state correctly set
        while not self.is_complete:
            yield self.reads_completed
            time.sleep(interval)

    def finish(self) -> None:
        """
        Call finish on the underlying c_api repacker instance to free resources
        """
        return self._repacker.finish()
