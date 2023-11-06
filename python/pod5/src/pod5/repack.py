"""
Tools to assist repacking pod5 data into other pod5 files
"""
from typing import Collection
import lib_pod5 as p5b

import pod5 as p5


class Repacker:
    """Wrapper class around native pod5 tools to repack data"""

    def __init__(self):
        self._repacker = p5b.Repacker()
        self._reads_requested = 0

    @property
    def is_complete(self) -> bool:
        """Find if the requested repack operations are complete"""
        return self._repacker.is_complete

    @property
    def currently_open_file_reader_count(self) -> int:
        """Returns the number of open file readers held by this repacker"""
        return self._repacker.currently_open_file_reader_count

    @property
    def reads_completed(self) -> int:
        """Find the number of reads written to files"""
        return self._repacker.reads_completed

    @property
    def reads_requested(self) -> int:
        """Find the number of requested reads to be written"""
        return self._reads_requested

    def add_output(
        self, output_file: p5.Writer, check_duplicate_read_ids: bool = True
    ) -> p5b.Pod5RepackerOutput:
        """
        Add an output file writer to the repacker, so it can have read data repacked
        into it.

        Once a user has added an output, it can be passed as an output
        to :py:meth:`add_selected_reads_to_output` or :py:meth:`add_reads_to_output`

        Parameters
        ----------
        output_file: :py:class:`writer.Writer`
            The output file writer to use
        check_duplicate_read_ids: bool
            Check the output for duplicate read ids, and raise an error if found.

        Returns
        -------
        repacker_object: p5b.Pod5RepackerOutput
            Use this as "output_ref" in calls to :py:meth:`add_selected_reads_to_output`
            or :py:meth:`add_reads_to_output`
        """
        assert output_file._writer is not None
        return self._repacker.add_output(output_file._writer, check_duplicate_read_ids)

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

    def finish(self) -> None:
        """
        Call finish on the underlying c_api repacker instance to write the footer
        completing the file and freeing resources
        """
        return self._repacker.finish()

    def set_output_finished(self, output) -> None:
        """
        Tell the repacker a specific output is complete and can be finalised.
        """
        return self._repacker.set_output_finished(output)
