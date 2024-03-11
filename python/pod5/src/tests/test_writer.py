"""
Testing Pod5Writer
"""
import math
import lib_pod5 as p5b
import numpy as np
import pytest

import pod5 as p5


class TestPod5Writer:
    """Test the Pod5Writer from a pod5 file"""

    def test_writer_fixture(self, writer: p5.Writer) -> None:
        """Basic assertions on the writer fixture"""
        assert isinstance(writer, p5.Writer)
        assert isinstance(writer._writer, p5b.FileWriter)

    @pytest.mark.parametrize("random_read", [1, 2, 3, 4], indirect=True)
    def test_writer_random_reads(self, writer: p5.Writer, random_read: p5.Read) -> None:
        """Write some random single reads to a writer"""

        writer.add_read(random_read)

    @pytest.mark.parametrize("random_read_pre_compressed", [1], indirect=True)
    def test_writer_random_reads_compressed(
        self, writer: p5.Writer, random_read_pre_compressed: p5.Read
    ) -> None:
        """Write some random single reads to a writer which are pre-compressed"""
        writer.add_read(random_read_pre_compressed)

    def test_read_edit_write(self, reader: p5.Reader, writer: p5.Writer) -> None:
        """Read some records, edit the reads and write an edited read"""

        records = 0
        for record in reader:
            records += 1
            read = record.to_read()

            # Edit some attributes
            read.calibration = p5.Calibration(0, 1)
            read.end_reason = p5.EndReason.from_reason_with_default_forced(
                p5.EndReasonEnum.DATA_SERVICE_UNBLOCK_MUX_CHANGE
            )
            # Edit the signal
            read.signal = np.arange(0, 100, dtype=np.int16)

            # Write the edited read
            writer.add_read(read)

        writer.close()

        edited = 0
        for edited_record in p5.Reader(writer.path):
            edited += 1
            assert edited_record.calibration.offset == 0
            assert edited_record.calibration.scale == 1
            assert (
                edited_record.end_reason
                == p5.EndReason.from_reason_with_default_forced(
                    p5.EndReasonEnum.DATA_SERVICE_UNBLOCK_MUX_CHANGE
                )
            )
            assert len(edited_record.signal) == 100
            assert min(edited_record.signal) == 0
            assert max(edited_record.signal) == 99

        assert edited == records

    def test_read_copy(self, reader: p5.Reader, writer: p5.Writer) -> None:
        """Read some records, edit the reads and write an edited read"""

        records = {}
        for record in reader:
            records[record.read_id] = record
            read = record.to_read()
            writer.add_read(read)
        writer.close()

        edited = {}
        for edited_record in p5.Reader(writer.path):
            edited[edited_record.read_id] = edited_record

        assert len(records) == len(edited)
        for read_id in records.keys():
            before = records[read_id]
            after = edited[read_id]

            assert before.read_id == after.read_id
            assert before.read_number == after.read_number
            assert before.start_sample == after.start_sample
            assert before.num_samples == after.num_samples
            assert (
                all(math.isnan(x) for x in (before.median_before, after.median_before))
                or before.median_before == after.median_before
            )
            assert before.num_minknow_events == after.num_minknow_events
            assert before.tracked_scaling == after.tracked_scaling
            assert before.predicted_scaling == after.predicted_scaling
            assert before.num_reads_since_mux_change == after.num_reads_since_mux_change
            assert before.time_since_mux_change == after.time_since_mux_change
            assert before.pore == after.pore
            assert before.calibration == after.calibration
            assert before.calibration_digitisation == after.calibration_digitisation
            assert before.calibration_range == after.calibration_range
            assert before.end_reason == after.end_reason
            assert before.run_info == after.run_info
            assert before.end_reason_index == after.end_reason_index
            assert before.run_info_index == after.run_info_index
            assert before.sample_count == after.sample_count
            # assert before.byte_count == after.byte_count
            assert before.has_cached_signal == after.has_cached_signal
            assert np.array_equal(before.signal, after.signal)
            assert np.array_equal(before.signal_pa, after.signal_pa)
