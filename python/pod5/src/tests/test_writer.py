"""
Testing Pod5Writer
"""
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

        for record in reader:
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

        for edited_record in p5.Reader(writer.path):
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
