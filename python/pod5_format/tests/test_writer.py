"""
Testing Pod5Writer
"""
import pytest
import pod5_format as p5
import pod5_format.pod5_format_pybind as p5b


class TestPod5ReaderCombined:
    """Test the Pod5Reader from a combined pod5 file"""

    def test_combined_writer_fixture(self, combined_writer: p5.Writer) -> None:
        """Basic assertions on the combined_reader fixture"""
        assert isinstance(combined_writer, p5.Writer)
        assert combined_writer._writer is not None
        assert isinstance(combined_writer._writer, p5b.FileWriter)

    @pytest.mark.parametrize("random_read", [1, 2, 3, 4], indirect=True)
    def test_combined_writer_random_reads(
        self, combined_writer: p5.Writer, random_read: p5.Read
    ) -> None:
        """Write some random single reads to a combined writer"""

        combined_writer.add_read_object(random_read, pre_compressed_signal=False)

    @pytest.mark.parametrize("random_read_pre_compressed", [1], indirect=True)
    def test_combined_writer_random_reads_compressed(
        self, combined_writer: p5.Writer, random_read_pre_compressed: p5.Read
    ) -> None:
        """Write some random single reads to a combined writer which are pre-compressed"""
        combined_writer.add_read_object(
            random_read_pre_compressed, pre_compressed_signal=True
        )
