"""
Testing Pod5Writer
"""
import pytest
import pod5_format as p5
import pod5_format.pod5_format_pybind as p5b


class TestPod5Reader:
    """Test the Pod5Reader from a pod5 file"""

    def test_writer_fixture(self, writer: p5.Writer) -> None:
        """Basic assertions on the reader fixture"""
        assert isinstance(writer, p5.Writer)
        assert isinstance(writer._writer, p5b.FileWriter)

    @pytest.mark.parametrize("random_read", [1, 2, 3, 4], indirect=True)
    def test_writer_random_reads(self, writer: p5.Writer, random_read: p5.Read) -> None:
        """Write some random single reads to a writer"""

        writer.add_read_object(random_read)

    @pytest.mark.parametrize("random_read_pre_compressed", [1], indirect=True)
    def test_writer_random_reads_compressed(
        self, writer: p5.Writer, random_read_pre_compressed: p5.Read
    ) -> None:
        """Write some random single reads to a writer which are pre-compressed"""
        writer.add_read_object(random_read_pre_compressed)
