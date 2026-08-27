from pathlib import Path
import shutil

import pytest

import pod5 as p5
from pod5.tools.pod5_merge import merge_pod5

from collections import Counter

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"


class TestMerge:
    """Test merge application"""

    def test_merge_runs(self, tmp_path: Path):
        """Test that the merge tool runs a trivial example"""

        # Test all pod5 inputs in test data, which will likely contain duplicates
        inputs = list(TEST_DATA_PATH.glob("split_*pod5"))
        output = tmp_path / "test.pod5"
        merge_pod5(
            inputs[:2],
            output,
            force_overwrite=False,
            recursive=False,
        )

        assert output.exists()

        with p5.Reader(output) as reader:
            reads = list(reader.reads())
            assert reads

    def test_merge_duplicate_stopped(self, tmp_path: Path):
        """Test that the merge tool prevents duplicate reads being merged"""

        # Test all pod5 inputs in test data, which will likely contain duplicates
        inputs = list(TEST_DATA_PATH.glob("*pod5"))
        output = tmp_path / "test.pod5"

        with pytest.raises(RuntimeError, match="Duplicate read id"):
            merge_pod5(inputs, output)

    def test_merge_mismatch_signal_data(self, tmp_path: Path):
        """Test that the merge tool raises an error when merging identical read ids with different signal data"""

        input = TEST_DATA_PATH / "multi_fast5_zip_v5.pod5"

        # Create a new pod5 file with the same read ids but different signal data
        with p5.Writer(tmp_path / "mismatch.pod5") as writer:
            with p5.Reader(input) as reader:
                for index, read_record in enumerate(reader.reads()):
                    read_to_write = read_record.to_read()
                    # Modify the signal data to be different for the first read
                    if index == 0:
                        read_to_write.signal[0] += 1
                    writer.add_read(read_to_write)

        output = tmp_path / "test.pod5"

        with pytest.raises(RuntimeError, match="Duplicate read id"):
            merge_pod5(
                [tmp_path / "mismatch.pod5", input], output, merge_duplicate_reads=True
            )

    def test_merge_duplicate_reads(self, tmp_path: Path):
        """Test that duplicate reads are merged and the output has unique read ids"""

        input = TEST_DATA_PATH / "multi_fast5_zip_v5.pod5"

        # Create a new pod5 file with the same read ids by copying the input file
        shutil.copy(input, tmp_path / "copy.pod5")

        output = tmp_path / "test.pod5"

        merge_pod5([tmp_path / "copy.pod5", input], output, merge_duplicate_reads=True)

        assert output.exists()

        with p5.Reader(output) as reader:
            read_ids = reader.read_ids
            assert len(read_ids) == len(set(read_ids))

    def test_merge_overlap(self, tmp_path: Path):
        """Test that merges two files with overlap"""

        input_file = TEST_DATA_PATH / "multi_fast5_zip_v5.pod5"

        # Create three distinct pod5 files by splitting one pod5
        file_A = tmp_path / "file_A.pod5"
        file_A_read_ids = []
        file_B = tmp_path / "file_B.pod5"
        file_B_read_ids = []
        file_C = tmp_path / "file_C.pod5"
        file_C_read_ids = []
        with p5.Writer(file_A) as writer_A:
            with p5.Writer(file_B) as writer_B:
                with p5.Writer(file_C) as writer_C:
                    with p5.Reader(input_file) as reader:
                        for index, read_record in enumerate(reader.reads()):
                            read_to_write = read_record.to_read()
                            if index % 3 == 0:
                                file_A_read_ids.append(str(read_to_write.read_id))
                                writer_A.add_read(read_to_write)
                            elif index % 3 == 1:
                                file_B_read_ids.append(str(read_to_write.read_id))
                                writer_B.add_read(read_to_write)
                            else:
                                file_C_read_ids.append(str(read_to_write.read_id))
                                writer_C.add_read(read_to_write)

        def get_read_ids(file_path: Path):
            with p5.Reader(file_path) as reader:
                # No duplicate read ids check
                assert len(reader.read_ids) == len(set(reader.read_ids))
                return reader.read_ids

        file_AB = tmp_path / "file_AB.pod5"
        file_AC = tmp_path / "file_AC.pod5"
        file_ABC = tmp_path / "file_ABC.pod5"

        # Create two merged files with overlap,
        merge_pod5([file_A, file_B], file_AB, merge_duplicate_reads=True)
        merge_pod5([file_A, file_C], file_AC, merge_duplicate_reads=True)

        assert file_AB.exists()
        assert file_AC.exists()
        read_ids_AB = get_read_ids(file_AB)
        read_ids_AC = get_read_ids(file_AC)

        # Merge into a final file
        merge_pod5([file_AB, file_AC], file_ABC, merge_duplicate_reads=True)

        read_ids_ABC = get_read_ids(file_ABC)
        read_ids_A = get_read_ids(file_A)
        read_ids_B = get_read_ids(file_B)
        read_ids_C = get_read_ids(file_C)

        assert Counter(read_ids_ABC) == Counter(
            file_A_read_ids + file_B_read_ids + file_C_read_ids
        )
        assert Counter(read_ids_AB) == Counter(file_A_read_ids + file_B_read_ids)
        assert Counter(read_ids_AC) == Counter(file_A_read_ids + file_C_read_ids)
        assert Counter(read_ids_A) == Counter(file_A_read_ids)
        assert Counter(read_ids_B) == Counter(file_B_read_ids)
        assert Counter(read_ids_C) == Counter(file_C_read_ids)
