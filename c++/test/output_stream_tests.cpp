#include "pod5_format/internal/async_output_stream.h"
#include "pod5_format/internal/linux_output_stream.h"
#include "test_utils.h"

#include <arrow/io/file.h>
#include <catch2/catch.hpp>

#include <fstream>

namespace {
static constexpr std::size_t TestDataSize = 1024 * 1024 * 100;

std::shared_ptr<arrow::Buffer> get_test_data()
{
    static std::shared_ptr<arrow::Buffer> const data = [] {
        auto result = *arrow::AllocateResizableBuffer(TestDataSize);
        for (std::size_t i = 0; i < TestDataSize; ++i) {
            result->mutable_data()[i] = i % 256;
        }
        return result;
    }();

    return data;
}

std::vector<char> read_file(char const * filename)
{
    std::ifstream fin(filename, std::ios::binary);

    return std::vector<char>(std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>());
}

void check_file_contents(char const * filename)
{
    auto contents = read_file(filename);
    auto expected_contents = get_test_data();
    auto data_ptr = reinterpret_cast<char const *>(expected_contents->data());
    std::vector<char> expected_contents_vec(data_ptr, data_ptr + expected_contents->size());

    auto fail_and_dump_context = [&](std::size_t i) {
        std::stringstream str;
        str << "Difference at index " << i << "\n";
        std::size_t min_index = 0, max_index = std::min(i + 16, expected_contents_vec.size() - 1);
        if (i > 16) {
            min_index = i - 16;
        }

        str << "Expected ";
        for (std::size_t i = min_index; i <= max_index; ++i) {
            str << std::hex << expected_contents_vec[i] << " ";
        }

        str << "Actual   ";
        for (std::size_t i = min_index; i <= max_index; ++i) {
            str << std::hex << contents[i] << " ";
        }
        FAIL(str.str().c_str());
    };

    for (std::size_t i = 0; i < expected_contents_vec.size(); i += 1) {
        if (contents[i] != expected_contents_vec[i]) {
            fail_and_dump_context(i);
        }
    }

    CHECK(contents.size() == expected_contents_vec.size());
}
}  // namespace

void run_output_stream_test(std::shared_ptr<arrow::io::OutputStream> output_stream)
{
    auto const data = get_test_data();

    std::size_t small_writes_bytes_consumed = 0;
    {
        CHECK_ARROW_STATUS_OK(output_stream->Write(data->data() + small_writes_bytes_consumed, 1));
        small_writes_bytes_consumed += 1;
        CHECK_ARROW_STATUS_OK(output_stream->Write(data->data() + small_writes_bytes_consumed, 2));
        small_writes_bytes_consumed += 2;
        CHECK_ARROW_STATUS_OK(output_stream->Write(data->data() + small_writes_bytes_consumed, 4));
        small_writes_bytes_consumed += 4;
        CHECK_ARROW_STATUS_OK(output_stream->Write(data->data() + small_writes_bytes_consumed, 8));
        small_writes_bytes_consumed += 8;

        CHECK_ARROW_STATUS_OK(output_stream->Flush());
    }

    auto remaining_data_buffer = arrow::SliceBuffer(data, small_writes_bytes_consumed);

    {
        auto chunk_1 = arrow::SliceBuffer(remaining_data_buffer, 0, 1024);
        auto chunk_2 = arrow::SliceBuffer(remaining_data_buffer, 1024, 63);
        remaining_data_buffer = arrow::SliceBuffer(remaining_data_buffer, 1024 + 63);
        CHECK_ARROW_STATUS_OK(output_stream->Write(chunk_1));
        CHECK_ARROW_STATUS_OK(output_stream->Write(chunk_2));
        CHECK_ARROW_STATUS_OK(output_stream->Flush());
    }

    {
        auto chunk_1 = arrow::SliceBuffer(remaining_data_buffer, 0, 1024 * 1024);
        auto chunk_2 = arrow::SliceBuffer(remaining_data_buffer, 1024 * 1024, 1023);
        remaining_data_buffer = arrow::SliceBuffer(remaining_data_buffer, 1024 * 1024 + 1023);
        CHECK_ARROW_STATUS_OK(output_stream->Write(chunk_1));
        CHECK_ARROW_STATUS_OK(output_stream->Write(chunk_2));
        CHECK_ARROW_STATUS_OK(output_stream->Flush());
    }

    CHECK_ARROW_STATUS_OK(output_stream->Write(remaining_data_buffer));
    CHECK_ARROW_STATUS_OK(output_stream->Flush());
}

TEST_CASE("AsyncOutputStream", "[OutputStream]")
{
    using namespace pod5;

    auto const filename = "./test_file.bin";
    {
        std::ofstream f(filename, std::ios_base::trunc);
    }
    {
        auto res = arrow::io::FileOutputStream::Open(filename);
        REQUIRE_ARROW_STATUS_OK(res);
        auto thread_pool = make_thread_pool(1);
        auto stream = *AsyncOutputStream::make(*res, thread_pool, arrow::default_memory_pool());

        run_output_stream_test(stream);
    }
    check_file_contents(filename);
}

#ifdef __linux__
TEST_CASE("LinuxOutputStream IOManagerSyncImpl", "[OutputStream]")
{
    using namespace pod5;

    auto filename = "./test_file.bin";
    {
        std::ofstream f(filename, std::ios_base::trunc);
    }
    {
        auto io_manager = pod5::make_sync_io_manager();
        REQUIRE_ARROW_STATUS_OK(io_manager);
        auto stream =
            *LinuxOutputStream::make(filename, *io_manager, 10 * 1024 * 1024, true, false, true);

        run_output_stream_test(stream);
    }
    check_file_contents(filename);
}

#endif
