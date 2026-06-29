#include "pod5_format/internal/async_output_stream.h"
#include "pod5_format/pdz_compression.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/signal_compression.h"
#include "pod5_format/signal_table_reader.h"
#include "pod5_format/signal_table_writer.h"
#include "pod5_format/types.h"
#include "pod5_format/uuid.h"
#include "pod5_format/version.h"
#include "test_utils.h"
#include "utils.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <catch2/catch.hpp>

#include <numeric>

SCENARIO("Signal table Tests")
{
    using namespace pod5;

    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    std::mt19937 gen{Catch::rngSeed()};
    auto uuid_gen = pod5::UuidRandomGenerator{gen};

    auto file_identifier = uuid_gen();

    auto read_id_1 = uuid_gen();
    auto read_id_2 = uuid_gen();
    std::vector<std::int16_t> signal_1(100'000);
    std::iota(signal_1.begin(), signal_1.end(), 0);
    std::vector<std::int16_t> signal_2(10'000, 1);

    GIVEN("A signal table writer")
    {
        auto filename = "./foo.pod5";
        auto pool = arrow::system_memory_pool();

        auto signal_type = GENERATE(
            SignalType::UncompressedSignal, SignalType::VbzSignal, SignalType::PdzSignal);

        {
            auto file_out =
                *pod5::AsyncOutputStream::make(filename, pod5::make_thread_pool(1), true);
            auto schema_metadata = make_schema_key_value_metadata(
                {file_identifier, "test_software", *parse_version_number(Pod5Version)});
            REQUIRE_ARROW_STATUS_OK(schema_metadata);

            auto writer =
                pod5::make_signal_table_writer(file_out, *schema_metadata, 100, signal_type, pool);
            REQUIRE_ARROW_STATUS_OK(writer);

            WHEN("Writing a read")
            {
                auto row_1 = writer->add_signal(read_id_1, gsl::make_span(signal_1));

                auto row_2 = writer->add_signal(read_id_2, gsl::make_span(signal_2));

                REQUIRE_ARROW_STATUS_OK(writer->close());

                THEN("Read row ids are correct")
                {
                    REQUIRE_ARROW_STATUS_OK(row_1);
                    REQUIRE_ARROW_STATUS_OK(row_2);
                    CHECK(*row_1 == 0);
                    CHECK(*row_2 == 1);
                }
            }
        }

        auto file_in = arrow::io::ReadableFile::Open(filename, pool);
        {
            REQUIRE_ARROW_STATUS_OK(file_in);

            auto reader = pod5::make_signal_table_reader(*file_in, 20, pool);
            CAPTURE(reader);
            REQUIRE_ARROW_STATUS_OK(reader);

            auto metadata = reader->schema_metadata();
            CHECK(metadata.file_identifier == file_identifier);
            CHECK(metadata.writing_software == "test_software");
            CHECK(metadata.writing_pod5_version == *parse_version_number(Pod5Version));

            REQUIRE(reader->num_record_batches() == 1);
            auto const record_batch_0 = reader->read_record_batch(0);
            REQUIRE_ARROW_STATUS_OK(record_batch_0);
            REQUIRE(record_batch_0->num_rows() == 2);

            auto read_id = record_batch_0->read_id_column();
            CHECK(read_id->length() == 2);
            CHECK(read_id->Value(0) == read_id_1);
            CHECK(read_id->Value(1) == read_id_2);

            if (signal_type == SignalType::VbzSignal) {
                auto signal = record_batch_0->vbz_signal_column();
                CHECK(signal->length() == 2);

                auto compare_compressed_signal =
                    [&](gsl::span<std::uint8_t const> compressed_actual,
                        std::vector<std::int16_t> const & expected) {
                        auto decompressed =
                            pod5::decompress_signal(compressed_actual, expected.size(), pool);
                        REQUIRE_ARROW_STATUS_OK(decompressed);

                        auto actual =
                            gsl::make_span((*decompressed)->data(), (*decompressed)->size())
                                .as_span<std::int16_t const>();
                        CHECK(actual == gsl::make_span(expected));
                    };

                auto signal_typed = std::static_pointer_cast<VbzSignalArray>(signal);
                compare_compressed_signal(signal_typed->Value(0), signal_1);
                compare_compressed_signal(signal_typed->Value(1), signal_2);
            } else if (signal_type == SignalType::PdzSignal) {
                auto signal = record_batch_0->pdz_signal_column();
                CHECK(signal->length() == 2);

                auto compare_compressed_signal_pdz =
                    [&](gsl::span<std::uint8_t const> compressed_actual,
                        std::vector<std::int16_t> const & expected) {
                        std::vector<std::int16_t> decompressed(expected.size(), 0);
                        REQUIRE_ARROW_STATUS_OK(pod5::decompress_signal_pdz(
                            compressed_actual, pool, gsl::make_span(decompressed)));
                        CHECK(decompressed == expected);
                    };

                auto signal_typed = std::static_pointer_cast<PdzSignalArray>(signal);
                compare_compressed_signal_pdz(signal_typed->Value(0), signal_1);
                compare_compressed_signal_pdz(signal_typed->Value(1), signal_2);
            } else if (signal_type == SignalType::UncompressedSignal) {
                auto signal = record_batch_0->uncompressed_signal_column();
                CHECK(signal->length() == 2);

                auto signal_1_read =
                    std::static_pointer_cast<arrow::Int16Array>(signal->value_slice(0));
                std::vector<std::int16_t> stored_values_1(
                    signal_1_read->raw_values(),
                    signal_1_read->raw_values() + signal_1_read->length());
                CHECK(stored_values_1 == signal_1);
                auto signal_2_read =
                    std::static_pointer_cast<arrow::Int16Array>(signal->value_slice(1));
                std::vector<std::int16_t> stored_values_2(
                    signal_2_read->raw_values(),
                    signal_2_read->raw_values() + signal_2_read->length());
                CHECK(stored_values_2 == signal_2);
            } else {
                FAIL("Unknown signal type");
            }

            auto samples = record_batch_0->samples_column();
            CHECK(samples->length() == 2);
            CHECK(samples->Value(0) == signal_1.size());
            CHECK(samples->Value(1) == signal_2.size());
        }
    }
}

SCENARIO("Signal table PDZ write/read dispatch")
{
    using namespace pod5;

    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    std::mt19937 gen{Catch::rngSeed()};
    auto uuid_gen = pod5::UuidRandomGenerator{gen};
    auto file_identifier = uuid_gen();

    auto pool = arrow::system_memory_pool();
    auto filename = "./pdz_signal.pod5";

    // Reads of varied length, including the 0 / 1 / large edge cases.
    std::vector<std::vector<std::int16_t>> base_signals;
    base_signals.push_back({});  // empty
    base_signals.push_back({-7});  // single sample
    base_signals.push_back({0, 1, -1, 2, -2, 100, -100, 32767, -32768});
    {
        std::vector<std::int16_t> ramp(50'000);
        std::iota(ramp.begin(), ramp.end(), static_cast<std::int16_t>(-25'000));
        base_signals.push_back(std::move(ramp));
    }
    base_signals.push_back(std::vector<std::int16_t>(8'000, 7));  // constant
    {
        std::mt19937 sig_gen{0xC0FFEE};
        std::vector<std::int16_t> noisy(4'321);
        for (auto & s : noisy) {
            s = static_cast<std::int16_t>(sig_gen());
        }
        base_signals.push_back(std::move(noisy));
    }

    // Repeat the set so the rows span several record batches.
    std::vector<std::vector<std::int16_t>> signals;
    for (int repeat = 0; repeat < 3; ++repeat) {
        for (auto const & s : base_signals) {
            signals.push_back(s);
        }
    }

    std::vector<Uuid> read_ids;
    for (std::size_t i = 0; i < signals.size(); ++i) {
        read_ids.push_back(uuid_gen());
    }

    std::size_t const table_batch_size = 4;

    GIVEN("A PDZ signal table written across several record batches")
    {
        {
            auto file_out =
                *pod5::AsyncOutputStream::make(filename, pod5::make_thread_pool(1), true);
            auto schema_metadata = make_schema_key_value_metadata(
                {file_identifier, "test_software", *parse_version_number(Pod5Version)});
            REQUIRE_ARROW_STATUS_OK(schema_metadata);

            auto writer = pod5::make_signal_table_writer(
                file_out, *schema_metadata, table_batch_size, SignalType::PdzSignal, pool);
            REQUIRE_ARROW_STATUS_OK(writer);

            for (std::size_t i = 0; i < signals.size(); ++i) {
                auto row = writer->add_signal(read_ids[i], gsl::make_span(signals[i]));
                REQUIRE_ARROW_STATUS_OK(row);
                CHECK(*row == i);
            }
            REQUIRE_ARROW_STATUS_OK(writer->close());
        }

        auto file_in = arrow::io::ReadableFile::Open(filename, pool);
        REQUIRE_ARROW_STATUS_OK(file_in);

        // Small cache to exercise the reader's batch-cache eviction.
        auto reader = pod5::make_signal_table_reader(*file_in, 2, pool);
        REQUIRE_ARROW_STATUS_OK(reader);

        CHECK(reader->signal_type() == SignalType::PdzSignal);

        std::size_t const expected_batches =
            (signals.size() + table_batch_size - 1) / table_batch_size;
        CHECK(reader->num_record_batches() == expected_batches);

        WHEN("Extracting each read's samples through the reader")
        {
            for (std::size_t i = 0; i < signals.size(); ++i) {
                std::uint64_t const row = i;
                auto row_span = gsl::make_span(&row, 1);

                auto sample_count = reader->extract_sample_count(row_span);
                REQUIRE_ARROW_STATUS_OK(sample_count);
                CHECK(*sample_count == signals[i].size());

                std::vector<std::int16_t> decoded(signals[i].size(), 0);
                REQUIRE_ARROW_STATUS_OK(reader->extract_samples(row_span, gsl::make_span(decoded)));
                CHECK(decoded == signals[i]);
            }
        }

        WHEN("Extracting raw PDZ buffers in place")
        {
            std::vector<std::uint64_t> all_rows(signals.size());
            std::iota(all_rows.begin(), all_rows.end(), std::uint64_t{0});

            std::vector<std::uint32_t> sample_counts;
            auto buffers = reader->extract_samples_inplace(gsl::make_span(all_rows), sample_counts);
            REQUIRE_ARROW_STATUS_OK(buffers);
            REQUIRE(buffers->size() == signals.size());
            REQUIRE(sample_counts.size() == signals.size());

            for (std::size_t i = 0; i < signals.size(); ++i) {
                CHECK(sample_counts[i] == signals[i].size());

                // The in-place buffer is the raw compressed PDZ block; its size
                // must agree with samples_byte_count for the same row.
                std::size_t batch_row = 0;
                auto batch_index = reader->signal_batch_for_row_id(i, &batch_row);
                REQUIRE_ARROW_STATUS_OK(batch_index);
                auto record_batch = reader->read_record_batch(*batch_index);
                REQUIRE_ARROW_STATUS_OK(record_batch);
                auto byte_count = record_batch->samples_byte_count(batch_row);
                REQUIRE_ARROW_STATUS_OK(byte_count);

                CHECK((*buffers)[i]->size() == *byte_count);

                // And it must decode back to the original samples.
                std::vector<std::int16_t> decoded(signals[i].size(), 0);
                REQUIRE_ARROW_STATUS_OK(pod5::decompress_signal_pdz(
                    gsl::make_span((*buffers)[i]->data(), (*buffers)[i]->size()),
                    pool,
                    gsl::make_span(decoded)));
                CHECK(decoded == signals[i]);
            }
        }

        WHEN("Inspecting the dispatched on-disk extension type")
        {
            // Schema dispatch must resolve the column to the PDZ extension type,
            // never VBZ — a PDZ file cannot be read as the wrong type.
            auto record_batch = reader->read_record_batch(0);
            REQUIRE_ARROW_STATUS_OK(record_batch);
            auto column = record_batch->pdz_signal_column();
            REQUIRE(column != nullptr);
            CHECK(column->type()->Equals(*pdz_signal()));
            CHECK_FALSE(column->type()->Equals(*vbz_signal()));
        }
    }
}
