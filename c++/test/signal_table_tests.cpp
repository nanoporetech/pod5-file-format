#include "pod5_format/schema_metadata.h"
#include "pod5_format/signal_compression.h"
#include "pod5_format/signal_table_reader.h"
#include "pod5_format/signal_table_writer.h"
#include "pod5_format/types.h"
#include "pod5_format/version.h"
#include "test_utils.h"
#include "utils.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <boost/uuid/random_generator.hpp>
#include <catch2/catch.hpp>

#include <numeric>

SCENARIO("Signal table Tests")
{
    using namespace pod5;

    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();

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

        auto file_out = arrow::io::FileOutputStream::Open(filename, pool);

        auto signal_type = GENERATE(SignalType::UncompressedSignal, SignalType::VbzSignal);

        {
            auto schema_metadata = make_schema_key_value_metadata(
                {file_identifier, "test_software", *parse_version_number(Pod5Version)});
            REQUIRE_ARROW_STATUS_OK(schema_metadata);
            REQUIRE_ARROW_STATUS_OK(file_out);

            auto writer =
                pod5::make_signal_table_writer(*file_out, *schema_metadata, 100, signal_type, pool);
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
