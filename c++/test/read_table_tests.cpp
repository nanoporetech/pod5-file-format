#include "pod5_format/read_table_reader.h"
#include "pod5_format/read_table_writer.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/types.h"
#include "pod5_format/version.h"
#include "test_utils.h"
#include "utils.h"

#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <boost/uuid/random_generator.hpp>
#include <catch2/catch.hpp>

bool operator==(
    std::shared_ptr<arrow::UInt64Array> const & array,
    std::vector<std::uint64_t> const & vec)
{
    if (array->length() != vec.size()) {
        return false;
    }

    for (std::size_t i = 0; i < array->length(); ++i) {
        if ((*array)[i] != vec[i]) {
            return false;
        }
    }
    return true;
}

SCENARIO("Read table Tests")
{
    using namespace pod5;

    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();

    auto file_identifier = uuid_gen();

    auto data_for_index = [&](std::size_t index) {
        std::array<std::uint8_t, 16> uuid_source{};
        gsl::make_span(uuid_source).as_span<std::uint8_t>()[0] = index;

        boost::uuids::uuid read_id{};
        std::copy(uuid_source.begin(), uuid_source.end(), read_id.begin());

        return std::make_tuple(
            pod5::ReadData{
                read_id,
                std::uint32_t(index * 2),
                std::uint64_t(index * 10),
                std::uint16_t(index + 1),
                std::uint8_t(index + 2),
                0,
                index * 0.1f,
                index * 0.2f,
                index * 100.0f,
                0,
                true,
                0,
                std::uint64_t(index * 150),
                index * 0.4f,
                index * 0.3f,
                index * 0.6f,
                index * 0.5f,
                std::uint32_t(index + 10),
                index * 50.0f,

            },
            std::vector<std::uint64_t>{index + 2, index + 3});
    };

    GIVEN("A read table writer")
    {
        auto filename = "./foo.pod5";
        auto pool = arrow::system_memory_pool();

        auto file_out = arrow::io::FileOutputStream::Open(filename, pool);

        auto const record_batch_count = GENERATE(1, 2, 5, 10);
        auto const read_count = GENERATE(1, 2);

        {
            auto schema_metadata = make_schema_key_value_metadata(
                {file_identifier, "test_software", *parse_version_number(Pod5Version)});
            REQUIRE_ARROW_STATUS_OK(schema_metadata);
            REQUIRE_ARROW_STATUS_OK(file_out);

            auto pore_writer = pod5::make_pore_writer(pool);
            REQUIRE_ARROW_STATUS_OK(pore_writer);
            auto end_reason_writer = pod5::make_end_reason_writer(pool);
            REQUIRE_ARROW_STATUS_OK(end_reason_writer);
            auto run_info_writer = pod5::make_run_info_writer(pool);
            REQUIRE_ARROW_STATUS_OK(run_info_writer);

            auto writer = pod5::make_read_table_writer(
                *file_out,
                *schema_metadata,
                read_count,
                *pore_writer,
                *end_reason_writer,
                *run_info_writer,
                pool);
            REQUIRE_ARROW_STATUS_OK(writer);

            auto const pore_1 = (*pore_writer)->add("Well Type");
            REQUIRE_ARROW_STATUS_OK(pore_1);
            auto const end_reason_1 = (*end_reason_writer)->lookup(pod5::ReadEndReason::mux_change);
            REQUIRE_ARROW_STATUS_OK(end_reason_1);
            auto const run_info_1 = (*run_info_writer)->add("acq_id_1");
            REQUIRE_ARROW_STATUS_OK(run_info_1);
            auto const run_info_2 = (*run_info_writer)->add("acq_id_2");
            REQUIRE_ARROW_STATUS_OK(run_info_2);

            for (std::size_t i = 0; i < record_batch_count; ++i) {
                for (std::size_t j = 0; j < read_count; ++j) {
                    auto const idx = j + i * read_count;

                    pod5::ReadData read_data;
                    std::vector<std::uint64_t> signal;
                    std::tie(read_data, signal) = data_for_index(idx);
                    auto row = writer->add_read(read_data, signal, signal.size());

                    REQUIRE_ARROW_STATUS_OK(row);
                    CHECK(*row == idx);
                }
            }
            REQUIRE_ARROW_STATUS_OK(writer->close());
        }

        auto file_in = arrow::io::ReadableFile::Open(filename, pool);
        {
            REQUIRE_ARROW_STATUS_OK(file_in);

            auto reader = pod5::make_read_table_reader(*file_in, pool);
            REQUIRE_ARROW_STATUS_OK(reader);

            auto metadata = reader->schema_metadata();
            CHECK(metadata.file_identifier == file_identifier);
            CHECK(metadata.writing_software == "test_software");
            CHECK(metadata.writing_pod5_version == *parse_version_number(Pod5Version));

            REQUIRE(reader->num_record_batches() == record_batch_count);
            for (std::size_t i = 0; i < record_batch_count; ++i) {
                auto const record_batch = reader->read_record_batch(i);
                REQUIRE_ARROW_STATUS_OK(record_batch);
                REQUIRE(record_batch->num_rows() == read_count);

                auto columns = record_batch->columns();

                CHECK(columns->read_id->length() == read_count);
                CHECK(columns->signal->length() == read_count);
                CHECK(columns->channel->length() == read_count);
                CHECK(columns->well->length() == read_count);
                CHECK(columns->pore_type->length() == read_count);
                CHECK(columns->calibration_offset->length() == read_count);
                CHECK(columns->calibration_scale->length() == read_count);
                CHECK(columns->read_number->length() == read_count);
                CHECK(columns->start_sample->length() == read_count);
                CHECK(columns->median_before->length() == read_count);
                CHECK(columns->num_samples->length() == read_count);
                CHECK(columns->end_reason->length() == read_count);
                CHECK(columns->end_reason_forced->length() == read_count);
                CHECK(columns->run_info->length() == read_count);

                auto pore_indices =
                    std::static_pointer_cast<arrow::Int16Array>(columns->pore_type->indices());
                auto end_reason_indices =
                    std::static_pointer_cast<arrow::Int16Array>(columns->end_reason->indices());
                auto run_info_indices =
                    std::static_pointer_cast<arrow::Int16Array>(columns->run_info->indices());
                for (auto j = 0; j < read_count; ++j) {
                    auto idx = j + i * read_count;

                    pod5::ReadData read_data;
                    std::vector<std::uint64_t> expected_signal;
                    std::tie(read_data, expected_signal) = data_for_index(idx);

                    CHECK(columns->read_id->Value(j) == read_data.read_id);

                    auto signal_data = std::static_pointer_cast<arrow::UInt64Array>(
                        columns->signal->value_slice(j));
                    CHECK(
                        gsl::make_span(signal_data->raw_values(), signal_data->length())
                        == gsl::make_span(expected_signal));

                    CHECK(columns->read_number->Value(j) == read_data.read_number);
                    CHECK(columns->start_sample->Value(j) == read_data.start_sample);
                    CHECK(columns->median_before->Value(j) == read_data.median_before);
                    CHECK(columns->num_samples->Value(j) == expected_signal.size());
                    CHECK(columns->calibration_offset->Value(j) == read_data.calibration_offset);
                    CHECK(columns->calibration_scale->Value(j) == read_data.calibration_scale);
                    CHECK(columns->channel->Value(j) == read_data.channel);
                    CHECK(columns->well->Value(j) == read_data.well);

                    CHECK(end_reason_indices->Value(j) == read_data.end_reason);
                    CHECK(pore_indices->Value(j) == read_data.pore_type);
                    CHECK(run_info_indices->Value(j) == read_data.run_info);
                }

                auto pore_data = record_batch->get_pore_type(0);
                REQUIRE_ARROW_STATUS_OK(pore_data);
                CHECK(*pore_data == "Well Type");

                auto end_reason_data = record_batch->get_end_reason(1);
                REQUIRE_ARROW_STATUS_OK(end_reason_data);
                CHECK(end_reason_data->first == pod5::ReadEndReason::mux_change);
                CHECK(end_reason_data->second == "mux_change");

                auto run_info_data = record_batch->get_run_info(0);
                REQUIRE_ARROW_STATUS_OK(run_info_data);
                CHECK(*run_info_data == "acq_id_1");

                run_info_data = record_batch->get_run_info(1);
                REQUIRE_ARROW_STATUS_OK(run_info_data);
                CHECK(*run_info_data == "acq_id_2");
            }
        }
    }
}
