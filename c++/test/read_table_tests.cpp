#include "mkr_format/read_table_reader.h"
#include "mkr_format/read_table_writer.h"
#include "mkr_format/schema_metadata.h"
#include "mkr_format/types.h"
#include "mkr_format/version.h"
#include "utils.h"

#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <boost/uuid/random_generator.hpp>
#include <catch2/catch.hpp>

bool operator==(std::shared_ptr<arrow::UInt64Array> const& array,
                std::vector<std::uint64_t> const& vec) {
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

SCENARIO("Read table Tests") {
    using namespace mkr;

    (void)mkr::register_extension_types();
    auto fin = gsl::finally([] { (void)mkr::unregister_extension_types(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();

    auto file_identifier = uuid_gen();

    auto data_for_index = [&](std::size_t index) {
        std::array<std::uint8_t, 16> uuid_source;
        gsl::make_span(uuid_source).as_span<std::uint64_t>()[0] = index;

        boost::uuids::uuid read_id;
        std::copy(uuid_source.begin(), uuid_source.end(), read_id.begin());

        return std::make_tuple(
                mkr::ReadData{
                        read_id, 0, 0, std::uint32_t(index * 2), std::uint64_t(index * 10),
                        index * 5.0f, 0,
                        0  //std::int16_t(index % 2)

                },
                std::vector<std::uint64_t>{index + 2, index + 3});
    };

    GIVEN("A read table writer") {
        auto filename = "./foo.mkr";
        auto pool = arrow::system_memory_pool();

        auto file_out = arrow::io::FileOutputStream::Open(filename, pool);

        auto const record_batch_count = GENERATE(1, 2, 5, 10);
        auto const read_count = GENERATE(1, 2);

        auto run_info_data_0 = get_test_run_info_data();
        auto run_info_data_1 = get_test_run_info_data("_2");

        {
            auto schema_metadata =
                    make_schema_key_value_metadata({file_identifier, "test_software", MkrVersion});
            REQUIRE(schema_metadata.ok());
            REQUIRE(file_out.ok());

            auto pore_writer = mkr::make_pore_writer(pool);
            REQUIRE(pore_writer.ok());
            auto calibration_writer = mkr::make_calibration_writer(pool);
            REQUIRE(calibration_writer.ok());
            auto end_reason_writer = mkr::make_end_reason_writer(pool);
            REQUIRE(end_reason_writer.ok());
            auto run_info_writer = mkr::make_run_info_writer(pool);
            REQUIRE(run_info_writer.ok());

            auto writer = mkr::make_read_table_writer(*file_out, *schema_metadata, *pore_writer,
                                                      *calibration_writer, *end_reason_writer,
                                                      *run_info_writer, pool);
            REQUIRE(writer.ok());

            auto const pore_1 = (*pore_writer)->add({12, 2, "Well Type"});
            REQUIRE(pore_1.ok());
            auto const calib_1 = (*calibration_writer)->add({100.0f, 0.5f});
            REQUIRE(calib_1.ok());
            auto const end_reason_1 =
                    (*end_reason_writer)
                            ->add({mkr::EndReasonData::ReadEndReason::mux_change, false});
            REQUIRE(end_reason_1.ok());
            auto const run_info_1 = (*run_info_writer)->add(run_info_data_0);
            REQUIRE(run_info_1.ok());
            auto const run_info_2 = (*run_info_writer)->add(run_info_data_1);
            REQUIRE(run_info_2.ok());

            for (std::size_t i = 0; i < record_batch_count; ++i) {
                for (std::size_t j = 0; j < read_count; ++j) {
                    auto const idx = j + i * read_count;

                    mkr::ReadData read_data;
                    std::vector<std::uint64_t> signal;
                    std::tie(read_data, signal) = data_for_index(idx);
                    auto row = writer->add_read(read_data, signal);

                    REQUIRE(row.ok());
                    CHECK(*row == idx);
                }

                auto flush_res = writer->flush();
                CAPTURE(flush_res);
                REQUIRE(flush_res.ok());
            }
            REQUIRE(writer->close().ok());
        }

        auto file_in = arrow::io::ReadableFile::Open(filename, pool);
        {
            REQUIRE(file_in.ok());

            auto reader = mkr::make_read_table_reader(*file_in, pool);
            CAPTURE(reader);
            REQUIRE(reader.ok());

            auto metadata = reader->schema_metadata();
            CHECK(metadata.file_identifier == file_identifier);
            CHECK(metadata.writing_software == "test_software");
            CHECK(metadata.writing_mkr_version == MkrVersion);

            REQUIRE(reader->num_record_batches() == record_batch_count);
            for (std::size_t i = 0; i < record_batch_count; ++i) {
                auto const record_batch = reader->read_record_batch(i);
                CAPTURE(record_batch);
                REQUIRE(record_batch.ok());
                REQUIRE(record_batch->num_rows() == read_count);

                auto read_id = record_batch->read_id_column();
                CHECK(read_id->length() == read_count);

                auto signal = record_batch->signal_column();
                CHECK(signal->length() == read_count);

                auto pore = record_batch->pore_column();
                CHECK(pore->length() == read_count);

                auto calibration = record_batch->calibration_column();
                CHECK(calibration->length() == read_count);

                auto read_number = record_batch->read_number_column();
                CHECK(read_number->length() == read_count);

                auto start_sample = record_batch->start_sample_column();
                CHECK(start_sample->length() == read_count);

                auto median_before = record_batch->median_before_column();
                CHECK(median_before->length() == read_count);

                auto end_reason = record_batch->end_reason_column();
                CHECK(end_reason->length() == read_count);

                auto run_info = record_batch->run_info_column();
                CHECK(run_info->length() == read_count);

                auto pore_indices = std::static_pointer_cast<arrow::Int16Array>(pore->indices());
                auto calibration_indices =
                        std::static_pointer_cast<arrow::Int16Array>(calibration->indices());
                auto end_reason_indices =
                        std::static_pointer_cast<arrow::Int16Array>(end_reason->indices());
                auto run_info_indices =
                        std::static_pointer_cast<arrow::Int16Array>(run_info->indices());
                for (auto j = 0; j < read_count; ++j) {
                    auto idx = j + i * read_count;

                    mkr::ReadData read_data;
                    std::vector<std::uint64_t> expected_signal;
                    std::tie(read_data, expected_signal) = data_for_index(idx);

                    CHECK(read_id->Value(j) == read_data.read_id);

                    auto signal_data =
                            std::static_pointer_cast<arrow::UInt64Array>(signal->value_slice(j));
                    CHECK(gsl::make_span(signal_data->raw_values(), signal_data->length()) ==
                          gsl::make_span(expected_signal));

                    CHECK(read_number->Value(j) == read_data.read_number);
                    CHECK(start_sample->Value(j) == read_data.start_sample);
                    CHECK(median_before->Value(j) == read_data.median_before);

                    CHECK(calibration_indices->Value(j) == read_data.calibration);
                    CHECK(end_reason_indices->Value(j) == read_data.end_reason);
                    CHECK(pore_indices->Value(j) == read_data.pore);
                    CHECK(run_info_indices->Value(j) == read_data.run_info);
                }

                auto pore_data = record_batch->get_pore(0);
                REQUIRE(pore_data.ok());
                CHECK(pore_data->channel == 12);
                CHECK(pore_data->well == 2);
                CHECK(pore_data->pore_type == "Well Type");

                auto calibration_data = record_batch->get_calibration(0);
                REQUIRE(calibration_data.ok());
                CHECK(calibration_data->offset == 100.0f);
                CHECK(calibration_data->scale == 0.5f);

                auto end_reason_data = record_batch->get_end_reason(0);
                REQUIRE(end_reason_data.ok());
                CHECK(end_reason_data->name == "mux_change");
                CHECK(end_reason_data->forced == false);

                auto run_info_data = record_batch->get_run_info(0);
                CAPTURE(run_info_data);
                REQUIRE(run_info_data.ok());
                CHECK(*run_info_data == run_info_data_0);

                run_info_data = record_batch->get_run_info(1);
                CAPTURE(run_info_data);
                REQUIRE(run_info_data.ok());
                CHECK(*run_info_data == run_info_data_1);
            }
        }
    }
}