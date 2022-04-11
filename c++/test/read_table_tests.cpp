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

    auto types = register_extension_types();

    auto uuid_gen = boost::uuids::random_generator_mt19937();

    auto file_identifier = uuid_gen();

    auto read_id_1 = uuid_gen();
    auto read_id_2 = uuid_gen();

    std::vector<std::uint64_t> signal_rows_1{1, 3};
    std::vector<std::uint64_t> signal_rows_2{2, 5};

    std::uint32_t read_number_1 = 2;
    std::uint32_t read_number_2 = 4;

    std::uint64_t start_sample_1 = 1000;
    std::uint64_t start_sample_2 = 5000;

    float median_before_1 = 100.0f;
    float median_before_2 = 200.0f;

    GIVEN("A read table writer") {
        auto filename = "./foo.mkr";
        auto pool = arrow::system_memory_pool();

        auto file_out = arrow::io::FileOutputStream::Open(filename, pool);

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

            WHEN("Writing a read") {
                auto const pore_1 = (*pore_writer)->add({12, 2, "Well Type"});
                REQUIRE(pore_1.ok());
                auto const calib_1 = (*calibration_writer)->add({100.0f, 0.5f});
                REQUIRE(calib_1.ok());
                auto const end_reason_1 =
                        (*end_reason_writer)
                                ->add({mkr::EndReasonData::ReadEndReason::mux_change, false});
                REQUIRE(end_reason_1.ok());
                auto const run_info_1 = (*run_info_writer)->add(get_test_run_info_data());
                REQUIRE(run_info_1.ok());
                auto const run_info_2 = (*run_info_writer)->add(get_test_run_info_data("_2"));
                REQUIRE(run_info_2.ok());
                auto row_1 = writer->add_read(
                        {read_id_1, *pore_1, *calib_1, read_number_1, start_sample_1,
                         median_before_1, *end_reason_1, *run_info_1},
                        gsl::make_span(signal_rows_1));
                auto row_2 = writer->add_read(
                        {read_id_2, *pore_1, *calib_1, read_number_2, start_sample_2,
                         median_before_2, *end_reason_1, *run_info_2},
                        gsl::make_span(signal_rows_2));

                auto flush_res = writer->flush();
                CAPTURE(flush_res);
                REQUIRE(flush_res.ok());
                REQUIRE(writer->close().ok());

                THEN("Read row ids are correct") {
                    REQUIRE(row_1.ok());
                    REQUIRE(row_2.ok());
                    CHECK(*row_1 == 0);
                    CHECK(*row_2 == 1);
                }
            }
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

            REQUIRE(reader->num_record_batches() == 1);
            auto const record_batch_0 = reader->read_record_batch(0);
            CAPTURE(record_batch_0);
            REQUIRE(record_batch_0.ok());
            REQUIRE(record_batch_0->num_rows() == 2);

            auto read_id = record_batch_0->read_id_column();
            CHECK(read_id->length() == 2);
            CHECK(read_id->Value(0) == read_id_1);
            CHECK(read_id->Value(1) == read_id_2);

            auto signal = record_batch_0->signal_column();
            CHECK(signal->length() == 2);
            CHECK(std::static_pointer_cast<arrow::UInt64Array>(signal->value_slice(0)) ==
                  signal_rows_1);
            CHECK(std::static_pointer_cast<arrow::UInt64Array>(signal->value_slice(1)) ==
                  signal_rows_2);

            auto pore = record_batch_0->pore_column();
            CHECK(pore->length() == 2);
            auto pore_indices = std::static_pointer_cast<arrow::Int16Array>(pore->indices());
            CHECK(pore_indices->Value(0) == 0);
            CHECK(pore_indices->Value(1) == 0);

            auto calibration = record_batch_0->calibration_column();
            CHECK(calibration->length() == 2);
            auto calibration_indices =
                    std::static_pointer_cast<arrow::Int16Array>(calibration->indices());
            CHECK(calibration_indices->Value(0) == 0);
            CHECK(calibration_indices->Value(1) == 0);

            auto read_number = record_batch_0->read_number_column();
            CHECK(read_number->length() == 2);
            CHECK(read_number->Value(0) == read_number_1);
            CHECK(read_number->Value(1) == read_number_2);

            auto start_sample = record_batch_0->start_sample_column();
            CHECK(start_sample->length() == 2);
            CHECK(start_sample->Value(0) == start_sample_1);
            CHECK(start_sample->Value(1) == start_sample_2);

            auto median_before = record_batch_0->median_before_column();
            CHECK(median_before->length() == 2);
            CHECK(median_before->Value(0) == median_before_1);
            CHECK(median_before->Value(1) == median_before_2);

            auto end_reason = record_batch_0->end_reason_column();
            CHECK(end_reason->length() == 2);
            auto end_reason_indices =
                    std::static_pointer_cast<arrow::Int16Array>(end_reason->indices());
            CHECK(end_reason_indices->Value(0) == 0);
            CHECK(end_reason_indices->Value(1) == 0);

            auto run_info = record_batch_0->run_info_column();
            CHECK(run_info->length() == 2);
            auto run_info_indices =
                    std::static_pointer_cast<arrow::Int16Array>(run_info->indices());
            CHECK(run_info_indices->Value(0) == 0);
            CHECK(run_info_indices->Value(1) == 1);
        }
    }
}