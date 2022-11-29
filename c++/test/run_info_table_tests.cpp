#include "pod5_format/run_info_table_reader.h"
#include "pod5_format/run_info_table_writer.h"
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

SCENARIO("Run Info table Tests")
{
    using namespace pod5;

    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();

    auto file_identifier = uuid_gen();

    GIVEN("A read table writer")
    {
        auto filename = "./foo.pod5";
        auto pool = arrow::system_memory_pool();

        auto file_out = arrow::io::FileOutputStream::Open(filename, pool);

        auto run_info_data_0 = get_test_run_info_data();
        auto run_info_data_1 = get_test_run_info_data("_2");

        {
            auto schema_metadata = make_schema_key_value_metadata(
                {file_identifier, "test_software", *parse_version_number(Pod5Version)});
            REQUIRE_ARROW_STATUS_OK(schema_metadata);
            REQUIRE_ARROW_STATUS_OK(file_out);

            std::size_t run_info_per_batch = 2;

            auto writer = pod5::make_run_info_table_writer(
                *file_out, *schema_metadata, run_info_per_batch, pool);
            REQUIRE_ARROW_STATUS_OK(writer);

            REQUIRE_ARROW_STATUS_OK(writer->add_run_info(run_info_data_0));
            REQUIRE_ARROW_STATUS_OK(writer->add_run_info(run_info_data_1));
        }

        auto file_in = arrow::io::ReadableFile::Open(filename, pool);
        {
            REQUIRE_ARROW_STATUS_OK(file_in);

            auto reader = pod5::make_run_info_table_reader(*file_in, pool);
            REQUIRE_ARROW_STATUS_OK(reader);

            auto metadata = reader->schema_metadata();
            CHECK(metadata.file_identifier == file_identifier);
            CHECK(metadata.writing_software == "test_software");
            CHECK(metadata.writing_pod5_version == *parse_version_number(Pod5Version));

            REQUIRE(reader->num_record_batches() == 1);
            auto const record_batch = reader->read_record_batch(0);
            REQUIRE_ARROW_STATUS_OK(record_batch);
            REQUIRE(record_batch->num_rows() == 2);

            auto columns = record_batch->columns();
            REQUIRE_ARROW_STATUS_OK(columns);

            auto check_run_info = [](auto & columns,
                                     std::size_t index,
                                     pod5::RunInfoData const & run_info_data) {
                CHECK(columns.acquisition_id->Value(index) == run_info_data.acquisition_id);

                CHECK(
                    columns.acquisition_start_time->Value(index)
                    == run_info_data.acquisition_start_time);
                CHECK(columns.adc_max->Value(index) == run_info_data.adc_max);
                CHECK(columns.adc_min->Value(index) == run_info_data.adc_min);
                CHECK(columns.experiment_name->Value(index) == run_info_data.experiment_name);
                CHECK(columns.flow_cell_id->Value(index) == run_info_data.flow_cell_id);
                CHECK(
                    columns.flow_cell_product_code->Value(index)
                    == run_info_data.flow_cell_product_code);
                CHECK(columns.protocol_name->Value(index) == run_info_data.protocol_name);
                CHECK(columns.protocol_run_id->Value(index) == run_info_data.protocol_run_id);
                CHECK(
                    columns.protocol_start_time->Value(index) == run_info_data.protocol_start_time);
                CHECK(columns.sample_id->Value(index) == run_info_data.sample_id);
                CHECK(columns.sample_rate->Value(index) == run_info_data.sample_rate);
                CHECK(columns.sequencing_kit->Value(index) == run_info_data.sequencing_kit);
                CHECK(columns.sequencer_position->Value(index) == run_info_data.sequencer_position);
                CHECK(
                    columns.sequencer_position_type->Value(index)
                    == run_info_data.sequencer_position_type);
                CHECK(columns.software->Value(index) == run_info_data.software);
                CHECK(columns.system_name->Value(index) == run_info_data.system_name);
                CHECK(columns.system_type->Value(index) == run_info_data.system_type);
            };

            check_run_info(*columns, 0, run_info_data_0);
            check_run_info(*columns, 1, run_info_data_1);

            auto found_run_info_0 = reader->find_run_info(run_info_data_0.acquisition_id);
            CHECK_ARROW_STATUS_OK(found_run_info_0);
            CHECK(**found_run_info_0 == run_info_data_0);

            auto found_run_info_1 = reader->find_run_info(run_info_data_1.acquisition_id);
            CHECK_ARROW_STATUS_OK(found_run_info_1);
            CHECK(**found_run_info_1 == run_info_data_1);
        }
    }
}
