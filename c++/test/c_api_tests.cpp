#include "pod5_format/c_api.h"

#include "pod5_format/file_reader.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/version.h"
#include "utils.h"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <catch2/catch.hpp>
#include <gsl/gsl-lite.hpp>

#include <iostream>
#include <numeric>

struct Pod5C_Result {
    static Pod5C_Result capture(pod5_error_t err_num)
    {
        return Pod5C_Result{err_num, pod5_get_error_string()};
    }

    pod5_error_t error_code;
    std::string error_string;
};

namespace Catch {
template <>
struct StringMaker<Pod5C_Result> {
    static std::string convert(Pod5C_Result const & value)
    {
        return "{ code: " + std::to_string(value.error_code) + "| " + value.error_string + " }";
    }
};
}  // namespace Catch

class IsPod5COk : public Catch::MatcherBase<Pod5C_Result> {
public:
    IsPod5COk() = default;

    bool match(Pod5C_Result const & result) const override { return result.error_code == POD5_OK; }

    virtual std::string describe() const override { return "== POD5_OK"; }
};

#define CHECK_POD5_OK(statement)                              \
    {                                                         \
        auto const & _res = (statement);                      \
        CHECK_THAT(Pod5C_Result::capture(_res), IsPod5COk()); \
    }

struct Pod5ReadId {
    Pod5ReadId() = default;

    Pod5ReadId(boost::uuids::uuid uid)
    {
        std::copy((uint8_t *)uid.begin(), (uint8_t *)uid.end(), read_id);
    }

    boost::uuids::uuid as_uuid() const
    {
        boost::uuids::uuid uid;
        std::copy(read_id, read_id + sizeof(read_id), (uint8_t *)uid.begin());
        return uid;
    }

    bool operator==(Pod5ReadId const & other) const { return as_uuid() == other.as_uuid(); }

    read_id_t read_id;
};

std::ostream & operator<<(std::ostream & str, Pod5ReadId rid) { return str << rid.as_uuid(); }

SCENARIO("C API Reads")
{
    static constexpr char const * filename = "./foo_c_api.pod5";

    pod5_init();
    auto fin = gsl::finally([] { pod5_terminate(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto input_read_id = uuid_gen();
    auto input_read_id_2 = uuid_gen();
    std::vector<int16_t> signal_1(10);
    std::iota(signal_1.begin(), signal_1.end(), -20000);

    std::vector<int16_t> signal_2(20);
    std::iota(signal_2.begin(), signal_2.end(), 0);

    std::size_t read_count = 0;

    std::int16_t adc_min = -4096;
    std::int16_t adc_max = 4095;

    float calibration_offset = 54.0f;
    float calibration_scale = 100.0f;

    float predicted_scale = 2.3f;
    float predicted_shift = 10.0f;
    float tracked_scale = 4.3f;
    float tracked_shift = 15.0f;
    std::uint32_t num_reads_since_mux_change = 1234;
    float time_since_mux_change = 2.4f;
    std::uint64_t num_minknow_events = 104;

    // Write the file:
    {
        CHECK_POD5_OK(pod5_get_error_no());
        CHECK(!pod5_create_file(NULL, "c_software", NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);
        CHECK(!pod5_create_file("", "c_software", NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);
        CHECK(!pod5_create_file("", NULL, NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);

        REQUIRE(remove_file_if_exists(filename).ok());

        auto file = pod5_create_file(filename, "c_software", NULL);
        REQUIRE(file);
        CHECK_POD5_OK(pod5_get_error_no());

        std::int16_t pore_type_id = -1;
        CHECK_POD5_OK(pod5_add_pore(&pore_type_id, file, "pore_type"));
        CHECK(pore_type_id == 0);

        std::vector<char const *> context_tags_keys{"thing", "foo"};
        std::vector<char const *> context_tags_values{"thing_val", "foo_val"};
        std::vector<char const *> tracking_id_keys{"baz", "other"};
        std::vector<char const *> tracking_id_values{"baz_val", "other_val"};

        std::uint32_t read_number = 12;
        std::uint64_t start_sample = 10245;
        float median_before = 200.0f;
        std::uint16_t channel = 43;
        std::uint8_t well = 4;
        pod5_end_reason_t end_reason = POD5_END_REASON_MUX_CHANGE;
        uint8_t end_reason_forced = false;
        auto read_id_array = (read_id_t const *)input_read_id.begin();

        std::int16_t run_info_id = 0;
        ReadBatchRowInfoArrayV3 row_data{
            read_id_array,
            &read_number,
            &start_sample,
            &median_before,
            &channel,
            &well,
            &pore_type_id,
            &calibration_offset,
            &calibration_scale,
            &end_reason,
            &end_reason_forced,
            &run_info_id,
            &num_minknow_events,
            &tracked_scale,
            &tracked_shift,
            &predicted_scale,
            &predicted_shift,
            &num_reads_since_mux_change,
            &time_since_mux_change};

        std::int16_t const * signal_arr[] = {signal_1.data()};
        std::uint32_t signal_size[] = {(std::uint32_t)signal_1.size()};

        // Referencing a non-existent run id should fail:
        CHECK(
            pod5_add_reads_data(
                file, 1, READ_BATCH_ROW_INFO_VERSION_3, &row_data, signal_arr, signal_size)
            == POD5_ERROR_INVALID);

        // Now actually add the run info:
        CHECK_POD5_OK(pod5_add_run_info(
            &run_info_id,
            file,
            "acquisition_id",
            15400,
            adc_max,
            adc_min,
            context_tags_keys.size(),
            context_tags_keys.data(),
            context_tags_values.data(),
            "experiment_name",
            "flow_cell_id",
            "flow_cell_product_code",
            "protocol_name",
            "protocol_run_id",
            200000,
            "sample_id",
            4000,
            "sequencing_kit",
            "sequencer_position",
            "sequencer_position_type",
            "software",
            "system_name",
            "system_type",
            tracking_id_keys.size(),
            tracking_id_keys.data(),
            tracking_id_values.data()));
        CHECK(run_info_id == 0);

        {
            CHECK_POD5_OK(pod5_add_reads_data(
                file, 1, READ_BATCH_ROW_INFO_VERSION_3, &row_data, signal_arr, signal_size));
            read_count += 1;
        }

        {
            auto compressed_read_max_size = pod5_vbz_compressed_signal_max_size(signal_2.size());
            std::vector<char> compressed_signal(compressed_read_max_size);
            char const * compressed_data[] = {compressed_signal.data()};
            char const ** compressed_data_ptr = compressed_data;
            std::size_t compressed_size[] = {compressed_signal.size()};
            std::size_t const * compressed_size_ptr = compressed_size;
            std::uint32_t signal_size[] = {(std::uint32_t)signal_2.size()};
            std::uint32_t const * signal_size_ptr = signal_size;
            pod5_vbz_compress_signal(
                signal_2.data(), signal_2.size(), compressed_signal.data(), compressed_size);

            std::size_t signal_counts = 1;

            auto read_id_array = (read_id_t const *)input_read_id_2.begin();
            row_data.read_id = read_id_array;

            CHECK_POD5_OK(pod5_add_reads_data_pre_compressed(
                file,
                1,
                READ_BATCH_ROW_INFO_VERSION_3,
                &row_data,
                &compressed_data_ptr,
                &compressed_size_ptr,
                &signal_size_ptr,
                &signal_counts));
            read_count += 1;
        }

        CHECK_POD5_OK(pod5_close_and_free_writer(file));
        CHECK_POD5_OK(pod5_get_error_no());
    }

    // Read the file back:
    {
        CHECK_POD5_OK(pod5_get_error_no());
        CHECK(!pod5_open_file(NULL));
        auto file = pod5_open_file(filename);
        CHECK_POD5_OK(pod5_get_error_no());
        CHECK(!!file);

        FileInfo_t file_info;
        CHECK_POD5_OK(pod5_get_file_info(file, &file_info));
        CHECK(file_info.version.major == pod5::Pod5MajorVersion);
        CHECK(file_info.version.minor == pod5::Pod5MinorVersion);
        CHECK(file_info.version.revision == pod5::Pod5RevVersion);
        {
            auto reader = pod5::open_file_reader(filename);
            boost::uuids::uuid file_identifier;
            std::copy(
                file_info.file_identifier,
                file_info.file_identifier + sizeof(file_info.file_identifier),
                file_identifier.begin());
            CHECK(file_identifier == (*reader)->schema_metadata().file_identifier);
        }

        std::size_t read_count = 0;
        CHECK_POD5_OK(pod5_get_read_count(file, &read_count));
        REQUIRE(read_count == 2);

        std::vector<Pod5ReadId> read_ids(2);
        CHECK(pod5_get_read_ids(file, 1, (read_id_t *)read_ids.data()) != POD5_OK);
        CHECK_POD5_OK(pod5_get_read_ids(file, read_ids.size(), (read_id_t *)read_ids.data()));
        std::vector<Pod5ReadId> expected_read_ids{input_read_id, input_read_id_2};
        CHECK(read_ids == expected_read_ids);

        std::size_t batch_count = 0;
        CHECK_POD5_OK(pod5_get_read_batch_count(&batch_count, file));
        REQUIRE(batch_count == 1);

        Pod5ReadRecordBatch * batch_0 = nullptr;
        CHECK_POD5_OK(pod5_get_read_batch(&batch_0, file, 0));
        REQUIRE(!!batch_0);

        // Check out of bounds accesses get errors
        {
            ReadBatchRowInfoV3 v3_struct;
            uint16_t input_version = 0;
            CHECK(
                pod5_get_read_batch_row_info_data(
                    batch_0, read_count, READ_BATCH_ROW_INFO_VERSION, &v3_struct, &input_version)
                == POD5_ERROR_INDEXERROR);

            std::vector<uint64_t> signal_row_indices{1};
            CHECK(
                pod5_get_signal_row_indices(
                    batch_0, read_count, signal_row_indices.size(), signal_row_indices.data())
                == POD5_ERROR_INDEXERROR);

            CalibrationExtraData calibration_extra_data{};
            CHECK(
                pod5_get_calibration_extra_info(batch_0, read_count, &calibration_extra_data)
                == POD5_ERROR_INDEXERROR);
        }

        for (std::size_t row = 0; row < read_count; ++row) {
            auto signal = signal_1;
            if (row == 1) {
                signal = signal_2;
            }

            static_assert(
                std::is_same<ReadBatchRowInfoV3, ReadBatchRowInfo_t>::value,
                "Update this if new structs added");

            ReadBatchRowInfoV3 v3_struct;
            uint16_t input_version = 0;
            CHECK_POD5_OK(pod5_get_read_batch_row_info_data(
                batch_0, row, READ_BATCH_ROW_INFO_VERSION, &v3_struct, &input_version));
            CHECK(input_version == 3);

            std::string formatted_uuid(36, '\0');
            CHECK_POD5_OK(pod5_format_read_id(v3_struct.read_id, &formatted_uuid[0]));
            CHECK(
                formatted_uuid.size()
                == boost::uuids::to_string(*(boost::uuids::uuid *)v3_struct.read_id).size());
            CHECK(
                formatted_uuid
                == boost::uuids::to_string(*(boost::uuids::uuid *)v3_struct.read_id));

            CHECK(v3_struct.read_number == 12);
            CHECK(v3_struct.start_sample == 10245);
            CHECK(v3_struct.median_before == 200.0f);
            CHECK(v3_struct.channel == 43);
            CHECK(v3_struct.well == 4);
            CHECK(v3_struct.pore_type == 0);
            CHECK(v3_struct.calibration_offset == calibration_offset);
            CHECK(v3_struct.calibration_scale == calibration_scale);
            CHECK(v3_struct.end_reason == 1);
            CHECK(v3_struct.end_reason_forced == false);
            CHECK(v3_struct.run_info == 0);
            CHECK(v3_struct.num_minknow_events == num_minknow_events);
            CHECK(v3_struct.tracked_scaling_scale == tracked_scale);
            CHECK(v3_struct.tracked_scaling_shift == tracked_shift);
            CHECK(v3_struct.predicted_scaling_scale == predicted_scale);
            CHECK(v3_struct.predicted_scaling_shift == predicted_shift);
            CHECK(v3_struct.num_reads_since_mux_change == num_reads_since_mux_change);
            CHECK(v3_struct.time_since_mux_change == time_since_mux_change);
            CHECK(v3_struct.signal_row_count == 1);
            CHECK(v3_struct.num_samples == signal.size());

            std::vector<uint64_t> signal_row_indices(v3_struct.signal_row_count);
            CHECK_POD5_OK(pod5_get_signal_row_indices(
                batch_0, row, signal_row_indices.size(), signal_row_indices.data()));

            std::vector<SignalRowInfo *> signal_row_info(v3_struct.signal_row_count);
            CHECK_POD5_OK(pod5_get_signal_row_info(
                file,
                signal_row_indices.size(),
                signal_row_indices.data(),
                signal_row_info.data()));

            std::vector<int16_t> read_signal(signal_row_info.front()->stored_sample_count);
            REQUIRE(signal_row_info.front()->stored_sample_count == signal.size());
            CHECK_POD5_OK(pod5_get_signal(
                file,
                signal_row_info.front(),
                signal_row_info.front()->stored_sample_count,
                read_signal.data()));
            CHECK(read_signal == signal);

            std::size_t sample_count = 0;
            CHECK_POD5_OK(pod5_get_read_complete_sample_count(file, batch_0, row, &sample_count));
            CHECK(sample_count == signal_row_info.front()->stored_sample_count);
            CHECK_POD5_OK(pod5_get_read_complete_signal(
                file, batch_0, row, sample_count, read_signal.data()));
            CHECK(read_signal == signal);

            CHECK_POD5_OK(
                pod5_free_signal_row_info(signal_row_indices.size(), signal_row_info.data()));

            std::string expected_pore_type{"pore_type"};
            std::array<char, 128> char_buffer{};
            std::size_t returned_size = 2;  // deliberately too short!
            {
                CHECK(
                    pod5_get_pore_type(
                        batch_0, v3_struct.pore_type, char_buffer.data(), &returned_size)
                    == POD5_ERROR_STRING_NOT_LONG_ENOUGH);
                CHECK(returned_size == expected_pore_type.size() + 1);
            }
            {
                returned_size = char_buffer.size();
                CHECK_POD5_OK(pod5_get_pore_type(
                    batch_0, v3_struct.pore_type, char_buffer.data(), &returned_size));
                CHECK(returned_size == expected_pore_type.size() + 1);
                CHECK(std::string{char_buffer.data()} == expected_pore_type);
            }
            {
                returned_size = char_buffer.size();
                CHECK(
                    pod5_get_pore_type(batch_0, -1, char_buffer.data(), &returned_size)
                    == POD5_ERROR_INDEXERROR);
                CHECK(returned_size == char_buffer.size());
            }

            std::string expected_end_reason{"mux_change"};
            {
                returned_size = 2;  // deliberately too short!
                pod5_end_reason end_reason = POD5_END_REASON_UNKNOWN;
                CHECK(
                    pod5_get_end_reason(
                        batch_0,
                        v3_struct.end_reason,
                        &end_reason,
                        char_buffer.data(),
                        &returned_size)
                    == POD5_ERROR_STRING_NOT_LONG_ENOUGH);
                CHECK(returned_size == expected_end_reason.size() + 1);
            }
            {
                returned_size = char_buffer.size();
                pod5_end_reason end_reason = POD5_END_REASON_UNKNOWN;
                CHECK_POD5_OK(pod5_get_end_reason(
                    batch_0,
                    v3_struct.end_reason,
                    &end_reason,
                    char_buffer.data(),
                    &returned_size));
                CHECK(returned_size == expected_end_reason.size() + 1);
                CHECK(end_reason == POD5_END_REASON_MUX_CHANGE);
                CHECK(std::string{char_buffer.data()} == expected_end_reason);
            }
            // Check getting with an invalid input end reason index:
            {
                returned_size = char_buffer.size();
                pod5_end_reason end_reason = POD5_END_REASON_UNKNOWN;
                CHECK(
                    pod5_get_end_reason(
                        batch_0,
                        v3_struct.end_reason + 100,
                        &end_reason,
                        char_buffer.data(),
                        &returned_size)
                    == POD5_ERROR_INDEXERROR);
                CHECK(returned_size == char_buffer.size());
                CHECK(end_reason == POD5_END_REASON_UNKNOWN);
            }

            CalibrationExtraData calibration_extra_data{};
            CHECK_POD5_OK(pod5_get_calibration_extra_info(batch_0, row, &calibration_extra_data));
            CHECK(calibration_extra_data.digitisation == adc_max - adc_min + 1);
            CHECK(calibration_extra_data.range == 8192 * calibration_scale);
        }

        run_info_index_t run_info_count = 0;
        CHECK_POD5_OK(pod5_get_file_run_info_count(file, &run_info_count));
        REQUIRE(run_info_count == 1);

        // Check getting invalid run info indexes fails correctly.
        RunInfoDictData * run_info_error = nullptr;
        CHECK(pod5_get_run_info(batch_0, -1, &run_info_error) == POD5_ERROR_INDEXERROR);
        CHECK(!run_info_error);
        CHECK(pod5_get_run_info(batch_0, run_info_count, &run_info_error) == POD5_ERROR_INDEXERROR);
        CHECK(!run_info_error);
        CHECK(pod5_get_file_run_info(file, -1, &run_info_error) == POD5_ERROR_INDEXERROR);
        CHECK(!run_info_error);
        CHECK(
            pod5_get_file_run_info(file, run_info_count, &run_info_error) == POD5_ERROR_INDEXERROR);
        CHECK(!run_info_error);

        auto check_run_info = [](RunInfoDictData * run_info) {
            REQUIRE(!!run_info);
            CHECK(run_info->tracking_id.size == 2);
            CHECK(run_info->tracking_id.keys[0] == std::string("baz"));
            CHECK(run_info->tracking_id.keys[1] == std::string("other"));
            CHECK(run_info->tracking_id.values[0] == std::string("baz_val"));
            CHECK(run_info->tracking_id.values[1] == std::string("other_val"));
            CHECK(run_info->context_tags.size == 2);
            CHECK(run_info->context_tags.keys[0] == std::string("thing"));
            CHECK(run_info->context_tags.keys[1] == std::string("foo"));
            CHECK(run_info->context_tags.values[0] == std::string("thing_val"));
            CHECK(run_info->context_tags.values[1] == std::string("foo_val"));
        };

        RunInfoDictData * run_info_data_out_1 = nullptr;
        CHECK_POD5_OK(pod5_get_file_run_info(file, 0, &run_info_data_out_1));
        check_run_info(run_info_data_out_1);
        pod5_free_run_info(run_info_data_out_1);

        RunInfoDictData * run_info_data_out_2 = nullptr;
        CHECK_POD5_OK(pod5_get_run_info(batch_0, 0, &run_info_data_out_2));
        check_run_info(run_info_data_out_2);
        pod5_free_run_info(run_info_data_out_2);

        pod5_free_read_batch(batch_0);

        pod5_close_and_free_reader(file);
        CHECK_POD5_OK(pod5_get_error_no());
    }
}

SCENARIO("C API Many Reads")
{
    static constexpr char const * filename = "./foo_c_api.pod5";

    pod5_init();
    auto fin = gsl::finally([] { pod5_terminate(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto input_read_id = uuid_gen();
    auto input_read_id_2 = uuid_gen();
    std::vector<int16_t> signal_1(10);
    std::iota(signal_1.begin(), signal_1.end(), -20000);

    std::vector<int16_t> signal_2(20);
    std::iota(signal_2.begin(), signal_2.end(), 0);

    std::size_t read_count = 10037;

    std::int16_t adc_min = -4096;
    std::int16_t adc_max = 4095;

    // Write the file:
    {
        CHECK_POD5_OK(pod5_get_error_no());
        CHECK(!pod5_create_file(NULL, "c_software", NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);
        CHECK(!pod5_create_file("", "c_software", NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);
        CHECK(!pod5_create_file("", NULL, NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);

        REQUIRE(remove_file_if_exists(filename).ok());

        auto file = pod5_create_file(filename, "c_software", NULL);
        REQUIRE(file);
        CHECK_POD5_OK(pod5_get_error_no());

        std::int16_t pore_type_id = -1;
        CHECK_POD5_OK(pod5_add_pore(&pore_type_id, file, "pore_type"));
        CHECK(pore_type_id == 0);

        std::vector<char const *> context_tags_keys{"thing", "foo"};
        std::vector<char const *> context_tags_values{"thing_val", "foo_val"};
        std::vector<char const *> tracking_id_keys{"baz", "other"};
        std::vector<char const *> tracking_id_values{"baz_val", "other_val"};

        std::int16_t run_info_id = -1;
        CHECK_POD5_OK(pod5_add_run_info(
            &run_info_id,
            file,
            "acquisition_id",
            15400,
            adc_max,
            adc_min,
            context_tags_keys.size(),
            context_tags_keys.data(),
            context_tags_values.data(),
            "experiment_name",
            "flow_cell_id",
            "flow_cell_product_code",
            "protocol_name",
            "protocol_run_id",
            200000,
            "sample_id",
            4000,
            "sequencing_kit",
            "sequencer_position",
            "sequencer_position_type",
            "software",
            "system_name",
            "system_type",
            tracking_id_keys.size(),
            tracking_id_keys.data(),
            tracking_id_values.data()));
        CHECK(run_info_id == 0);

        std::vector<std::uint32_t> read_number(read_count, 12);
        std::vector<std::uint64_t> start_sample(read_count, 10245);
        std::vector<float> median_before(read_count, 200.0f);
        std::vector<std::uint16_t> channel(read_count, 43);
        std::vector<std::uint8_t> well(read_count, 4);
        std::vector<pod5_end_reason_t> end_reason(read_count, POD5_END_REASON_MUX_CHANGE);
        std::vector<uint8_t> end_reason_forced(read_count, false);
        std::vector<boost::uuids::uuid> read_id_array(read_count, input_read_id);

        std::vector<float> calibration_offset(read_count, 54.0f);
        std::vector<float> calibration_scale(read_count, 100.0f);

        std::vector<float> predicted_scale(read_count, 2.3f);
        std::vector<float> predicted_shift(read_count, 10.0f);
        std::vector<float> tracked_scale(read_count, 4.3f);
        std::vector<float> tracked_shift(read_count, 15.0f);
        std::vector<std::uint32_t> num_reads_since_mux_change(read_count, 1234);
        std::vector<float> time_since_mux_change(read_count, 2.4f);
        std::vector<std::uint64_t> num_minknow_events(read_count, 104);

        std::vector<std::int16_t> pore_type_ids(read_count, pore_type_id);
        std::vector<std::int16_t> run_info_ids(read_count, run_info_id);

        std::vector<std::int16_t const *> signal_arr;
        std::vector<std::uint32_t> signal_size;
        ReadBatchRowInfoArrayV3 row_data{
            (read_id_t *)read_id_array.data(),
            read_number.data(),
            start_sample.data(),
            median_before.data(),
            channel.data(),
            well.data(),
            pore_type_ids.data(),
            calibration_offset.data(),
            calibration_scale.data(),
            end_reason.data(),
            end_reason_forced.data(),
            run_info_ids.data(),
            num_minknow_events.data(),
            tracked_scale.data(),
            tracked_shift.data(),
            predicted_scale.data(),
            predicted_shift.data(),
            num_reads_since_mux_change.data(),
            time_since_mux_change.data()};

        for (std::size_t i = 0; i < read_count; ++i) {
            signal_arr.push_back(signal_1.data());
            signal_size.push_back((std::uint32_t)signal_1.size());
        }

        CHECK_POD5_OK(pod5_add_reads_data(
            file,
            read_count,
            READ_BATCH_ROW_INFO_VERSION_3,
            &row_data,
            signal_arr.data(),
            signal_size.data()));

        CHECK_POD5_OK(pod5_close_and_free_writer(file));
        CHECK_POD5_OK(pod5_get_error_no());
    }

    // Read the file back:
    {
        Pod5ReaderOptions_t options{};
        options.force_disable_file_mapping = true;

        CHECK_POD5_OK(pod5_get_error_no());
        CHECK(!pod5_open_file_options(NULL, &options));
        CHECK(!pod5_open_file_options(filename, NULL));
        auto file = pod5_open_file_options(filename, &options);
        CHECK_POD5_OK(pod5_get_error_no());
        CHECK(!!file);

        FileInfo_t file_info;
        CHECK_POD5_OK(pod5_get_file_info(file, &file_info));
        CHECK(file_info.version.major == pod5::Pod5MajorVersion);
        CHECK(file_info.version.minor == pod5::Pod5MinorVersion);
        CHECK(file_info.version.revision == pod5::Pod5RevVersion);
        {
            auto reader = pod5::open_file_reader(filename);
            boost::uuids::uuid file_identifier;
            std::copy(
                file_info.file_identifier,
                file_info.file_identifier + sizeof(file_info.file_identifier),
                file_identifier.begin());
            CHECK(file_identifier == (*reader)->schema_metadata().file_identifier);
        }

        std::size_t read_count_returned = 0;
        CHECK_POD5_OK(pod5_get_read_count(file, &read_count_returned));
        REQUIRE(read_count_returned == read_count);

        pod5_close_and_free_reader(file);
        CHECK_POD5_OK(pod5_get_error_no());
    }
}

SCENARIO("C API Run Info")
{
    static constexpr char const * filename = "./foo_c_api.pod5";

    pod5_init();
    auto fin = gsl::finally([] { pod5_terminate(); });

    std::int16_t adc_min = -4096;
    std::int16_t adc_max = 4095;

    auto expected_acq_id = [](std::size_t index) {
        std::string acquisition_id{"acquisition_id_"};
        acquisition_id += std::to_string(index);
        return acquisition_id;
    };

    // Write the file:
    {
        REQUIRE(remove_file_if_exists(filename).ok());

        auto file = pod5_create_file(filename, "c_software", NULL);
        REQUIRE(file);
        CHECK_POD5_OK(pod5_get_error_no());

        std::vector<char const *> context_tags_keys{"thing", "foo"};
        std::vector<char const *> context_tags_values{"thing_val", "foo_val"};
        std::vector<char const *> tracking_id_keys{"baz", "other"};
        std::vector<char const *> tracking_id_values{"baz_val", "other_val"};

        for (std::size_t i = 0; i < 10; ++i) {
            std::int16_t run_info_id = -1;
            CHECK_POD5_OK(pod5_add_run_info(
                &run_info_id,
                file,
                expected_acq_id(i).c_str(),
                15400,
                adc_max,
                adc_min,
                context_tags_keys.size(),
                context_tags_keys.data(),
                context_tags_values.data(),
                "experiment_name",
                "flow_cell_id",
                "flow_cell_product_code",
                "protocol_name",
                "protocol_run_id",
                200000,
                "sample_id",
                4000,
                "sequencing_kit",
                "sequencer_position",
                "sequencer_position_type",
                "software",
                "system_name",
                "system_type",
                tracking_id_keys.size(),
                tracking_id_keys.data(),
                tracking_id_values.data()));
            CHECK(run_info_id == i);
        }
        CHECK_POD5_OK(pod5_close_and_free_writer(file));
    }

    // Read the file back:
    {
        CHECK_POD5_OK(pod5_get_error_no());
        CHECK(!pod5_open_file(NULL));
        auto file = pod5_open_file(filename);
        CHECK_POD5_OK(pod5_get_error_no());
        CHECK(pod5_get_error_string() == std::string{""});
        CHECK(!!file);

        run_info_index_t run_info_count = 0;
        CHECK_POD5_OK(pod5_get_file_run_info_count(file, &run_info_count));
        REQUIRE(run_info_count == 10);

        for (run_info_index_t i = 0; i < 10; ++i) {
            RunInfoDictData * run_info_data_out = nullptr;
            CHECK_POD5_OK(pod5_get_file_run_info(file, i, &run_info_data_out));
            CHECK(run_info_data_out->acquisition_id == expected_acq_id(i));
            pod5_free_run_info(run_info_data_out);
        }

        CHECK_POD5_OK(pod5_close_and_free_reader(file));
    }
}
