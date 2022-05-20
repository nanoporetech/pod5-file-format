#include "pod5_format/c_api.h"

#include <boost/filesystem/fstream.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <iostream>

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Expected two arguments:\n"
                  << " - an pod5 file to search\n"
                  << " - a file containing newline separated of read ids\n";
    }

    // Initialise the POD5 library:
    pod5_init();

    // Open the file ready for walking:
    Pod5FileReader_t* file = pod5_open_combined_file(argv[1]);
    if (!file) {
        std::cerr << "Failed to open file " << argv[1] << ": " << pod5_get_error_string() << "\n";
        return EXIT_FAILURE;
    }

    std::size_t batch_count = 0;
    if (pod5_get_read_batch_count(&batch_count, file) != POD5_OK) {
        std::cerr << "Failed to query batch count: " << pod5_get_error_string() << "\n";
        return EXIT_FAILURE;
    }

    std::vector<boost::uuids::uuid> search_uuids;
    boost::filesystem::path input_path(argv[2]);
    try {
        std::cout << "Reading input read ids from " << input_path << "\n";
        std::string line;
        boost::filesystem::ifstream input_stream(input_path);
        while (std::getline(input_stream, line)) {
            search_uuids.push_back(boost::lexical_cast<boost::uuids::uuid>(line));
        }
        std::cout << "  Read " << search_uuids.size() << " ids from the text file\n";
    } catch (std::exception const& e) {
        std::cerr << "Failed to parse UUID values from " << input_path << ": " << e.what() << "\n";
    }

    boost::filesystem::path output_path("read_numbers.txt");
    std::cout << "Writing selected read numbers to " << output_path << "\n";
    boost::filesystem::ofstream output_stream(output_path);

    // Plan the most efficient route through the file for the required read ids:
    std::vector<std::uint32_t> traversal_batch_counts(batch_count);
    std::vector<std::uint32_t> traversal_row_indices(search_uuids.size());
    std::size_t find_success_count = 0;
    if (pod5_plan_traversal(file, (uint8_t*)search_uuids.data(), search_uuids.size(),
                            traversal_batch_counts.data(), traversal_row_indices.data(),
                            &find_success_count) != POD5_OK) {
        std::cerr << "Failed to plan traversal of file: " << pod5_get_error_string() << "\n";
        return EXIT_FAILURE;
    }

    if (find_success_count != search_uuids.size()) {
        std::cerr << "Failed to find " << (search_uuids.size() - find_success_count) << " reads\n";
    }

    std::size_t read_count = 0;
    std::size_t samples_read = 0;

    std::size_t row_offset = 0;

    // Walk the suggested traversal route, storing read data.
    std::size_t step_index = 0;
    for (std::size_t batch_index = 0; batch_index < batch_count; ++batch_index) {
        Pod5ReadRecordBatch_t* batch = nullptr;
        if (pod5_get_read_batch(&batch, file, batch_index) != POD5_OK) {
            std::cerr << "Failed to get batch: " << pod5_get_error_string() << "\n";
            return EXIT_FAILURE;
        }

        std::cout << "Processing batch " << (batch_index + 1) << " of " << batch_count << "\n";
        for (std::size_t row_index = 0; row_index < traversal_batch_counts[batch_index];
             ++row_index) {
            std::uint32_t batch_row = traversal_row_indices[row_index + row_offset];
            // Read out the per read details:
            boost::uuids::uuid read_id;
            int16_t pore = 0;
            int16_t calibration = 0;
            uint32_t read_number = 0;
            uint64_t start_sample = 0;
            float median_before = 0.0f;
            int16_t end_reason = 0;
            int16_t run_info = 0;
            int64_t signal_row_count = 0;
            if (pod5_get_read_batch_row_info(batch, batch_row, read_id.begin(), &pore, &calibration,
                                             &read_number, &start_sample, &median_before,
                                             &end_reason, &run_info,
                                             &signal_row_count) != POD5_OK) {
                std::cerr << "Failed to get read " << batch_row << ": " << pod5_get_error_string()
                          << "\n";
                return EXIT_FAILURE;
            }

            // Now read out the calibration params:
            CalibrationDictData_t* calib_data = nullptr;
            if (pod5_get_calibration(batch, calibration, &calib_data) != POD5_OK) {
                std::cerr << "Failed to get read " << batch_row
                          << " calibration data: " << pod5_get_error_string() << "\n";
                return EXIT_FAILURE;
            }

            // Find the absolute indices of the signal rows in the signal table
            std::vector<std::uint64_t> signal_rows_indices(signal_row_count);
            if (pod5_get_signal_row_indices(batch, batch_row, signal_row_count,
                                            signal_rows_indices.data()) != POD5_OK) {
                std::cerr << "Failed to get read " << batch_row
                          << " signal row indices: " << pod5_get_error_string() << "\n";
                return EXIT_FAILURE;
            }

            // Find the locations of each row in signal batches:
            std::vector<SignalRowInfo_t*> signal_rows(signal_row_count);
            if (pod5_get_signal_row_info(file, signal_row_count, signal_rows_indices.data(),
                                         signal_rows.data()) != POD5_OK) {
                std::cerr << "Failed to get read " << batch_row
                          << " signal row locations: " << pod5_get_error_string() << "\n";
            }

            std::size_t total_sample_count = 0;
            for (std::size_t i = 0; i < signal_row_count; ++i) {
                total_sample_count += signal_rows[i]->stored_sample_count;
            }

            std::vector<std::int16_t> samples(total_sample_count);
            std::size_t samples_read_so_far = 0;
            for (std::size_t i = 0; i < signal_row_count; ++i) {
                if (pod5_get_signal(file, signal_rows[i], signal_rows[i]->stored_sample_count,
                                    samples.data() + samples_read_so_far) != POD5_OK) {
                    std::cerr << "Failed to get read " << batch_row
                              << " signal: " << pod5_get_error_string() << "\n";
                }

                samples_read_so_far += signal_rows[i]->stored_sample_count;
            }

            std::int64_t samples_sum = 0;
            for (std::size_t i = 0; i < samples.size(); ++i) {
                samples_sum += samples[i];
            }

            pod5_release_calibration(calib_data);
            pod5_free_signal_row_info(signal_row_count, signal_rows.data());

            output_stream << calib_data->offset << " " << calib_data->scale << " " << samples_sum
                          << "\n";
            read_count += 1;
            samples_read += samples.size();
        }
        row_offset += traversal_batch_counts[batch_index];

        if (pod5_free_read_batch(batch) != POD5_OK) {
            std::cerr << "Failed to release batch\n";
            return EXIT_FAILURE;
        }
    }

    std::cout << "Extracted " << read_count << " reads and " << samples_read << " samples into "
              << output_path << "\n";
}
