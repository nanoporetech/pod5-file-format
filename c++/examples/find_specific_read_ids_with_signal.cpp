#include "pod5_format/c_api.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <fstream>
#include <iostream>
#include <vector>

int main(int argc, char ** argv)
{
    if (argc != 3) {
        std::cerr << "Expected two arguments:\n"
                  << " - an pod5 file to search\n"
                  << " - a file containing newline separated of read ids\n";
        return EXIT_FAILURE;
    }

    // Initialise the POD5 library:
    pod5_init();

    // Open the file ready for walking:
    Pod5FileReader_t * file = pod5_open_file(argv[1]);
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
    std::string input_path(argv[2]);
    try {
        std::cout << "Reading input read ids from " << input_path << "\n";
        std::string line;
        std::ifstream input_stream(input_path);
        while (std::getline(input_stream, line)) {
            search_uuids.push_back(boost::lexical_cast<boost::uuids::uuid>(line));
        }
        std::cout << "  Read " << search_uuids.size() << " ids from the text file\n";
    } catch (std::exception const & e) {
        std::cerr << "Failed to parse UUID values from " << input_path << ": " << e.what() << "\n";
    }

    std::string output_path("read_ids.txt");
    std::cout << "Writing selected read numbers to " << output_path << "\n";
    std::ofstream output_stream(output_path);

    // Plan the most efficient route through the file for the required read ids:
    std::vector<std::uint32_t> traversal_batch_counts(batch_count);
    std::vector<std::uint32_t> traversal_row_indices(search_uuids.size());
    std::size_t find_success_count = 0;
    if (pod5_plan_traversal(
            file,
            (uint8_t *)search_uuids.data(),
            search_uuids.size(),
            traversal_batch_counts.data(),
            traversal_row_indices.data(),
            &find_success_count)
        != POD5_OK)
    {
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
        Pod5ReadRecordBatch_t * batch = nullptr;
        if (pod5_get_read_batch(&batch, file, batch_index) != POD5_OK) {
            std::cerr << "Failed to get batch: " << pod5_get_error_string() << "\n";
            return EXIT_FAILURE;
        }

        std::cout << "Processing batch " << (batch_index + 1) << " of " << batch_count << "\n";
        for (std::size_t row_index = 0; row_index < traversal_batch_counts[batch_index];
             ++row_index) {
            std::uint32_t batch_row = traversal_row_indices[row_index + row_offset];

            uint16_t read_table_version = 0;
            ReadBatchRowInfo_t read_data;
            if (pod5_get_read_batch_row_info_data(
                    batch, batch_row, READ_BATCH_ROW_INFO_VERSION, &read_data, &read_table_version)
                != POD5_OK)
            {
                std::cerr << "Failed to get read " << batch_row << "\n";
                return EXIT_FAILURE;
            }

            std::size_t sample_count = 0;
            pod5_get_read_complete_sample_count(file, batch, batch_row, &sample_count);

            std::vector<std::int16_t> samples;
            samples.resize(sample_count);
            pod5_get_read_complete_signal(file, batch, batch_row, samples.size(), samples.data());

            std::int64_t samples_sum = 0;
            for (std::size_t i = 0; i < samples.size(); ++i) {
                samples_sum += samples[i];
            }

            output_stream << read_data.calibration_offset << " " << read_data.calibration_scale
                          << " " << samples_sum << "\n";
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

    // Close the reader
    if (pod5_close_and_free_reader(file) != POD5_OK) {
        std::cerr << "Failed to close reader: " << pod5_get_error_string() << "\n";
        return EXIT_FAILURE;
    }

    // Cleanup the library
    pod5_terminate();
}
