#include "pod5_format/c_api.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <array>
#include <fstream>
#include <iostream>
#include <vector>

int main(int argc, char ** argv)
{
    if (argc != 2) {
        std::cerr << "Expected one argument - an pod5 file to search\n";
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

    std::string output_path("read_ids.txt");
    std::cout << "Writing read ids to " << output_path << "\n";
    std::ofstream output_stream(output_path);

    std::size_t read_count = 0;

    for (std::size_t batch_index = 0; batch_index < batch_count; ++batch_index) {
        Pod5ReadRecordBatch_t * batch = nullptr;
        if (pod5_get_read_batch(&batch, file, batch_index) != POD5_OK) {
            std::cerr << "Failed to get batch: " << pod5_get_error_string() << "\n";
            return EXIT_FAILURE;
        }

        std::size_t batch_row_count = 0;
        if (pod5_get_read_batch_row_count(&batch_row_count, batch) != POD5_OK) {
            std::cerr << "Failed to get batch row count\n";
            return EXIT_FAILURE;
        }

        for (std::size_t row = 0; row < batch_row_count; ++row) {
            uint16_t read_table_version = 0;
            ReadBatchRowInfo_t read_data;
            if (pod5_get_read_batch_row_info_data(
                    batch, row, READ_BATCH_ROW_INFO_VERSION, &read_data, &read_table_version)
                != POD5_OK)
            {
                std::cerr << "Failed to get read " << row << "\n";
                return EXIT_FAILURE;
            }

            std::array<char, 37> formatted_read_id;
            pod5_format_read_id(read_data.read_id, formatted_read_id.data());
            output_stream << formatted_read_id.data() << "\n";
            read_count += 1;

            std::size_t sample_count = 0;
            pod5_get_read_complete_sample_count(file, batch, row, &sample_count);

            std::vector<std::int16_t> samples;
            samples.resize(sample_count);
            pod5_get_read_complete_signal(file, batch, row, samples.size(), samples.data());
        }

        if (pod5_free_read_batch(batch) != POD5_OK) {
            std::cerr << "Failed to release batch\n";
            return EXIT_FAILURE;
        }
    }

    std::cout << "Extracted " << read_count << " read ids into " << output_path << "\n";

    // Close the reader
    if (pod5_close_and_free_reader(file) != POD5_OK) {
        std::cerr << "Failed to close reader: " << pod5_get_error_string() << "\n";
        return EXIT_FAILURE;
    }

    // Cleanup the library
    pod5_terminate();
}
