#include "mkr_format/c_api.h"

#include <boost/filesystem/fstream.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <iostream>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Expected one argument - an mkr file to search\n";
    }

    // Initialise the MKR library:
    mkr_init();

    // Open the file ready for walking:
    MkrFileReader_t* file = mkr_open_combined_file(argv[1]);
    if (!file) {
        std::cerr << "Failed to open file " << argv[1] << ": " << mkr_get_error_string() << "\n";
        return EXIT_FAILURE;
    }

    std::size_t batch_count = 0;
    if (mkr_get_read_batch_count(&batch_count, file) != MKR_OK) {
        std::cerr << "Failed to query batch count: " << mkr_get_error_string() << "\n";
        return EXIT_FAILURE;
    }

    boost::filesystem::path output_path("read_ids.txt");
    std::cout << "Writing read ids to " << output_path << "\n";
    boost::filesystem::ofstream output_stream(output_path);

    std::size_t read_count = 0;

    for (std::size_t batch_index = 0; batch_index < batch_count; ++batch_index) {
        MkrReadRecordBatch_t* batch = nullptr;
        if (mkr_get_read_batch(&batch, file, batch_index) != MKR_OK) {
            std::cerr << "Failed to get batch: " << mkr_get_error_string() << "\n";
            return EXIT_FAILURE;
        }

        std::size_t batch_row_count = 0;
        if (mkr_get_read_batch_row_count(&batch_row_count, batch) != MKR_OK) {
            std::cerr << "Failed to get batch row count\n";
            return EXIT_FAILURE;
        }

        for (std::size_t row = 0; row < batch_row_count; ++row) {
            boost::uuids::uuid read_id;
            int16_t pore = 0;
            int16_t calibration = 0;
            uint32_t read_number = 0;
            uint64_t start_sample = 0;
            float median_before = 0.0f;
            int16_t end_reason = 0;
            int16_t run_info = 0;
            int64_t signal_row_count = 0;
            if (mkr_get_read_batch_row_info(batch, row, read_id.begin(), &pore, &calibration,
                                            &read_number, &start_sample, &median_before,
                                            &end_reason, &run_info, &signal_row_count) != MKR_OK) {
                std::cerr << "Failed to get read " << row << "\n";
                return EXIT_FAILURE;
            }
            output_stream << boost::uuids::to_string(read_id) << "\n";
            read_count += 1;
        }

        if (mkr_free_read_batch(batch) != MKR_OK) {
            std::cerr << "Failed to release batch\n";
            return EXIT_FAILURE;
        }
    }

    std::cout << "Extracted " << read_count << " read ids into " << output_path << "\n";
}