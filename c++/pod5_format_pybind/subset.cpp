#include "subset.h"

#include "pod5_format/file_reader.h"
#include "pod5_format/file_writer.h"
#include "repack/repacker.h"

#include <iostream>

/// \brief Simple progress bar for console applications
class ProgressBar {
public:
    static constexpr int PB_WIDTH = 60;

    ProgressBar() {}

    ~ProgressBar() { std::fputs("\n", stdout); }

    void set_task(std::string const & task_name)
    {
        m_task = task_name;
        print_progress();
    }

    void update_max_steps(std::size_t max_steps) { this->m_max_steps = max_steps; }

    void update(std::size_t current_step)
    {
        if (current_step == m_current_step) {
            return;
        }
        m_current_step = current_step;
        print_progress();
    }

    void print_progress()
    {
        float complete_ratio = static_cast<float>(m_current_step) / static_cast<float>(m_max_steps);
        int complete_length = static_cast<int>(complete_ratio * PB_WIDTH);
        std::string complete_string{"\r["};
        for (int i = 0; i < PB_WIDTH; ++i) {
            if (i < complete_length) {
                complete_string += "=";
            } else {
                complete_string += " ";
            }
        }
        complete_string += "] (" + std::to_string(m_current_step) + "/"
                           + std::to_string(m_max_steps) + ") " + m_task;
        m_max_printed_width = std::max<std::size_t>(m_max_printed_width, complete_string.size());
        // Pad to max width to overwrite previous longer lines
        complete_string.resize(m_max_printed_width, ' ');
        std::cout << complete_string.c_str() << std::flush;
    }

private:
    std::string m_task;
    std::size_t m_max_steps{0};
    std::size_t m_current_step{0};
    std::size_t m_max_printed_width{0};
};

void subset_pod5s_with_mapping(
    std::vector<std::filesystem::path> inputs,
    std::filesystem::path output,
    std::map<std::string, std::vector<std::string>> read_id_to_dest,
    bool missing_ok,
    bool duplicate_ok,
    bool force_overwrite)
{
    ProgressBar progress_bar;
    progress_bar.set_task("Computing subset reads...");
    auto repacker = std::make_shared<repack::Pod5Repacker>();

    struct OutputInfo {
        OutputInfo(std::shared_ptr<repack::Pod5RepackerOutput> && repacker_output_)
        : repacker_output(std::move(repacker_output_))
        {
        }

        std::shared_ptr<repack::Pod5RepackerOutput> repacker_output;

        void clear_per_input_working_data()
        {
            batch_counts.clear();
            all_batch_rows.clear();
            batch_counts.reserve(32);
            all_batch_rows.reserve(128);
        }

        void add_row(std::uint32_t row_index)
        {
            all_batch_rows.push_back(row_index);
            next_batch_size += 1;
        }

        void finish_batch()
        {
            batch_counts.push_back(next_batch_size);
            next_batch_size = 0;
        }

        // Per file working vectors:
        std::uint32_t next_batch_size = 0;
        std::vector<std::uint32_t> batch_counts;
        std::vector<std::uint32_t> all_batch_rows;
    };

    static constexpr std::size_t max_open_input_files = 128;
    std::vector<OutputInfo> dest_to_output;
    std::unordered_multimap<pod5::Uuid, std::uint32_t> read_id_lookup;
    pod5::FileWriterOptions output_options{};
    output_options.set_keep_signal_file_open(false);
    output_options.set_keep_read_table_file_open(false);
    output_options.set_keep_run_info_file_open(false);
    pod5::FileReaderOptions input_options{};
    input_options.set_force_disable_file_mapping(true);

    std::vector<std::filesystem::path> created_output_files;
    auto cleanup = gsl::finally([&]() {
        for (auto const & path : created_output_files) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    });

    // Prepare the read id input list to output mapping:
    for (auto const & read_id_dest : read_id_to_dest) {
        auto const output_path = output / read_id_dest.first;
        if (std::filesystem::exists(output_path)) {
            if (!force_overwrite) {
                throw std::runtime_error(
                    "Output files already exists and --force-overwrite not set. ");
            } else {
                std::filesystem::remove(output_path);
            }
        }

        auto writer = pod5::create_file_writer(output_path.string(), "pod5_subset", output_options);
        if (!writer.ok()) {
            std::cerr << "Failed to create output file: " << output_path << std::endl;
            throw std::runtime_error("Failed to create output POD5 file");
        }

        created_output_files.push_back(output_path);
        auto output_repacker_file = repacker->add_output(std::move(*writer), !duplicate_ok);
        std::size_t output_index = dest_to_output.size();
        dest_to_output.emplace_back(std::move(output_repacker_file));

        for (auto const & read_id : read_id_dest.second) {
            auto read_id_uuid = pod5::Uuid::from_string(read_id);
            if (!read_id_uuid) {
                std::cerr << "Invalid read id uuid: " << read_id << std::endl;
                throw std::runtime_error("Invalid read id uuid in mapping");
            }
            read_id_lookup.insert(std::make_pair(*read_id_uuid, output_index));
        }
    }

    progress_bar.update_max_steps(read_id_lookup.size());

    // Walk each input file:
    std::size_t found_read_count = 0;
    for (auto const & input_path : inputs) {
        // Clear per-input working data in the output files:
        for (auto & output_file : dest_to_output) {
            output_file.clear_per_input_working_data();
        }

        // Limit the number of open input files:
        auto start_time = std::chrono::steady_clock::now();
        while (repacker->currently_open_file_reader_count() > max_open_input_files) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            if (std::chrono::steady_clock::now() - start_time > std::chrono::seconds(1)) {
                progress_bar.set_task("Waiting for queued writes to complete...");
            }
        }
        progress_bar.set_task("Subsetting " + input_path.filename().string());

        auto input_reader_opt = pod5::open_file_reader(input_path.string(), input_options);
        if (!input_reader_opt.ok()) {
            std::cerr << "Failed to open input file: " << input_path << std::endl;
            throw std::runtime_error("Failed to open input POD5 file");
        }
        auto const & input_reader = *input_reader_opt;

        // Walk the input file batches:
        for (std::size_t i = 0; i < input_reader->num_read_record_batches(); ++i) {
            auto batch = input_reader->read_read_record_batch(i);
            if (!batch.ok()) {
                std::cerr << "Failed to read batch " << i << " from input file: " << input_path
                          << std::endl;
                throw std::runtime_error("Failed to read read record batch from POD5 file");
            }

            // Test each read id in the batch to see if we want it:
            auto const & read_id_column = batch->read_id_column();
            for (std::int64_t row = 0; row < read_id_column->length(); ++row) {
                auto const found = read_id_lookup.equal_range(read_id_column->Value(row));
                for (auto it = found.first; it != found.second; ++it) {
                    dest_to_output[it->second].add_row(row);
                    found_read_count += 1;
                }
            }

            // Now store how many rows in this batch we selected:
            for (auto & output_file : dest_to_output) {
                output_file.finish_batch();
            }

            progress_bar.update(repacker->reads_completed());
        }

        // Now submit the selected reads to each output:
        for (auto & output : dest_to_output) {
            repacker->add_selected_reads_to_output(
                output.repacker_output,
                input_reader,
                gsl::make_span(output.batch_counts),
                gsl::make_span(output.all_batch_rows));
        }
    }

    if (found_read_count < read_id_lookup.size() && !missing_ok) {
        throw std::runtime_error("Missing read_ids from inputs but --missing-ok not set");
    }

    // Set each output to finished:
    std::thread finisher([&] {
        // Mark all outputs as finished, we won't write any new reads:
        for (auto & output : dest_to_output) {
            repacker->set_output_finished(output.repacker_output);
        }
    });

    // Wait for repacker to complete:
    progress_bar.set_task("Waiting for IO to complete...");
    try {
        while (!repacker->is_complete()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            progress_bar.update(repacker->reads_completed());
        }
    } catch (std::exception const & e) {
        std::cout << "\nError during repacking: " << e.what() << std::endl;
    }

    finisher.join();

    repacker->finish();

    // Clear created output files from cleanup since we succeeded:
    created_output_files.clear();
}
