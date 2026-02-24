#include "subset.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <limits>
#include <ostream>
#include <string>

#ifndef _WIN32
#include <sys/resource.h>
#include <unistd.h>
#else
#include <io.h>

#include <cstdio>  // _getmaxstdio
#endif

#include "pod5_format/file_reader.h"
#include "pod5_format/file_writer.h"
#include "pod5_format/schema_metadata.h"
#include "repack/repacker.h"

#include <iostream>

namespace io_limits {

// Balance the number of open inputs by the output-side handle usage.
// Prefer outputs over inputs to reduce the number of output
// batches which iterate over all inputs.
constexpr std::float_t kOutputsBias = 0.7f;
constexpr std::size_t kMinHandles = 1;
constexpr std::size_t kBaseReserve = 16;

std::size_t clamp_open_inputs(std::size_t soft_limit, std::size_t output_files)
{
    constexpr std::size_t kMaxInHandles = 256;
    std::size_t const reserve = kBaseReserve + output_files;
    if (soft_limit <= reserve + kMinHandles) {
        return kMinHandles;
    }
    return std::clamp(soft_limit - reserve, kMinHandles, kMaxInHandles);
}

std::size_t clamp_open_outputs(std::size_t soft_limit)
{
    constexpr std::size_t kMaxOutHandles = 4096;
    std::size_t const reserve = kBaseReserve + kMinHandles;
    if (soft_limit <= reserve + kMinHandles) {
        return kMinHandles;
    }
    std::size_t soft_upper = (std::size_t)(soft_limit * kOutputsBias);
    if (soft_upper > 32) {
        soft_upper = (soft_upper / 16) * 16;
    }

    return std::clamp(std::min(soft_limit - reserve, soft_upper), kMinHandles, kMaxOutHandles);
}

std::size_t detect_soft_limit()
{
    //
    constexpr std::size_t kSoftLimitFallback = 1024;

#ifndef _WIN32
    // Attempt to get the resource limits (if any)
    struct rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY) {
        return static_cast<std::size_t>(rl.rlim_cur);
    }
    long sc = sysconf(_SC_OPEN_MAX);
    return sc > 0 ? static_cast<std::size_t>(sc) : kSoftLimitFallback;
#else
    // Only stdio stream limit, not a true OS handle limit.
    int n = _getmaxstdio();
    return n > 0 ? static_cast<std::size_t>(n) : kSoftLimitFallback;
#endif
}
}  // namespace io_limits

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
    auto next_interrupt_check = std::chrono::steady_clock::now();
    auto poll_python_interrupt = [&]() {
        auto const now = std::chrono::steady_clock::now();
        if (now < next_interrupt_check) {
            return;
        }
        next_interrupt_check = now + std::chrono::milliseconds(500);

        pybind11::gil_scoped_acquire gil;
        if (PyErr_CheckSignals() != 0) {
            throw pybind11::error_already_set();
        }
    };

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

    pod5::FileWriterOptions output_options{};
    output_options.set_keep_signal_file_open(false);
    output_options.set_keep_read_table_file_open(false);
    output_options.set_keep_run_info_file_open(false);
    pod5::FileReaderOptions input_options{};
    input_options.set_force_disable_file_mapping(true);

    // Process inputs in deterministic lexical path order.
    std::sort(inputs.begin(), inputs.end());

    std::vector<std::filesystem::path> created_output_files;
    auto cleanup = gsl::finally([&]() {
        for (auto const & path : created_output_files) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    });

    bool issued_migration_warning = false;
    std::size_t const io_soft_limit = io_limits::detect_soft_limit();
    std::size_t const max_out_size = io_limits::clamp_open_outputs(io_soft_limit);

    // Create indexable view of the map iterators so we can conveniently index in batches.
    std::vector<std::map<std::string, std::vector<std::string>>::const_iterator> read_id_dest_iters;
    read_id_dest_iters.reserve(read_id_to_dest.size());
    std::size_t total_requested_read_ids = 0;
    for (auto it = read_id_to_dest.begin(); it != read_id_to_dest.end(); ++it) {
        poll_python_interrupt();

        // Check we're not unintentionally overwriting files
        auto const output_path = output / it->first;
        if (std::filesystem::exists(output_path)) {
            if (!force_overwrite) {
                throw std::runtime_error(
                    "Output files already exists and --force-overwrite not set. ");
            } else {
                std::filesystem::remove(output_path);
            }
        }

        // Index the map iterator and tally total reads
        read_id_dest_iters.push_back(it);
        total_requested_read_ids += it->second.size();
    }

    std::size_t found_read_count = 0;
    std::size_t total_reads_completed = 0;

    std::size_t const total_output_batches =
        (read_id_dest_iters.size() + max_out_size - 1) / max_out_size;

    if (total_output_batches > 1) {
        std::cerr << "Subsetting inputs into " << std::to_string(read_id_dest_iters.size())
                  << " files in " << std::to_string(total_output_batches) << " batches of at most "
                  << max_out_size << " outputs. IO limit: " << std::to_string(io_soft_limit)
                  << std::endl;
    }

    ProgressBar progress_bar;
    progress_bar.set_task("Starting...");
    progress_bar.update_max_steps(total_requested_read_ids);

    // Iterate over outputs in batches
    for (std::size_t out_st = 0; out_st < read_id_dest_iters.size(); out_st += max_out_size) {
        poll_python_interrupt();
        std::size_t const output_batch_index = (out_st / max_out_size) + 1;
        std::size_t const out_end = std::min(out_st + max_out_size, read_id_dest_iters.size());
        std::string const batch_prefix = "Batch [" + std::to_string(output_batch_index) + "/"
                                         + std::to_string(total_output_batches) + "]: ";

        auto repacker = std::make_shared<repack::Pod5Repacker>();
        std::unordered_multimap<pod5::Uuid, std::uint32_t> read_id_lookup;
        std::vector<OutputInfo> dest_to_output;
        dest_to_output.reserve(out_end - out_st);

        // For each output in this batch
        for (std::size_t out_idx = out_st; out_idx < out_end; ++out_idx) {
            poll_python_interrupt();
            auto const & read_id_dest = *read_id_dest_iters[out_idx];
            auto const output_path = output / read_id_dest.first;

            // Create the output file
            auto writer =
                pod5::create_file_writer(output_path.string(), "pod5_subset", output_options);
            if (!writer.ok()) {
                std::cerr << "Failed to create output file: " << output_path << std::endl;
                throw std::runtime_error("Failed to create output POD5 file");
            }

            // Add the output file writer to the repacker
            created_output_files.push_back(output_path);
            auto repacker_output_file = repacker->add_output(std::move(*writer), !duplicate_ok);
            std::size_t const repacker_output_idx = dest_to_output.size();
            dest_to_output.emplace_back(std::move(repacker_output_file));

            // Associate the requested read_ids to this output
            for (auto const & read_id : read_id_dest.second) {
                auto read_id_uuid = pod5::Uuid::from_string(read_id);
                if (!read_id_uuid) {
                    std::cerr << "Invalid read id uuid: " << read_id << std::endl;
                    throw std::runtime_error("Invalid read id uuid in mapping");
                }
                read_id_lookup.insert(std::make_pair(*read_id_uuid, repacker_output_idx));
            }
        }

        // Scale the max open input files by current output handle usage and system limits.
        std::size_t const max_open_input_files =
            io_limits::clamp_open_inputs(io_soft_limit, dest_to_output.size());
        std::size_t const max_in_size = std::max<std::size_t>(1, max_open_input_files);

        // Wait for the number of open readers in the repacker to go below `limit`
        auto wait_for_open_readers_below = [&](std::size_t limit) {
            auto last_update = std::chrono::steady_clock::now();
            while (repacker->currently_open_file_reader_count() >= limit) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                poll_python_interrupt();

                auto const now = std::chrono::steady_clock::now();
                if (now - last_update >= std::chrono::milliseconds(2000)) {
                    progress_bar.update(total_reads_completed + repacker->reads_completed());
                    progress_bar.set_task(
                        batch_prefix + "Waiting for queued writes to complete from "
                        + std::to_string(repacker->currently_open_file_reader_count())
                        + "files...");
                    last_update = now;
                }
            }
        };

        // Wait for the repacker to finish with it's currently open readers
        auto wait_for_open_readers_zero = [&]() {
            auto last_update = std::chrono::steady_clock::now();
            while (repacker->currently_open_file_reader_count() > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                poll_python_interrupt();

                auto const now = std::chrono::steady_clock::now();
                if (now - last_update >= std::chrono::milliseconds(2000)) {
                    progress_bar.update(total_reads_completed + repacker->reads_completed());
                    progress_bar.set_task(batch_prefix + "Waiting for batch IO to complete...");
                    last_update = now;
                }
            }
        };

        // Walk each input file in chunks for this output batch.
        for (std::size_t in_st = 0; in_st < inputs.size(); in_st += max_in_size) {
            poll_python_interrupt();
            std::size_t const in_end = std::min(in_st + max_in_size, inputs.size());

            // Add an input in this chunk
            for (std::size_t in_idx = in_st; in_idx < in_end; ++in_idx) {
                poll_python_interrupt();
                auto const & input_path = inputs[in_idx];

                // Keep in-flight readers below chunk limit.
                wait_for_open_readers_below(max_in_size);

                // Clear previous row selections from a previous input file.
                for (auto & output_file : dest_to_output) {
                    output_file.clear_per_input_working_data();
                }

                // "Batch [i/N]: Subsetting {input}"
                progress_bar.set_task(
                    batch_prefix + "Subsetting " + input_path.filename().string());

                // Open the input file
                auto input_reader_opt = pod5::open_file_reader(input_path.string(), input_options);
                if (!input_reader_opt.ok()) {
                    std::cerr << "Failed to open input file: " << input_path << std::endl;
                    throw std::runtime_error("Failed to open input POD5 file");
                }
                auto const & input_reader = *input_reader_opt;
                if (!issued_migration_warning && out_st == 0) {
                    auto const pre_migration_version = input_reader->file_version_pre_migration();
                    auto const post_migration_version =
                        input_reader->schema_metadata().writing_pod5_version;
                    if (pre_migration_version != post_migration_version) {
                        std::cerr << "Warning: Migrated an input from POD5 version "
                                  << pre_migration_version.to_string() << " to "
                                  << post_migration_version.to_string()
                                  << " while subsetting. This can affect performance "
                                     "significantly. Consider updating input files."
                                  << std::endl;
                    }
                    issued_migration_warning = true;
                }

                // Walk the input file batches:
                for (std::size_t i = 0; i < input_reader->num_read_record_batches(); ++i) {
                    poll_python_interrupt();
                    auto batch = input_reader->read_read_record_batch(i);
                    if (!batch.ok()) {
                        std::cerr << "Failed to read batch " << i
                                  << " from input file: " << input_path << std::endl;
                        throw std::runtime_error("Failed to read read record batch from POD5 file");
                    }

                    // Test each read id in the batch to see if we want it:
                    auto const & read_id_column = batch->read_id_column();
                    for (std::int64_t row = 0; row < read_id_column->length(); ++row) {
                        if ((row & 0x3FF) == 0) {
                            poll_python_interrupt();
                        }
                        auto const found = read_id_lookup.equal_range(read_id_column->Value(row));
                        for (auto it = found.first; it != found.second; ++it) {
                            dest_to_output[it->second].add_row(row);
                            found_read_count += 1;
                        }
                    }

                    // Store how many rows in this batch were selected:
                    for (auto & output_file : dest_to_output) {
                        output_file.finish_batch();
                    }

                    progress_bar.update(total_reads_completed + repacker->reads_completed());
                }

                // Submit selected reads to each output:
                for (auto & output_file : dest_to_output) {
                    repacker->add_selected_reads_to_output(
                        output_file.repacker_output,
                        input_reader,
                        gsl::make_span(output_file.batch_counts),
                        gsl::make_span(output_file.all_batch_rows));
                }
            }

            // Batch drain barrier for inputs in this output batch.
            wait_for_open_readers_zero();
        }

        // Set this output batch to finished:
        std::thread finisher([&] {
            for (auto & output_file : dest_to_output) {
                repacker->set_output_finished(output_file.repacker_output);
            }
        });
        auto join_finisher = gsl::finally([&] {
            if (finisher.joinable()) {
                finisher.join();
            }
        });

        // Wait for this batch to complete:
        progress_bar.set_task(batch_prefix + "Waiting for batch IO to complete...");
        try {
            while (!repacker->is_complete()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                poll_python_interrupt();
                progress_bar.update(total_reads_completed + repacker->reads_completed());
            }
        } catch (pybind11::error_already_set const &) {
            throw;
        } catch (std::exception const & e) {
            std::cout << "\nError during repacking: " << e.what() << std::endl;
        }

        if (finisher.joinable()) {
            finisher.join();
        }

        repacker->finish();
        total_reads_completed += repacker->reads_completed();
    }
    progress_bar.set_task("Finished");

    if (found_read_count < total_requested_read_ids && !missing_ok) {
        throw std::runtime_error("Missing read_ids from inputs but --missing-ok not set");
    }

    // Clear created output files from cleanup since we succeeded:
    created_output_files.clear();
}
