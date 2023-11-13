#pragma once

#include "pod5_format/file_writer.h"
#include "repack_states.h"

#include <boost/thread/synchronized_value.hpp>

#include <deque>

namespace repack {

class Pod5Repacker;

class Pod5RepackerOutputState;

class Pod5RepackerOutput {
public:
    Pod5RepackerOutput(
        std::shared_ptr<Pod5Repacker> const & repacker,
        boost::asio::io_context & m_context,
        std::shared_ptr<pod5::FileWriter> const & output,
        bool check_duplicate_read_ids);
    ~Pod5RepackerOutput();

    std::string path() const { return m_output->path(); }

    std::shared_ptr<Pod5Repacker> const & repacker() const { return m_repacker; }

    bool has_tasks() const;

    arrow::Status const & error() { return *m_error; }

    bool has_error() const { return m_has_error.load(); }

    // Inform the output no further reads will be added
    void set_finished();

    // Check if the output has completed all writes
    bool is_complete() const;

    // Number of reads completed
    std::size_t reads_completed() const;

    // Register new writes to the output, should not be called after #set_reads_finished
    void register_new_reads(
        std::shared_ptr<pod5::FileReader> const & input,
        std::size_t batch_index,
        std::vector<std::uint32_t> && batch_rows = {}  // All rows by default
    );

private:
    void post_try_work();

    void set_error(arrow::Status const & error)
    {
        m_error = error;
        m_has_error = true;
    }

    std::shared_ptr<Pod5Repacker> m_repacker;
    boost::asio::io_context & m_context;
    std::shared_ptr<pod5::FileWriter> m_output;
    std::atomic<bool> m_finished{false};

    std::atomic<bool> m_has_error{false};
    boost::synchronized_value<arrow::Status> m_error;

    std::atomic<std::size_t> m_in_flight{0};
    boost::synchronized_value<std::deque<states::shared_variant>> m_active_read_table_states;

    std::unique_ptr<Pod5RepackerOutputState> m_progress_state;
};

}  // namespace repack
