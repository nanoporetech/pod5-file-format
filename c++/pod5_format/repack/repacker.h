#pragma once

#include "pod5_format/file_reader.h"
#include "pod5_format/file_writer.h"

#include <memory>
#include <set>
#include <vector>

namespace repack {

class Pod5RepackerOutput;

class Pod5Repacker final : public std::enable_shared_from_this<Pod5Repacker> {
    // Private since we must be inside a shared_ptr.
    struct MustBeSharedPtr {};

public:
    static std::shared_ptr<Pod5Repacker> create();

    Pod5Repacker(MustBeSharedPtr);
    ~Pod5Repacker();

    void finish();

    std::shared_ptr<Pod5RepackerOutput> add_output(
        std::shared_ptr<pod5::FileWriter> const & output,
        bool check_duplicate_read_ids);
    void set_output_finished(std::shared_ptr<Pod5RepackerOutput> const & output);

    void add_all_reads_to_output(
        std::shared_ptr<Pod5RepackerOutput> const & output,
        std::shared_ptr<pod5::FileReader> const & input);

    void add_selected_reads_to_output(
        std::shared_ptr<Pod5RepackerOutput> const & output,
        std::shared_ptr<pod5::FileReader> const & input,
        gsl::span<std::uint32_t const> batch_counts,
        gsl::span<std::uint32_t const> all_batch_rows);

    bool is_complete() const;
    std::size_t reads_completed() const;

    std::size_t currently_open_file_reader_count()
    {
        check_for_error();
        cleanup_submitted_readers();
        return m_file_readers.size();
    }

private:
    void check_for_error() const;

    void cleanup_submitted_readers()
    {
        std::erase_if(m_file_readers, [](auto const & ptr) { return ptr.expired(); });
    }

    void register_submitted_reader(std::shared_ptr<pod5::FileReader> const & input)
    {
        cleanup_submitted_readers();
        m_file_readers.insert(input);
    }

    std::shared_ptr<pod5::ThreadPool> m_thread_pool;
    std::set<std::weak_ptr<pod5::FileReader>, std::owner_less<>> m_file_readers;
    std::vector<std::shared_ptr<Pod5RepackerOutput>> m_outputs;

    std::size_t m_reads_complete_deleted_outputs{0};
};

}  // namespace repack
