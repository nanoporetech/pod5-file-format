#pragma once

#include "pod5_format_pybind/api.h"

#include <pybind11/pybind11.h>

#include <memory>
#include <set>
#include <vector>

namespace repack {

class Pod5RepackerOutput;

class Pod5Repacker : public std::enable_shared_from_this<Pod5Repacker> {
public:
    Pod5Repacker();
    ~Pod5Repacker();

    void finish();

    std::shared_ptr<Pod5RepackerOutput> add_output(
        std::shared_ptr<pod5::FileWriter> const & output,
        bool check_duplicate_read_ids);
    void set_output_finished(std::shared_ptr<Pod5RepackerOutput> const & output);

    void add_all_reads_to_output(
        std::shared_ptr<Pod5RepackerOutput> const & output,
        Pod5FileReaderPtr const & input);

    void py_add_selected_reads_to_output(
        std::shared_ptr<Pod5RepackerOutput> const & output,
        Pod5FileReaderPtr const & input,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batch_counts,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && all_batch_rows);

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
