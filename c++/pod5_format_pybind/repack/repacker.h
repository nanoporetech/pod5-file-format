#pragma once

#include "pod5_format_pybind/api.h"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <pybind11/pybind11.h>

#include <iostream>

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

    void add_selected_reads_to_output(
        std::shared_ptr<Pod5RepackerOutput> const & output,
        Pod5FileReaderPtr const & input,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batch_counts,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && all_batch_rows);

    bool is_complete() const;
    std::size_t reads_completed() const;

    std::size_t currently_open_file_reader_count()
    {
        check_for_error();
        std::size_t reader_count = 0;
        bool any_expired = false;
        for (auto const & weak_reader : m_file_readers) {
            if (!weak_reader.expired()) {
                reader_count += 1;
            } else {
                any_expired = true;
            }
        }

        if (any_expired) {
            cleanup_submitted_readers();
        }

        return reader_count;
    }

private:
    void check_for_error() const;

    void cleanup_submitted_readers()
    {
        auto new_end = std::remove_if(
            m_file_readers.begin(), m_file_readers.end(), [](auto & ptr) { return ptr.expired(); });
        m_file_readers.erase(new_end, m_file_readers.end());
    }

    void register_submitted_reader(Pod5FileReaderPtr const & input)
    {
        cleanup_submitted_readers();
        m_file_readers.emplace_back(input.reader);
    }

    boost::asio::io_context m_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> m_work;
    std::vector<std::thread> m_workers;

    mutable std::vector<std::weak_ptr<pod5::FileReader>> m_file_readers;
    std::vector<std::shared_ptr<Pod5RepackerOutput>> m_outputs;

    std::size_t m_reads_complete_deleted_outputs{0};
};

}  // namespace repack
