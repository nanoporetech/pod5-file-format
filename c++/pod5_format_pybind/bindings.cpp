#include "pod5_format/c_api.h"
#include "pod5_format/file_reader.h"
#include "pod5_format/file_writer.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_compression.h"
#include "pod5_format/signal_table_reader.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/memory_pool.h>
#include <boost/lexical_cast.hpp>
#include <boost/thread/synchronized_value.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#define POD5_PYTHON_RETURN_NOT_OK(result)                     \
    if (!result.ok()) {                                       \
        throw std::runtime_error(result.status().ToString()); \
    }

#define POD5_PYTHON_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr)  \
    auto&& result_name = (rexpr);                                  \
    if (!(result_name).ok()) {                                     \
        throw std::runtime_error(result_name.status().ToString()); \
    }                                                              \
    lhs = std::move(result_name).ValueUnsafe();

#define POD5_PYTHON_ASSIGN_OR_RAISE(lhs, rexpr)                                                \
    POD5_PYTHON_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                                     lhs, rexpr);

namespace py = pybind11;

namespace {

void throw_on_error(pod5::Status const& s) {
    if (!s.ok()) {
        throw std::runtime_error(s.ToString());
    }
}

template <typename T>
T throw_on_error(pod5::Result<T> const& s) {
    if (!s.ok()) {
        throw std::runtime_error(s.status().ToString());
    }
    return *s;
}
std::shared_ptr<pod5::FileWriter> create_combined_file(char const* path,
                                                       std::string const& writer_name,
                                                       pod5::FileWriterOptions const* options) {
    pod5::FileWriterOptions dummy;
    POD5_PYTHON_ASSIGN_OR_RAISE(
            auto writer,
            pod5::create_combined_file_writer(path, writer_name,
                                              options ? *options : pod5::FileWriterOptions{}));
    return writer;
}

std::shared_ptr<pod5::FileWriter> create_split_file(char const* signal_path,
                                                    char const* reads_path,
                                                    std::string const& writer_name,
                                                    pod5::FileWriterOptions const* options) {
    pod5::FileWriterOptions dummy;
    POD5_PYTHON_ASSIGN_OR_RAISE(
            auto writer,
            pod5::create_split_file_writer(signal_path, reads_path, writer_name,
                                           options ? *options : pod5::FileWriterOptions{}));
    return writer;
}

class Pod5SignalCacheBatch {
public:
    Pod5SignalCacheBatch(std::uint32_t batch_index,
                         std::size_t batch_rows_start_offset,
                         std::size_t batch_row_count,
                         pod5::ReadTableRecordBatch&& read_batch)
            : m_batch_index(batch_index),
              m_batch_rows_start_offset(batch_rows_start_offset),
              m_batch_row_count(batch_row_count),
              m_next_row_to_start(0),
              m_completed_rows(0),
              m_sample_counts(batch_row_count),
              m_samples(batch_row_count),
              m_read_batch(std::move(read_batch)) {}

    py::array_t<std::uint64_t> sample_count() const {
        return py::array_t<std::uint64_t>(m_sample_counts.size(), m_sample_counts.data());
    }

    py::list samples() const {
        py::list py_samples;
        for (auto const& row_samples : m_samples) {
            py_samples.append(py::array_t<std::int16_t>(row_samples.size(), row_samples.data()));
        }

        return py_samples;
    }

    void set_samples(std::size_t row,
                     std::uint64_t sample_count,
                     std::vector<std::int16_t>&& samples) {
        m_sample_counts[row] = sample_count;
        m_samples[row] = std::move(samples);
    }

    std::uint32_t batch_index() const { return m_batch_index; }
    std::size_t batch_rows_start_offset() const { return m_batch_rows_start_offset; }
    std::uint32_t batch_row_count() const { return m_batch_row_count; }

    pod5::ReadTableRecordBatch const& read_batch() const { return m_read_batch; }

    std::uint32_t start_rows(std::unique_lock<std::mutex>& l, std::size_t row_count) {
        auto row = m_next_row_to_start;
        m_next_row_to_start += row_count;
        return row;
    }

    void complete_rows(std::uint32_t row_count) { m_completed_rows += row_count; }

    bool has_work_left() const { return m_next_row_to_start < m_batch_row_count; }

    bool is_complete() const { return m_completed_rows.load() >= m_batch_row_count; }

private:
    std::uint32_t m_batch_index;
    std::size_t m_batch_rows_start_offset;
    std::uint32_t m_batch_row_count;

    std::uint32_t m_next_row_to_start;
    std::atomic<std::uint32_t> m_completed_rows;

    std::vector<std::uint64_t> m_sample_counts;
    std::vector<std::vector<std::int16_t>> m_samples;
    pod5::ReadTableRecordBatch m_read_batch;
};

class Pod5SignalCache {
public:
    struct CppReader {
        std::unique_ptr<pod5::FileReader> reader;
    };

    Pod5SignalCache(
            Pod5FileReader_t* reader,
            bool get_samples,
            bool get_sample_count,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_counts,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_rows,
            std::size_t worker_count = 10)
            : m_reader((CppReader*)reader),
              m_get_samples(get_samples),
              m_get_sample_count(get_sample_count),
              m_reads_batch_count(m_reader->reader->num_read_record_batches()),
              m_batch_counts_ref(std::move(batch_counts)),
              m_batch_counts(gsl::make_span(m_batch_counts_ref.data(), m_batch_counts_ref.size())),
              m_total_batch_count_so_far(0),
              m_batch_rows_ref(std::move(batch_rows)),
              m_batch_rows(gsl::make_span(m_batch_rows_ref.data(), m_batch_rows_ref.size())),
              m_worker_job_size(std::max<std::size_t>(
                      50,
                      m_batch_rows.size() / (m_reads_batch_count * worker_count * 2))),
              m_current_batch(0),
              m_finished(false),
              m_has_error(false) {
        {
            std::unique_lock<std::mutex> l(m_worker_sync);
            setup_next_in_progress_batch(l);
        }
        for (std::size_t i = 0; i < worker_count; ++i) {
            m_workers.emplace_back([&] { run_worker(); });
        }
    }

    ~Pod5SignalCache() {
        //std::cout << "dtor start\n";
        m_finished = true;
        for (std::size_t i = 0; i < m_workers.size(); ++i) {
            m_workers[i].join();
        }
        //std::cout << "dtor done\n";
    }

    std::shared_ptr<Pod5SignalCacheBatch> release_next_batch() {
        std::shared_ptr<Pod5SignalCacheBatch> batch;
        do {
            std::unique_lock<std::mutex> l(m_batches_sync);
            m_batch_done.wait_until(
                    l, std::chrono::steady_clock::now() + std::chrono::milliseconds(10),
                    [&] { return m_batches.size() || m_finished; });
            if (!m_batches.empty()) {
                batch = std::move(m_batches.front());
                assert(batch);
                m_batches.pop_front();
                m_batches_size -= 1;
                break;
            }
        } while (!m_finished);

        if (m_has_error) {
            throw std::runtime_error(m_error->ToString());
        }

        if (batch) {
            // Wait if we are ahead of the loader:
            while (!batch->is_complete()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            //std::cout << "Return batch " << batch->batch_index() << "\n";
            return batch;
        }

        assert(m_finished);
        throw pybind11::stop_iteration();
    }

private:
    static constexpr std::size_t MAX_PENDING_BATCHES = 10;

    void run_worker() {
        while (!m_finished) {
            std::shared_ptr<Pod5SignalCacheBatch> batch;
            std::uint32_t row_start = 0;
            {
                std::unique_lock<std::mutex> l(m_worker_sync);
                if (m_current_batch >= m_reads_batch_count) {
                    release_in_progress_batch();
                    break;
                }

                if (!m_in_progress_batch->has_work_left()) {
                    // If we have many batches complete that have not been queried, wait for it to get taken:
                    if (m_batches_size > MAX_PENDING_BATCHES) {
                        l.unlock();
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        continue;
                    }

                    // Then release our now complete batch, and setup the next:
                    if (!m_batch_counts.empty()) {
                        m_total_batch_count_so_far += m_batch_counts[m_current_batch];
                    }
                    m_current_batch += 1;
                    //std::cout << "start next batch " << m_current_batch << "\n";

                    release_in_progress_batch();
                    if (m_current_batch >= m_reads_batch_count) {
                        // No more work to do.
                        //std::cout << "all work done";
                        m_finished = true;
                        break;
                    }

                    setup_next_in_progress_batch(l);
                }

                batch = m_in_progress_batch;
                row_start = m_in_progress_batch->start_rows(l, m_worker_job_size);
            }

            std::uint32_t const row_end =
                    std::min(row_start + m_worker_job_size, batch->batch_row_count());

            do_work(batch, row_start, row_end);
            batch->complete_rows(m_worker_job_size);
        }
        //std::cout << "worker done\n";
    }

    void do_work(std::shared_ptr<Pod5SignalCacheBatch> const& batch,
                 std::uint32_t row_start,
                 std::uint32_t row_end) {
        auto sample_counts = batch->read_batch().signal_column();

        std::size_t abs_batch_row_offset = batch->batch_rows_start_offset();
        for (std::uint32_t i = row_start; i < row_end; ++i) {
            auto actual_batch_row = i;
            if (!m_batch_rows.empty()) {
                actual_batch_row = m_batch_rows[i + abs_batch_row_offset];
            }
            auto signal_rows = std::static_pointer_cast<arrow::UInt64Array>(
                    sample_counts->value_slice(actual_batch_row));

            auto const signal_rows_span =
                    gsl::make_span(signal_rows->raw_values(), signal_rows->length());

            auto sample_count_result = m_reader->reader->extract_sample_count(signal_rows_span);
            if (!sample_count_result.ok()) {
                m_error = sample_count_result.status();
                m_has_error = true;
                return;
            }
            std::uint64_t sample_count = *sample_count_result;

            std::vector<std::int16_t> samples(sample_count);
            if (m_get_samples) {
                auto samples_result = m_reader->reader->extract_samples(signal_rows_span,
                                                                        gsl::make_span(samples));
                if (!samples_result.ok()) {
                    m_error = samples_result;
                    m_has_error = true;
                    return;
                }
                sample_count = samples.size();
            }

            batch->set_samples(i, sample_count, std::move(samples));
        }
    }

    void setup_next_in_progress_batch(std::unique_lock<std::mutex>& lock) {
        assert(!m_in_progress_batch);
        auto read_batch = m_reader->reader->read_read_record_batch(m_current_batch);
        if (!read_batch.ok()) {
            m_error = read_batch.status();
            m_has_error = true;
            return;
        }
        std::size_t row_count = read_batch->num_rows();
        if (!m_batch_counts.empty()) {
            row_count = m_batch_counts[m_current_batch];
        }
        m_in_progress_batch = std::make_shared<Pod5SignalCacheBatch>(
                m_current_batch, m_total_batch_count_so_far, row_count, std::move(*read_batch));
    }

    void release_in_progress_batch() {
        if (m_in_progress_batch) {
            std::lock_guard<std::mutex> l2(m_batches_sync);
            m_batches.emplace_back(std::move(m_in_progress_batch));
            m_batches_size += 1;
            m_batch_done.notify_all();
        }
    }

    CppReader* m_reader;
    bool m_get_samples;
    bool m_get_sample_count;
    std::size_t m_reads_batch_count;
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> m_batch_counts_ref;
    gsl::span<std::uint32_t const> m_batch_counts;
    std::size_t m_total_batch_count_so_far;
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> m_batch_rows_ref;
    gsl::span<std::uint32_t const> m_batch_rows;

    std::uint32_t const m_worker_job_size;

    std::mutex m_worker_sync;
    std::condition_variable m_batch_done;
    std::uint32_t m_current_batch;

    std::atomic<bool> m_finished;
    std::atomic<bool> m_has_error;
    boost::synchronized_value<pod5::Status> m_error;
    std::shared_ptr<Pod5SignalCacheBatch> m_in_progress_batch;

    std::mutex m_batches_sync;
    std::atomic<std::uint32_t> m_batches_size;
    std::deque<std::shared_ptr<Pod5SignalCacheBatch>> m_batches;

    std::vector<std::thread> m_workers;
};

struct Pod5FileReaderPtr {
    Pod5FileReader_t* reader = nullptr;

    Pod5FileReaderPtr(Pod5FileReader_t* reader_) : reader(reader_) {}

    EmbeddedFileData get_combined_file_read_table_location() const {
        EmbeddedFileData result;
        auto error = pod5_get_combined_file_read_table_location(reader, &result);
        if (error != POD5_OK) {
            throw std::runtime_error(pod5_get_error_string());
        }
        return result;
    }

    EmbeddedFileData get_combined_file_signal_table_location() const {
        EmbeddedFileData result;
        auto error = pod5_get_combined_file_signal_table_location(reader, &result);
        if (error != POD5_OK) {
            throw std::runtime_error(pod5_get_error_string());
        }
        return result;
    }

    void close() {
        auto error = pod5_close_and_free_reader(reader);
        if (error != POD5_OK) {
            throw std::runtime_error(pod5_get_error_string());
        }
        reader = nullptr;
    }

    std::size_t plan_traversal(
            py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const&
                    read_id_data,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>& batch_counts,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>& batch_rows) {
        std::size_t successful_find_count = 0;
        auto error = pod5_plan_traversal(reader, read_id_data.data(), read_id_data.shape(0),
                                         batch_counts.mutable_data(), batch_rows.mutable_data(),
                                         &successful_find_count);
        if (error != POD5_OK) {
            throw std::runtime_error(pod5_get_error_string());
        }
        return successful_find_count;
    }

    std::shared_ptr<Pod5SignalCache> batch_get_signal(
            bool get_samples,
            bool get_sample_count,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_counts,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_rows) {
        return std::make_shared<Pod5SignalCache>(reader, get_samples, get_sample_count,
                                                 std::move(batch_counts), std::move(batch_rows));
    }
};

Pod5FileReaderPtr open_combined_file(char const* filename) {
    auto reader = pod5_open_combined_file(filename);
    if (!reader) {
        throw std::runtime_error(pod5_get_error_string());
    }
    return Pod5FileReaderPtr(reader);
}

Pod5FileReaderPtr open_split_file(char const* signal_filename, char const* reads_filename) {
    auto reader = pod5_open_split_file(signal_filename, reads_filename);
    if (!reader) {
        throw std::runtime_error(pod5_get_error_string());
    }
    return Pod5FileReaderPtr(reader);
}

pod5::RunInfoDictionaryIndex FileWriter_add_run_info(
        pod5::FileWriter& w,
        std::string& acquisition_id,
        std::int64_t acquisition_start_time,
        std::int16_t adc_max,
        std::int16_t adc_min,
        std::vector<std::pair<std::string, std::string>>&& context_tags,
        std::string& experiment_name,
        std::string& flow_cell_id,
        std::string& flow_cell_product_code,
        std::string& protocol_name,
        std::string& protocol_run_id,
        std::int64_t protocol_start_time,
        std::string& sample_id,
        std::uint16_t sample_rate,
        std::string& sequencing_kit,
        std::string& sequencer_position,
        std::string& sequencer_position_type,
        std::string& software,
        std::string& system_name,
        std::string& system_type,
        std::vector<std::pair<std::string, std::string>>&& tracking_id) {
    return throw_on_error(w.add_run_info({std::move(acquisition_id),
                                          acquisition_start_time,
                                          adc_max,
                                          adc_min,
                                          std::move(context_tags),
                                          std::move(experiment_name),
                                          std::move(flow_cell_id),
                                          std::move(flow_cell_product_code),
                                          std::move(protocol_name),
                                          std::move(protocol_run_id),
                                          std::move(protocol_start_time),
                                          std::move(sample_id),
                                          sample_rate,
                                          std::move(sequencing_kit),
                                          std::move(sequencer_position),
                                          std::move(sequencer_position_type),
                                          std::move(software),
                                          std::move(system_name),
                                          std::move(system_type),
                                          std::move(tracking_id)}));
}

void FileWriter_add_reads(
        pod5::FileWriter& w,
        std::size_t count,
        py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const& read_id_data,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& pores,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& calibrations,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const& read_numbers,
        py::array_t<std::uint64_t, py::array::c_style | py::array::forcecast> const& start_samples,
        py::array_t<float, py::array::c_style | py::array::forcecast> const& median_befores,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& end_reasons,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& run_infos,
        py::list signal_ptrs) {
    if (read_id_data.shape(1) != 16) {
        throw std::runtime_error("Read id array is of unexpected size");
    }

    auto read_ids = reinterpret_cast<boost::uuids::uuid const*>(read_id_data.data(0));
    auto signal_it = signal_ptrs.begin();
    for (std::size_t i = 0; i < count; ++i, ++signal_it) {
        if (signal_it == signal_ptrs.end()) {
            throw std::runtime_error("Missing signal data");
        }
        auto signal = signal_it->cast<
                py::array_t<std::int16_t, py::array::c_style | py::array::forcecast>>();
        auto signal_span = gsl::make_span(signal.data(), signal.size());
        throw_on_error(w.add_complete_read(
                pod5::ReadData{read_ids[i], *pores.data(i), *calibrations.data(i),
                               *read_numbers.data(i), *start_samples.data(i),
                               *median_befores.data(i), *end_reasons.data(i), *run_infos.data(i)},
                signal_span));
    }
}

void FileWriter_add_reads_pre_compressed(
        pod5::FileWriter& w,
        std::size_t count,
        py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const& read_id_data,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& pores,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& calibrations,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const& read_numbers,
        py::array_t<std::uint64_t, py::array::c_style | py::array::forcecast> const& start_samples,
        py::array_t<float, py::array::c_style | py::array::forcecast> const& median_befores,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& end_reasons,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& run_infos,
        py::list compressed_signal_ptrs,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const& sample_counts,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const&
                signal_chunk_counts) {
    if (read_id_data.shape(1) != 16) {
        throw std::runtime_error("Read id array is of unexpected size");
    }

    auto read_ids = reinterpret_cast<boost::uuids::uuid const*>(read_id_data.data(0));
    auto compressed_signal_it = compressed_signal_ptrs.begin();
    auto sample_counts_it = sample_counts.data();
    for (std::size_t i = 0; i < count; ++i) {
        auto const read_id = read_ids[i];

        auto const signal_chunk_count = *signal_chunk_counts.data(i);
        std::vector<std::uint64_t> signal_rows(signal_chunk_count);
        for (std::size_t signal_chunk_idx = 0; signal_chunk_idx < signal_chunk_count;
             ++signal_chunk_idx) {
            if (compressed_signal_it == compressed_signal_ptrs.end()) {
                throw std::runtime_error("Missing signal data");
            }
            auto compressed_signal = compressed_signal_it->cast<
                    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast>>();
            auto compressed_signal_span =
                    gsl::make_span(compressed_signal.data(), compressed_signal.size());

            auto signal_row = throw_on_error(w.add_pre_compressed_signal(
                    read_id, compressed_signal_span, *sample_counts_it));
            signal_rows[signal_chunk_idx] = signal_row;

            ++compressed_signal_it;
            ++sample_counts_it;
        }

        throw_on_error(w.add_complete_read(
                pod5::ReadData{read_id, *pores.data(i), *calibrations.data(i),
                               *read_numbers.data(i), *start_samples.data(i),
                               *median_befores.data(i), *end_reasons.data(i), *run_infos.data(i)},
                signal_rows));
    }
}

void decompress_signal_wrapper(
        py::array_t<uint8_t, py::array::c_style | py::array::forcecast> const& compressed_signal,
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast>& signal_out) {
    throw_on_error(pod5::decompress_signal(
            gsl::make_span(compressed_signal.data(0), compressed_signal.shape(0)),
            arrow::system_memory_pool(),
            gsl::make_span(signal_out.mutable_data(0), signal_out.shape(0))));
}

std::size_t compress_signal_wrapper(
        py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const& signal,
        py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast>&
                compressed_signal_out) {
    auto size = throw_on_error(pod5::compress_signal(
            gsl::make_span(signal.data(), signal.shape(0)), arrow::system_memory_pool(),
            gsl::make_span(compressed_signal_out.mutable_data(), compressed_signal_out.shape(0))));

    return size;
}

std::size_t vbz_compressed_signal_max_size(std::size_t sample_count) {
    return pod5::compressed_signal_max_size(sample_count);
}

void load_read_id_iterable(
        py::iterable const& read_ids_str,
        py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast>& read_id_data_out) {
    std::size_t out_idx = 0;
    auto read_ids = reinterpret_cast<boost::uuids::uuid*>(read_id_data_out.mutable_data());
    auto read_ids_out_len = read_id_data_out.shape(0);

    std::string temp_uuid;
    for (auto& read_id : read_ids_str) {
        if (out_idx >= read_ids_out_len) {
            throw std::runtime_error("Too many input uuids for output container");
        }

        temp_uuid = read_id.cast<py::str>();
        read_ids[out_idx++] = boost::lexical_cast<boost::uuids::uuid>(temp_uuid);
    }
}

py::list format_read_id_to_str(
        py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast>& read_id_data_out) {
    if (read_id_data_out.size() % 16 != 0) {
        throw std::runtime_error(
                "Unexpected amount of data for read id - expected data to align to 16 bytes.");
    }

    py::list result;

    std::array<char, 37> str_data;
    std::size_t const count = read_id_data_out.size() / 16;
    for (std::size_t i = 0; i < count; ++i) {
        auto read_id_data = read_id_data_out.data() + (i * 16);

        pod5_format_read_id(read_id_data, str_data.data());
        result.append(py::str(str_data.data(), str_data.size() - 1));
    }

    return result;
}

}  // namespace

PYBIND11_MODULE(pod5_format_pybind, m) {
    using namespace pod5;
    pod5_init();

    m.doc() = "POD5 Format Raw Bindings";

    py::class_<FileWriterOptions>(m, "FileWriterOptions")
            .def_property("max_signal_chunk_size", &FileWriterOptions::max_signal_chunk_size,
                          &FileWriterOptions::set_max_signal_chunk_size)
            .def_property("signal_table_batch_size", &FileWriterOptions::signal_table_batch_size,
                          &FileWriterOptions::set_signal_table_batch_size)
            .def_property("read_table_batch_size", &FileWriterOptions::read_table_batch_size,
                          &FileWriterOptions::set_read_table_batch_size)
            .def_property("signal_compression_type", &FileWriterOptions::signal_type,
                          &FileWriterOptions::set_signal_type);

    py::class_<FileWriter, std::shared_ptr<FileWriter>>(m, "FileWriter")
            .def("close", [](pod5::FileWriter& w) { throw_on_error(w.close()); })
            .def("add_pore",
                 [](pod5::FileWriter& w, std::uint16_t channel, std::uint8_t well,
                    std::string pore_type) {
                     return throw_on_error(
                             w.add_pore(pod5::PoreData{channel, well, std::move(pore_type)}));
                 })
            .def("add_end_reason",
                 [](pod5::FileWriter& w, int name, bool forced) {
                     return throw_on_error(w.add_end_reason(pod5::EndReasonData{
                             (pod5::EndReasonData::ReadEndReason)name, forced}));
                 })
            .def("add_calibration",
                 [](pod5::FileWriter& w, float offset, float scale) {
                     return throw_on_error(w.add_calibration(pod5::CalibrationData{offset, scale}));
                 })
            .def("add_run_info", FileWriter_add_run_info)
            .def("add_reads", FileWriter_add_reads)
            .def("add_reads_pre_compressed", FileWriter_add_reads_pre_compressed);

    py::class_<EmbeddedFileData>(m, "EmbeddedFileData")
            .def_readonly("offset", &EmbeddedFileData::offset)
            .def_readonly("length", &EmbeddedFileData::length);

    py::class_<Pod5SignalCache, std::shared_ptr<Pod5SignalCache>>(m, "Pod5SignalCache")
            .def("release_next_batch", &Pod5SignalCache::release_next_batch);

    py::class_<Pod5SignalCacheBatch, std::shared_ptr<Pod5SignalCacheBatch>>(m,
                                                                            "Pod5SignalCacheBatch")
            .def_property_readonly("batch_index", &Pod5SignalCacheBatch::batch_index)
            .def_property_readonly("sample_count", &Pod5SignalCacheBatch::sample_count)
            .def_property_readonly("samples", &Pod5SignalCacheBatch::samples);

    py::class_<Pod5FileReaderPtr>(m, "Pod5FileReader")
            .def("get_combined_file_read_table_location",
                 &Pod5FileReaderPtr::get_combined_file_read_table_location)
            .def("get_combined_file_signal_table_location",
                 &Pod5FileReaderPtr::get_combined_file_signal_table_location)
            .def("plan_traversal", &Pod5FileReaderPtr::plan_traversal)
            .def("batch_get_signal", &Pod5FileReaderPtr::batch_get_signal)
            .def("close", &Pod5FileReaderPtr::close);

    // Errors API
    m.def("get_error_string", &pod5_get_error_string, "Get the most recent error as a string");

    // Creating files
    m.def("create_combined_file", &create_combined_file, "Create a combined POD5 file for writing",
          py::arg("filename"), py::arg("writer_name"), py::arg("options") = nullptr);
    m.def("create_split_file", &create_split_file, "Create a split POD5 file for writing",
          py::arg("signal_filename"), py::arg("reads_filename"), py::arg("writer_name"),
          py::arg("options") = nullptr);

    // Opening files
    m.def("open_combined_file", &open_combined_file, "Open a combined POD5 file for reading");
    m.def("open_split_file", &open_split_file, "Open a split POD5 file for reading");

    // Signal API
    m.def("decompress_signal", &decompress_signal_wrapper, "Decompress a numpy array of signal");
    m.def("compress_signal", &compress_signal_wrapper, "Compress a numpy array of signal");
    m.def("vbz_compressed_signal_max_size", &vbz_compressed_signal_max_size);

    // Util API
    m.def("load_read_id_iterable", &load_read_id_iterable,
          "Load an iterable of read ids into a numpy array of data");
    m.def("format_read_id_to_str", &format_read_id_to_str, "Format an array of read ids to string");
}
