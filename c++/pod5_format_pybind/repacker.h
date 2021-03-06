#include "api.h"
#include "pod5_format/internal/tracing/tracing.h"

#include <arrow/array/array_dict.h>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/optional.hpp>
#include <boost/thread/synchronized_value.hpp>
#include <pybind11/pybind11.h>

#include <thread>

class Pod5Repacker;

struct pair_hasher {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2>& pair) const {
        return std::hash<T1>{}(pair.first) ^ std::hash<T2>{}(pair.second);
    }
};

struct ReadSignalBatch {
    pod5::SignalType signal_type;

    struct ReadSignal {
        std::vector<std::shared_ptr<arrow::Buffer>> signal_data;
        std::vector<std::uint32_t> sample_counts;
    };

    std::vector<ReadSignal> data;
    std::int64_t preload_sum;
};

class Pod5ReadBatch {
public:
    Pod5ReadBatch(pod5::ReadTableRecordBatch&& batch,
                  std::shared_ptr<pod5::FileReader> const& file,
                  ReadSignalBatch&& read_signal)
            : m_batch(std::move(batch)), m_file(file), m_read_signal(std::move(read_signal)) {}

    pod5::ReadTableRecordBatch& batch() { return m_batch; }
    std::shared_ptr<pod5::FileReader>& file() { return m_file; }
    ReadSignalBatch const& read_signal() const { return m_read_signal; }

private:
    pod5::ReadTableRecordBatch m_batch;
    std::shared_ptr<pod5::FileReader> m_file;
    ReadSignalBatch m_read_signal;
};

class Pod5RepackerOutput {
public:
    using WriteIndex = std::uint64_t;

    struct PendingWrite {
        WriteIndex index;
        std::shared_ptr<Pod5ReadBatch> batch;
        std::vector<std::uint32_t> selected_rows;
    };

    Pod5RepackerOutput(std::shared_ptr<Pod5Repacker> const& repacker,
                       boost::asio::io_context& context,
                       std::shared_ptr<pod5::FileWriter> const& output_file)
            : m_strand(context),
              m_repacker(repacker),
              m_output_file(output_file),
              m_signal_type(output_file->signal_type()),
              m_pending_write_count(0),
              m_reads_completed(0),
              m_reads_sample_bytes_completed(0),
              m_has_error(false) {}

    std::shared_ptr<Pod5Repacker> repacker() const { return m_repacker; }
    std::shared_ptr<pod5::FileWriter> output_file() const { return m_output_file; }
    pod5::SignalType signal_type() const { return m_signal_type; }

    template <typename CompletionHandler>
    void batch_write(WriteIndex index,
                     std::vector<std::uint32_t>&& batch_rows,
                     std::shared_ptr<Pod5ReadBatch> const& batch,
                     CompletionHandler complete) {
        m_strand.post([this, index, batch, complete, batch_rows = std::move(batch_rows)] {
            m_pending_writes.push_back({index, batch, std::move(batch_rows)});
            m_pending_write_count += 1;

            std::sort(m_pending_writes.begin(), m_pending_writes.end(),
                      [](auto const& a, auto const& b) { return a.index < b.index; });

            try_write_next_batch();
            complete();
        });
    }

    void try_write_next_batch() {
        if (m_pending_writes.empty()) {
            return;
        }

        auto& next_batch = m_pending_writes.front();
        if (next_batch.index == m_next_write_write_index) {
            auto result = write_next_batch(next_batch.batch, next_batch.selected_rows);
            if (!result.ok()) {
                set_error(result);
                return;
            }
            m_pending_writes.pop_front();
            m_pending_write_count -= 1;

            m_next_write_write_index += 1;

            // And try to write the next:
            m_strand.post([this] { try_write_next_batch(); });
        }
    }

    arrow::Status write_next_batch(std::shared_ptr<Pod5ReadBatch> const& batch,
                                   std::vector<std::uint32_t> const& selected_row_indices) {
        POD5_TRACE_FUNCTION();
        // Move signal between the two locations:
        auto const& source_read_table_batch = batch->batch();
        auto const& source_file = batch->file();

        m_reads_completed += source_read_table_batch.num_rows();

        auto source_reads_read_id_column = source_read_table_batch.read_id_column();
        auto source_reads_read_number_column = source_read_table_batch.read_number_column();
        auto source_reads_start_sample_column = source_read_table_batch.start_sample_column();
        auto source_reads_median_before_column = source_read_table_batch.median_before_column();

        auto source_reads_pore_column = std::static_pointer_cast<arrow::Int16Array>(
                source_read_table_batch.pore_column()->indices());
        auto source_reads_calibration_column = std::static_pointer_cast<arrow::Int16Array>(
                source_read_table_batch.calibration_column()->indices());
        auto source_reads_end_reason_column = std::static_pointer_cast<arrow::Int16Array>(
                source_read_table_batch.end_reason_column()->indices());
        auto source_reads_run_info_column = std::static_pointer_cast<arrow::Int16Array>(
                source_read_table_batch.run_info_column()->indices());

        auto const& loaded_signal = batch->read_signal();
        assert(loaded_signal.data.size() == selected_row_indices.size());

        for (std::size_t batch_row_index = 0; batch_row_index < selected_row_indices.size();
             ++batch_row_index) {
            auto batch_row = selected_row_indices[batch_row_index];
            // Find the read params
            auto const& read_id = source_reads_read_id_column->Value(batch_row);
            auto const& read_number = source_reads_read_number_column->Value(batch_row);
            auto const& start_sample = source_reads_start_sample_column->Value(batch_row);
            auto const& median_before = source_reads_median_before_column->Value(batch_row);

            auto const& pore_index = source_reads_pore_column->Value(batch_row);
            auto const& calibration_index = source_reads_calibration_column->Value(batch_row);
            auto const& end_reason_index = source_reads_end_reason_column->Value(batch_row);
            auto const& run_info_index = source_reads_run_info_column->Value(batch_row);

            std::vector<std::uint64_t> signal_rows;
            auto const& read_signal = loaded_signal.data[batch_row_index];

            // Write each compressed row to the dest file, and store its rows:
            for (std::size_t i = 0; i < read_signal.signal_data.size(); ++i) {
                auto const& sample_count = read_signal.sample_counts[i];
                auto const& signal_buffer = read_signal.signal_data[i];
                auto const signal_span =
                        gsl::make_span(signal_buffer->data(), signal_buffer->size());

                if (loaded_signal.signal_type == m_output_file->signal_type()) {
                    ARROW_ASSIGN_OR_RAISE(auto signal_row,
                                          m_output_file->add_pre_compressed_signal(
                                                  read_id, signal_span, sample_count));
                    signal_rows.push_back(signal_row);
                } else {
                    ARROW_ASSIGN_OR_RAISE(
                            auto new_signal_rows,
                            m_output_file->add_signal(read_id,
                                                      signal_span.as_span<std::int16_t const>()));
                    signal_rows.insert(signal_rows.end(), new_signal_rows.begin(),
                                       new_signal_rows.end());
                }

                m_reads_sample_bytes_completed += signal_span.size();
            }

            ARROW_ASSIGN_OR_RAISE(
                    auto dest_pore_index,
                    find_pore_index(source_file, source_read_table_batch, pore_index));
            ARROW_ASSIGN_OR_RAISE(auto dest_calibration_index,
                                  find_calibration_index(source_file, source_read_table_batch,
                                                         calibration_index));
            ARROW_ASSIGN_OR_RAISE(
                    auto dest_end_reason_index,
                    find_end_reason_index(source_file, source_read_table_batch, end_reason_index));
            ARROW_ASSIGN_OR_RAISE(
                    auto dest_run_info_index,
                    find_run_info_index(source_file, source_read_table_batch, run_info_index));

            ARROW_RETURN_NOT_OK(m_output_file->add_complete_read(
                    pod5::ReadData(read_id, dest_pore_index, dest_calibration_index, read_number,
                                   start_sample, median_before, dest_end_reason_index,
                                   dest_run_info_index),
                    signal_rows));
        }

        return arrow::Status::OK();
    }

    // Find or create a pore index in the output file - expects to run on strand.
    arrow::Result<pod5::PoreDictionaryIndex> find_pore_index(
            std::shared_ptr<pod5::FileReader> const& source_file,
            pod5::ReadTableRecordBatch const& source_batch,
            pod5::PoreDictionaryIndex source_index) {
        auto const key = std::make_pair(source_file, source_index);
        auto const it = m_pore_indexes.find(key);
        if (it != m_pore_indexes.end()) {
            return it->second;
        }

        ARROW_ASSIGN_OR_RAISE(auto source_data, source_batch.get_pore(source_index));
        ARROW_ASSIGN_OR_RAISE(auto const new_index, m_output_file->add_pore(source_data));
        m_pore_indexes[key] = new_index;
        return new_index;
    }

    // Find or create a calibration index in the output file - expects to run on strand.
    arrow::Result<pod5::CalibrationDictionaryIndex> find_calibration_index(
            std::shared_ptr<pod5::FileReader> const& source_file,
            pod5::ReadTableRecordBatch const& source_batch,
            pod5::CalibrationDictionaryIndex source_index) {
        auto const key = std::make_pair(source_file, source_index);
        auto const it = m_calibration_indexes.find(key);
        if (it != m_calibration_indexes.end()) {
            return it->second;
        }

        ARROW_ASSIGN_OR_RAISE(auto source_data, source_batch.get_calibration(source_index));
        ARROW_ASSIGN_OR_RAISE(auto const new_index, m_output_file->add_calibration(source_data));
        m_calibration_indexes[key] = new_index;
        return new_index;
    }

    // Find or create a end reason index in the output file - expects to run on strand.
    arrow::Result<pod5::EndReasonDictionaryIndex> find_end_reason_index(
            std::shared_ptr<pod5::FileReader> const& source_file,
            pod5::ReadTableRecordBatch const& source_batch,
            pod5::EndReasonDictionaryIndex source_index) {
        auto const key = std::make_pair(source_file, source_index);
        auto const it = m_end_reason_indexes.find(key);
        if (it != m_end_reason_indexes.end()) {
            return it->second;
        }

        ARROW_ASSIGN_OR_RAISE(auto source_data, source_batch.get_end_reason(source_index));
        ARROW_ASSIGN_OR_RAISE(auto const new_index, m_output_file->add_end_reason(source_data));
        m_end_reason_indexes[key] = new_index;
        return new_index;
    }

    // Find or create a run_info index in the output file - expects to run on strand.
    arrow::Result<pod5::RunInfoDictionaryIndex> find_run_info_index(
            std::shared_ptr<pod5::FileReader> const& source_file,
            pod5::ReadTableRecordBatch const& source_batch,
            pod5::RunInfoDictionaryIndex source_index) {
        auto const key = std::make_pair(source_file, source_index);
        auto const it = m_run_info_indexes.find(key);
        if (it != m_run_info_indexes.end()) {
            return it->second;
        }

        ARROW_ASSIGN_OR_RAISE(auto source_data, source_batch.get_run_info(source_index));
        ARROW_ASSIGN_OR_RAISE(auto const new_index, m_output_file->add_run_info(source_data));
        m_run_info_indexes[key] = new_index;
        return new_index;
    }

    // Find an incrementing index that orders writes onto the output, used to ensure all writes occur
    // in the desired order.
    WriteIndex get_next_write_index() { return m_next_write_index++; }

    std::size_t pending_writes() { return m_pending_write_count.load(); }
    std::size_t reads_completed() { return m_reads_completed.load(); }
    std::size_t reads_sample_bytes_completed() { return m_reads_sample_bytes_completed.load(); }

    boost::asio::io_context::strand& strand() { return m_strand; }

    arrow::Status const& error() { return *m_error; }
    bool has_error() const { return m_has_error.load(); }

private:
    void set_error(arrow::Status const& error) {
        m_error = error;
        m_has_error = true;
    }

    boost::asio::io_context::strand m_strand;
    std::shared_ptr<Pod5Repacker> m_repacker;
    std::shared_ptr<pod5::FileWriter> m_output_file;
    pod5::SignalType m_signal_type;

    std::deque<PendingWrite> m_pending_writes;
    std::atomic<std::size_t> m_pending_write_count;
    std::atomic<std::size_t> m_reads_completed;
    std::atomic<std::size_t> m_reads_sample_bytes_completed;

    std::atomic<bool> m_has_error;
    boost::synchronized_value<arrow::Status> m_error;

    WriteIndex m_next_write_index = 0;
    WriteIndex m_next_write_write_index = 0;

    template <typename IndexType>
    using DictionaryLookup =
            std::unordered_map<std::pair<std::shared_ptr<pod5::FileReader>, IndexType>,
                               IndexType,
                               pair_hasher>;
    DictionaryLookup<pod5::PoreDictionaryIndex> m_pore_indexes;
    DictionaryLookup<pod5::PoreDictionaryIndex> m_calibration_indexes;
    DictionaryLookup<pod5::PoreDictionaryIndex> m_end_reason_indexes;
    DictionaryLookup<pod5::PoreDictionaryIndex> m_run_info_indexes;
};

class Pod5Repacker : public std::enable_shared_from_this<Pod5Repacker> {
public:
    struct AddReadBatchToOutput {
        AddReadBatchToOutput(std::shared_ptr<Pod5RepackerOutput> output_,
                             Pod5RepackerOutput::WriteIndex write_index_,
                             Pod5FileReaderPtr input_,
                             std::size_t read_batch_index_)
                : output(output_),
                  write_index(write_index_),
                  input(input_),
                  read_batch_index(read_batch_index_) {}

        AddReadBatchToOutput(std::shared_ptr<Pod5RepackerOutput> output_,
                             Pod5RepackerOutput::WriteIndex write_index_,
                             Pod5FileReaderPtr input_,
                             std::size_t read_batch_index_,
                             std::vector<std::uint32_t>&& selected_rows_)
                : AddReadBatchToOutput(output_, write_index_, input_, read_batch_index_) {
            selected_rows = std::move(selected_rows_);
        }

        std::shared_ptr<Pod5RepackerOutput> output;
        Pod5RepackerOutput::WriteIndex write_index;
        Pod5FileReaderPtr input;
        std::size_t read_batch_index;
        std::vector<std::uint32_t> selected_rows;
    };

    Pod5Repacker(std::size_t target_pending_writes = 20,
                 std::size_t worker_count = std::thread::hardware_concurrency())
            : m_target_pending_writes(target_pending_writes),
              m_current_pending_reads(0),
              m_has_error(false),
              m_batches_requested(0),
              m_batches_completed(0),
              m_work(boost::asio::make_work_guard(m_context)) {
        m_workers.reserve(worker_count);
        for (std::size_t i = 0; i < worker_count; ++i) {
            m_workers.emplace_back([&] { m_context.run(); });
        }
    }

    ~Pod5Repacker() { finish(); }

    boost::asio::io_context& io_context() { return m_context; }

    void finish() {
        m_work.reset();
        for (auto& worker : m_workers) {
            worker.join();
        }
    }

    std::shared_ptr<Pod5RepackerOutput> add_output(
            std::shared_ptr<pod5::FileWriter> const& output) {
        auto repacker_output =
                std::make_shared<Pod5RepackerOutput>(shared_from_this(), m_context, output);
        m_outputs.push_back(repacker_output);
        return repacker_output;
    }

    void add_all_reads_to_output(std::shared_ptr<Pod5RepackerOutput> const& output,
                                 Pod5FileReaderPtr const& input) {
        if (output->repacker() != shared_from_this()) {
            throw std::runtime_error("Invalid repacker output passed, created by another repacker");
        }

        if (!input.reader) {
            throw std::runtime_error("Invalid input passed to repacker, no reader");
        }

        auto pending_batch_reads = m_pending_batch_reads.synchronize();
        for (std::size_t i = 0; i < input.reader->num_read_record_batches(); ++i) {
            pending_batch_reads->emplace_back(
                    AddReadBatchToOutput(output, output->get_next_write_index(), input, i));
        }
        m_batches_requested += input.reader->num_read_record_batches();

        post_do_batch_reads(m_target_pending_writes);
    }

    void add_selected_reads_to_output(
            std::shared_ptr<Pod5RepackerOutput> const& output,
            Pod5FileReaderPtr const& input,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_counts,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&&
                    all_batch_rows) {
        if (output->repacker() != shared_from_this()) {
            throw std::runtime_error("Invalid repacker output passed, created by another repacker");
        }

        if (!input.reader) {
            throw std::runtime_error("Invalid input passed to repacker, no reader");
        }

        auto batch_counts_span = gsl::make_span(batch_counts.data(), batch_counts.size());
        auto all_batch_rows_span = gsl::make_span(all_batch_rows.data(), all_batch_rows.size());

        auto pending_batch_reads = m_pending_batch_reads.synchronize();
        std::size_t current_start_point = 0;
        for (std::size_t i = 0; i < batch_counts_span.size(); ++i) {
            std::vector<std::uint32_t> batch_rows;
            auto const batch_rows_span =
                    all_batch_rows_span.subspan(current_start_point, batch_counts_span[i]);
            batch_rows.insert(batch_rows.end(), batch_rows_span.begin(), batch_rows_span.end());
            current_start_point += batch_counts_span[i];

            pending_batch_reads->emplace_back(AddReadBatchToOutput(
                    output, output->get_next_write_index(), input, i, std::move(batch_rows)));
        }
        m_batches_requested += batch_counts_span.size();

        post_do_batch_reads(m_target_pending_writes);
    }

    bool is_complete() const {
        if (m_has_error) {
            throw std::runtime_error(m_error->ToString());
        }

        for (auto const& output : m_outputs) {
            if (output->has_error()) {
                throw std::runtime_error(output->error().ToString());
            }
            if (output->pending_writes() > 0) {
                return false;
            }
        }

        return m_pending_batch_reads->empty();
    }

    std::size_t reads_sample_bytes_completed() const {
        std::size_t reads_sample_bytes_completed = 0;
        for (auto const& output : m_outputs) {
            reads_sample_bytes_completed += output->reads_sample_bytes_completed();
        }
        return reads_sample_bytes_completed;
    }
    std::size_t reads_completed() const {
        std::size_t reads_completed = 0;
        for (auto const& output : m_outputs) {
            reads_completed += output->reads_completed();
        }
        return reads_completed;
    }
    std::size_t pending_batch_writes() const {
        std::size_t pending_batch_writes = 0;
        for (auto const& output : m_outputs) {
            pending_batch_writes += output->pending_writes();
        }
        return pending_batch_writes;
    }

    std::size_t batches_requested() const { return m_batches_requested.load(); }
    std::size_t batches_completed() const { return m_batches_completed.load(); }

private:
    void post_do_batch_reads(std::size_t count) {
        for (std::size_t i = 0; i < count; ++i) {
            boost::asio::post(m_context, [=]() {
                if (m_has_error) {
                    return;
                }

                boost::optional<AddReadBatchToOutput> task;
                {
                    auto pending_batch_reads = m_pending_batch_reads.synchronize();
                    if (!pending_batch_reads->size()) {
                        return;
                    }
                    task = pending_batch_reads->front();
                    pending_batch_reads->pop_front();
                }

                // Do the task:
                auto selected_rows = std::move(task->selected_rows);
                auto batch = read_batch(task->input.reader, task->output->signal_type(),
                                        selected_rows, task->read_batch_index);
                if (!batch.ok()) {
                    set_error(batch.status());
                    return;
                }

                task->output->batch_write(task->write_index, std::move(selected_rows), *batch,
                                          [this, output = task->output] {
                                              m_batches_completed += 1;

                                              m_current_pending_reads -= 1;
                                              auto pending_actions =
                                                      m_current_pending_reads.load() +
                                                      output->pending_writes();
                                              if (m_target_pending_writes > pending_actions) {
                                                  int pending_write_target =
                                                          m_target_pending_writes - pending_actions;

                                                  // And post the next batch read now we are complete:
                                                  post_do_batch_reads(pending_write_target);
                                              }
                                          });
            });
        }
        m_current_pending_reads += count;
    }

    pod5::Result<std::shared_ptr<Pod5ReadBatch>> read_batch(
            std::shared_ptr<pod5::FileReader> const& source_file,
            pod5::SignalType dest_signal_type,
            std::vector<std::uint32_t>& selected_rows,
            std::size_t batch_index) {
        POD5_TRACE_FUNCTION();
        ARROW_ASSIGN_OR_RAISE(auto read_batch, source_file->read_read_record_batch(batch_index));

        // Default to all rows if not specfied
        if (selected_rows.empty()) {
            auto const source_batch_row_count = read_batch.num_rows();
            selected_rows.resize(source_batch_row_count);
            std::iota(selected_rows.begin(), selected_rows.end(), 0);
        }

        auto source_reads_signal_column = read_batch.signal_column();

        ReadSignalBatch read_signal;
        read_signal.data.reserve(selected_rows.size());
        read_signal.signal_type = dest_signal_type;

        auto const input_signal_type = source_file->signal_type();

        // Loop for each read in the batch:
        for (auto const batch_row : selected_rows) {
            // Get the signal row data for the read:
            auto const signal_rows = std::static_pointer_cast<arrow::UInt64Array>(
                    source_reads_signal_column->value_slice(batch_row));
            auto const signal_rows_span =
                    gsl::make_span(signal_rows->raw_values(), signal_rows->length());

            ReadSignalBatch::ReadSignal row_signal;

            // If were using the same compression type in both files, just copy compressed:
            if (input_signal_type == dest_signal_type) {
                ARROW_ASSIGN_OR_RAISE(row_signal.signal_data,
                                      source_file->extract_samples_inplace(
                                              signal_rows_span, row_signal.sample_counts));
            } else {
                // Find the sample count of the complete read:
                ARROW_ASSIGN_OR_RAISE(auto sample_count,
                                      source_file->extract_sample_count(signal_rows_span));
                row_signal.sample_counts.push_back(sample_count);

                // Read the samples:
                ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> buffer,
                                      arrow::AllocateBuffer(sample_count * sizeof(std::int16_t)));
                row_signal.signal_data.push_back(buffer);

                auto signal_buffer_span = gsl::make_span(buffer->mutable_data(), buffer->size())
                                                  .as_span<std::int16_t>();
                ARROW_RETURN_NOT_OK(
                        source_file->extract_samples(signal_rows_span, signal_buffer_span));
            }

            read_signal.preload_sum += cache_signal(row_signal.signal_data);
            read_signal.data.emplace_back(std::move(row_signal));
        }

        return std::make_shared<Pod5ReadBatch>(std::move(read_batch), source_file,
                                               std::move(read_signal));
    }

    std::int64_t cache_signal(std::vector<std::shared_ptr<arrow::Buffer>> const& row) {
        POD5_TRACE_FUNCTION();
        std::size_t step_size = 512;
        std::int64_t sum = 0;
        for (auto const& section : row) {
            for (std::size_t i = 0; i < section->size(); i += step_size) {
                sum += section->data()[i];
            }
        }
        return sum;
    }

    void set_error(arrow::Status const& error) {
        m_error = error;
        m_has_error = true;
    }

    std::size_t m_target_pending_writes;
    std::atomic<std::size_t> m_current_pending_reads;

    std::atomic<bool> m_has_error;
    boost::synchronized_value<arrow::Status> m_error;

    std::atomic<std::size_t> m_batches_requested;
    std::atomic<std::size_t> m_batches_completed;

    std::atomic<bool> m_has_pending_write;

    boost::asio::io_context m_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> m_work;
    std::vector<std::thread> m_workers;

    std::vector<std::shared_ptr<Pod5RepackerOutput>> m_outputs;

    boost::synchronized_value<std::deque<AddReadBatchToOutput>> m_pending_batch_reads;
};