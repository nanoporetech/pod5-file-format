#pragma once

#include "pod5_format/file_reader.h"
#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_table_reader.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <boost/thread/synchronized_value.hpp>

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

namespace pod5 {

class POD5_FORMAT_EXPORT CachedBatchSignalData {
public:
    CachedBatchSignalData(std::uint32_t batch_index, std::size_t entry_count)
    : m_batch_index(batch_index)
    , m_sample_counts(entry_count)
    , m_samples(entry_count)
    {
    }

    std::uint32_t batch_index() const { return m_batch_index; }

    /// Find a list of sample counts for all requested batch rows.
    std::vector<std::uint64_t> const & sample_count() const { return m_sample_counts; }

    /// Find a list of signal samples counts for all requested batch rows.
    std::vector<std::vector<std::int16_t>> const & samples() const { return m_samples; }

    void
    set_samples(std::size_t row, std::uint64_t sample_count, std::vector<std::int16_t> && samples)
    {
        m_sample_counts[row] = sample_count;
        m_samples[row] = std::move(samples);
    }

private:
    std::uint32_t m_batch_index;
    std::vector<std::uint64_t> m_sample_counts;
    std::vector<std::vector<std::int16_t>> m_samples;
};

class POD5_FORMAT_EXPORT SignalCacheWorkPackage {
public:
    SignalCacheWorkPackage(
        std::uint32_t batch_index,
        std::size_t job_row_count,
        gsl::span<std::uint32_t const> const & specific_job_rows,
        pod5::ReadTableRecordBatch && read_batch)
    : m_job_row_count(job_row_count)
    , m_specific_job_rows(specific_job_rows)
    , m_next_row_to_start(0)
    , m_completed_rows(0)
    , m_cached_data(std::make_unique<CachedBatchSignalData>(batch_index, m_job_row_count))
    , m_read_batch(std::move(read_batch))
    {
    }

    std::uint32_t job_row_count() const { return m_job_row_count; }

    void
    set_samples(std::size_t row, std::uint64_t sample_count, std::vector<std::int16_t> && samples)
    {
        m_cached_data->set_samples(row, sample_count, std::move(samples));
    }

    std::unique_ptr<CachedBatchSignalData> release_data() { return std::move(m_cached_data); }

    pod5::ReadTableRecordBatch const & read_batch() const { return m_read_batch; }

    // Find the actual batch row to query, for a given job row index.
    std::uint32_t get_batch_row_to_query(std::uint32_t job_row_index) const
    {
        // We allow the caller to specify a subset of batch rows to iterate:
        if (!m_specific_job_rows.empty()) {
            return m_specific_job_rows[job_row_index];
        }

        return job_row_index;
    }

    std::uint32_t start_rows(std::unique_lock<std::mutex> & l, std::size_t row_count)
    {
        auto row = m_next_row_to_start;
        m_next_row_to_start += row_count;
        return row;
    }

    void complete_rows(std::uint32_t row_count) { m_completed_rows += row_count; }

    bool has_work_left() const { return m_next_row_to_start < m_job_row_count; }

    bool is_complete() const { return m_completed_rows.load() >= m_job_row_count; }

private:
    std::size_t m_job_row_count;
    gsl::span<std::uint32_t const> m_specific_job_rows;

    std::uint32_t m_next_row_to_start;
    std::atomic<std::uint32_t> m_completed_rows;

    std::unique_ptr<CachedBatchSignalData> m_cached_data;
    pod5::ReadTableRecordBatch m_read_batch;
};

class POD5_FORMAT_EXPORT AsyncSignalLoader {
public:
    // Minimum number of tasks one thread will do in a batch.
    static const std::size_t MINIMUM_JOB_SIZE;
    enum class SamplesMode {
        NoSamples,
        Samples,
    };

    AsyncSignalLoader(
        std::shared_ptr<pod5::FileReader> const & reader,
        SamplesMode samples_mode,
        gsl::span<std::uint32_t const> const & batch_counts,
        gsl::span<std::uint32_t const> const & batch_rows,
        std::size_t worker_count = std::thread::hardware_concurrency(),
        std::size_t max_pending_batches = 10);

    ~AsyncSignalLoader();

    /// Find if all work is complete in the loader.
    bool is_finished() const { return m_finished; }

    /// Get the next batch of loaded signal, always returns the consecutive next signal batch
    /// \note Returns nullptr when timeoout occurs, or if all data is exhausted.
    Result<std::unique_ptr<CachedBatchSignalData>> release_next_batch(
        boost::optional<std::chrono::steady_clock::time_point> timeout = boost::none);

private:
    /// Set an error code that will stop all async loading and return an error to the caller.
    void set_error(pod5::Status status);

    void run_worker();
    void do_work(
        std::shared_ptr<SignalCacheWorkPackage> const & batch,
        std::uint32_t row_start,
        std::uint32_t row_end);

    /// Setup a new batch for in progress work to contain.
    /// \param lock A lock held on m_worker_sync.
    /// \note There must not be a batch already in progress.
    /// \note m_current_batch is used as the index of the next batch to begin.
    Status setup_next_in_progress_batch(std::unique_lock<std::mutex> & lock);

    /// Release the currently in progress batch to readers, if it exists.
    /// \note This call locks m_batches_sync internally.
    /// \note The batch must not have any work remaining to start, but can be completing already started work.
    /// \note This call notifys the condition variable to alert readers that new data is available.
    void release_in_progress_batch();

    std::shared_ptr<pod5::FileReader> m_reader;
    SamplesMode m_samples_mode;
    std::size_t m_max_pending_batches;
    std::size_t m_reads_batch_count;
    gsl::span<std::uint32_t const> m_batch_counts;
    std::size_t m_total_batch_count_so_far;
    gsl::span<std::uint32_t const> m_batch_rows;

    std::uint32_t const m_worker_job_size;

    std::mutex m_worker_sync;
    std::condition_variable m_batch_done;
    std::uint32_t m_current_batch;

    std::atomic<bool> m_finished;
    std::atomic<bool> m_has_error;
    boost::synchronized_value<pod5::Status> m_error;
    std::shared_ptr<SignalCacheWorkPackage> m_in_progress_batch;

    std::mutex m_batches_sync;
    std::atomic<std::uint32_t> m_batches_size;
    std::deque<std::shared_ptr<SignalCacheWorkPackage>> m_batches;

    std::vector<std::thread> m_workers;
};

}  // namespace pod5
