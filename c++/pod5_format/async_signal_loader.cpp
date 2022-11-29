#include "pod5_format/async_signal_loader.h"

namespace pod5 {

const std::size_t AsyncSignalLoader::MINIMUM_JOB_SIZE = 50;

AsyncSignalLoader::AsyncSignalLoader(
    std::shared_ptr<pod5::FileReader> const & reader,
    SamplesMode samples_mode,
    gsl::span<std::uint32_t const> const & batch_counts,
    gsl::span<std::uint32_t const> const & batch_rows,
    std::size_t worker_count,
    std::size_t max_pending_batches)
: m_reader(reader)
, m_samples_mode(samples_mode)
, m_max_pending_batches(max_pending_batches)
, m_reads_batch_count(m_reader->num_read_record_batches())
, m_batch_counts(batch_counts)
, m_total_batch_count_so_far(0)
, m_batch_rows(batch_rows)
, m_worker_job_size(std::max<std::size_t>(
      MINIMUM_JOB_SIZE,
      m_batch_rows.size() / (m_reads_batch_count * worker_count * 2)))
, m_current_batch(0)
, m_finished(false)
, m_has_error(false)
, m_batches_size(0)
{
    // Setup first batch:
    {
        std::unique_lock<std::mutex> l(m_worker_sync);
        auto setup_result = setup_next_in_progress_batch(l);
        if (!setup_result.ok()) {
            set_error(setup_result);
        }
    }

    // Kick off workers on jobs:
    for (std::size_t i = 0; i < worker_count; ++i) {
        m_workers.emplace_back([&] { run_worker(); });
    }
}

AsyncSignalLoader::~AsyncSignalLoader()
{
    m_finished = true;
    // Wait for all workers to complete:
    for (std::size_t i = 0; i < m_workers.size(); ++i) {
        m_workers[i].join();
    }
}

Result<std::unique_ptr<CachedBatchSignalData>> AsyncSignalLoader::release_next_batch(
    boost::optional<std::chrono::steady_clock::time_point> timeout)
{
    std::shared_ptr<SignalCacheWorkPackage> batch;

    // Return any error, if one has occurred:
    if (m_has_error) {
        return Status(*m_error);
    }

    // First wait until there is a batch available:
    do {
        std::unique_lock<std::mutex> l(m_batches_sync);
        // Wait until there is a batch available:
        m_batch_done.wait_until(
            l,
            timeout.get_value_or(std::chrono::steady_clock::now() + std::chrono::seconds(5)),
            [&] { return m_batches.size() || m_finished || m_has_error; });

        // Grab a batch if one exists (note error or user destroying us might have happened instead):
        if (!m_batches.empty()) {
            batch = std::move(m_batches.front());
            assert(batch);
            m_batches.pop_front();
            m_batches_size -= 1;
            break;
        }

        if (timeout && std::chrono::steady_clock::now() > *timeout) {
            return nullptr;
        }
    } while (!m_finished && !m_has_error);

    // Return any error, if one has occurred during our wait:
    if (m_has_error) {
        return Status(*m_error);
    }

    // If we got a batch, wait for all work to be finished, then return it:
    if (batch) {
        // Wait if we are ahead of the loader:
        while (!batch->is_complete()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        return batch->release_data();
    }

    // No more data - return null.
    return nullptr;
}

void AsyncSignalLoader::set_error(pod5::Status status)
{
    assert(!status.ok());
    m_error = status;
    m_has_error = true;
}

void AsyncSignalLoader::run_worker()
{
    // Continue to work while there is work to do, and no error has occurred
    while (!m_finished && !m_has_error) {
        std::shared_ptr<SignalCacheWorkPackage> batch;
        std::uint32_t row_start = 0;

        // Try to secure some new work:
        {
            std::unique_lock<std::mutex> l(m_worker_sync);
            // If we have run out of batches to process, release anything in progress and return:
            if (m_current_batch >= m_reads_batch_count) {
                release_in_progress_batch();
                break;
            }

            // If we have more batches than asked for complete that have
            // not been queried, wait for it to get taken:
            if (m_batches_size > m_max_pending_batches) {
                l.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            // Now, if we have no work left in the current batch, release that:
            if (!m_in_progress_batch->has_work_left()) {
                if (!m_batch_counts.empty()) {
                    m_total_batch_count_so_far += m_batch_counts[m_current_batch];
                }

                // Release the current batch:
                release_in_progress_batch();

                // Then try to setup the next batch, if one exists:
                m_current_batch += 1;
                if (m_current_batch >= m_reads_batch_count) {
                    // No more work to do.
                    m_finished = true;
                    break;
                }

                auto setup_result = setup_next_in_progress_batch(l);
                if (!setup_result.ok()) {
                    set_error(setup_result);
                    return;
                }
            }

            // Finally, tell the work package we have secured we are starting to do some work:
            batch = m_in_progress_batch;
            row_start = m_in_progress_batch->start_rows(l, m_worker_job_size);
        }

        // Now execute the work, for all the rows we said we would:
        std::uint32_t const row_end =
            std::min(row_start + m_worker_job_size, batch->job_row_count());

        do_work(batch, row_start, row_end);

        // And report the work completed for anyone waiting:
        batch->complete_rows(m_worker_job_size);
    }
}

void AsyncSignalLoader::do_work(
    std::shared_ptr<SignalCacheWorkPackage> const & batch,
    std::uint32_t row_start,
    std::uint32_t row_end)
{
    // First secure the sample counts column for the batch we are processing:
    auto signal_column = batch->read_batch().signal_column();

    // And record where we are starting in the batch rows array, if it exists:
    for (std::uint32_t i = row_start; i < row_end; ++i) {
        // Find the actual batch row to query - we may be working on a subset of batch data:
        auto const actual_batch_row = batch->get_batch_row_to_query(i);
        // Get the signal row data for the read:
        auto const signal_rows = std::static_pointer_cast<arrow::UInt64Array>(
            signal_column->value_slice(actual_batch_row));
        auto const signal_rows_span =
            gsl::make_span(signal_rows->raw_values(), signal_rows->length());

        // Find the sample count for these rows:
        auto sample_count_result = m_reader->extract_sample_count(signal_rows_span);
        if (!sample_count_result.ok()) {
            m_error = sample_count_result.status();
            m_has_error = true;
            return;
        }
        std::uint64_t sample_count = *sample_count_result;

        // And query the samples if that has been requested:
        std::vector<std::int16_t> samples;
        if (m_samples_mode == SamplesMode::Samples) {
            samples.resize(sample_count);
            auto samples_result =
                m_reader->extract_samples(signal_rows_span, gsl::make_span(samples));
            if (!samples_result.ok()) {
                m_error = samples_result;
                m_has_error = true;
                return;
            }
            sample_count = samples.size();
        }

        // Store the queried data into the batch:
        batch->set_samples(i, sample_count, std::move(samples));
    }
}

Status AsyncSignalLoader::setup_next_in_progress_batch(std::unique_lock<std::mutex> & lock)
{
    assert(!m_in_progress_batch);
    ARROW_ASSIGN_OR_RAISE(auto read_batch, m_reader->read_read_record_batch(m_current_batch));
    std::size_t row_count = read_batch.num_rows();

    gsl::span<std::uint32_t const> next_specific_batch_rows;
    if (!m_batch_counts.empty()) {
        row_count = m_batch_counts[m_current_batch];
        if (!m_batch_rows.empty()) {
            next_specific_batch_rows = m_batch_rows.subspan(m_total_batch_count_so_far, row_count);
        }
    }

    m_in_progress_batch = std::make_shared<SignalCacheWorkPackage>(
        m_current_batch, row_count, next_specific_batch_rows, std::move(read_batch));
    return Status::OK();
}

void AsyncSignalLoader::release_in_progress_batch()
{
    if (m_in_progress_batch) {
        assert(!m_in_progress_batch->has_work_left());
        std::lock_guard<std::mutex> l(m_batches_sync);
        m_batches.emplace_back(std::move(m_in_progress_batch));
        m_batches_size += 1;
        m_batch_done.notify_all();
    }
}

}  // namespace pod5
