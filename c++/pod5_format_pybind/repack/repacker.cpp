#include "repacker.h"

#include "pod5_format/internal/tracing/tracing.h"
#include "repack_output.h"
#include "repack_states.h"

namespace repack {

namespace {

void repacker_add_reads_preconditions(
    std::shared_ptr<Pod5Repacker> const & repacker,
    std::shared_ptr<Pod5RepackerOutput> const & output,
    Pod5FileReaderPtr const & input)
{
    if (output->repacker() != repacker) {
        throw std::runtime_error("Invalid repacker output passed, created by another repacker");
    }

    if (!input.reader) {
        throw std::runtime_error("Invalid input passed to repacker, no reader");
    }
}

}  // namespace

Pod5Repacker::Pod5Repacker() : m_work(boost::asio::make_work_guard(m_context))
{
    std::size_t worker_count = 10;
    m_workers.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        m_workers.emplace_back([&] { m_context.run(); });
    }
}

Pod5Repacker::~Pod5Repacker() { finish(); }

void Pod5Repacker::finish()
{
    POD5_TRACE_FUNCTION();
    for (auto & output : m_outputs) {
        output->set_finished();
    }

    check_for_error();

    m_work.reset();
    for (auto & worker : m_workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    for (auto & output : m_outputs) {
        m_reads_complete_deleted_outputs += output->reads_completed();
    }
    m_outputs.clear();
}

std::shared_ptr<Pod5RepackerOutput> Pod5Repacker::add_output(
    std::shared_ptr<pod5::FileWriter> const & output,
    bool check_duplicate_read_ids)
{
    POD5_TRACE_FUNCTION();
    auto repacker_output = std::make_shared<Pod5RepackerOutput>(
        shared_from_this(), m_context, output, check_duplicate_read_ids);
    m_outputs.push_back(repacker_output);
    return repacker_output;
}

void Pod5Repacker::set_output_finished(std::shared_ptr<Pod5RepackerOutput> const & output)
{
    if (output->repacker() != shared_from_this()) {
        throw std::runtime_error("Invalid repacker output passed, created by another repacker");
    }

    output->set_finished();
}

void Pod5Repacker::add_all_reads_to_output(
    std::shared_ptr<Pod5RepackerOutput> const & output,
    Pod5FileReaderPtr const & input)
{
    POD5_TRACE_FUNCTION();
    repacker_add_reads_preconditions(shared_from_this(), output, input);

    for (std::size_t i = 0; i < input.reader->num_read_record_batches(); ++i) {
        output->register_new_reads(input.reader, i);
    }

    register_submitted_reader(input);
}

void Pod5Repacker::add_selected_reads_to_output(
    std::shared_ptr<Pod5RepackerOutput> const & output,
    Pod5FileReaderPtr const & input,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batch_counts,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && all_batch_rows)
{
    POD5_TRACE_FUNCTION();
    repacker_add_reads_preconditions(shared_from_this(), output, input);

    auto batch_counts_span = gsl::make_span(batch_counts.data(), batch_counts.size());
    auto all_batch_rows_span = gsl::make_span(all_batch_rows.data(), all_batch_rows.size());

    std::size_t current_start_point = 0;
    for (std::size_t i = 0; i < input.reader->num_read_record_batches(); ++i) {
        std::vector<std::uint32_t> batch_rows;
        auto const batch_rows_span =
            all_batch_rows_span.subspan(current_start_point, batch_counts_span[i]);

        // If this batch has no selected
        if (batch_rows_span.empty()) {
            continue;
        }

        batch_rows.insert(batch_rows.end(), batch_rows_span.begin(), batch_rows_span.end());
        current_start_point += batch_counts_span[i];

        output->register_new_reads(input.reader, i, std::move(batch_rows));
    }

    register_submitted_reader(input);
}

void Pod5Repacker::check_for_error() const
{
    for (auto const & output : m_outputs) {
        if (output->has_error()) {
            throw std::runtime_error(output->error().ToString());
        }
    }
}

bool Pod5Repacker::is_complete() const
{
    POD5_TRACE_FUNCTION();
    check_for_error();

    for (auto const & output : m_outputs) {
        if (!output->is_complete()) {
            return false;
        }
    }

    return true;
}

std::size_t Pod5Repacker::reads_completed() const
{
    POD5_TRACE_FUNCTION();
    check_for_error();

    std::size_t reads_complete = 0;
    for (auto const & output : m_outputs) {
        reads_complete += output->reads_completed();
    }

    return reads_complete + m_reads_complete_deleted_outputs;
}

}  // namespace repack
