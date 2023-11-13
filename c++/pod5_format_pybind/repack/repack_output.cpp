#include "repack_output.h"

#include "pod5_format/internal/tracing/tracing.h"
#include "repack_functions.h"

#include <iostream>
#include <thread>
#include <unordered_set>

namespace repack {

namespace {
struct is_not_nullptr : boost::static_visitor<bool> {
    template <typename T>
    bool operator()(T const & t) const
    {
        return t != nullptr;
    }
};

#if 0
struct get_name : boost::static_visitor<std::string>{
    template <typename T>
    std::string operator()(T const& t) const {
        return typeid(typename T::element_type).name();
    }
};

template <typename T>
void dump_queued_items(T const& queued) {
    std::map<std::string, std::size_t> items;

    for (auto const& item : queued) {
        items[boost::apply_visitor(get_name{}, item)] += 1;
    }

    std::cout << "Queued items:\n";
    for (auto pr : items) {
        std::cout << "  " << pr.first << ": " << pr.second << "\n";
    }
}
#endif

}  // namespace

struct Pod5RepackerOutputThreadState {
    Pod5RepackerOutputThreadState(std::shared_ptr<ReadsTableDictionaryManager> const & dict_manager)
    : dict_cache(dict_manager)
    {
    }

    ReadsTableDictionaryThreadCache dict_cache;
};

struct Pod5RepackerOutputState {
    Pod5RepackerOutputState(
        std::shared_ptr<pod5::FileWriter> const & _output_file,
        bool _check_duplicate_read_ids,
        arrow::MemoryPool * _memory_pool)
    : output_file(_output_file)
    , check_duplicate_read_ids(_check_duplicate_read_ids)
    , memory_pool(_memory_pool)
    , dict_manager(
          std::make_shared<ReadsTableDictionaryManager>(_output_file, read_table_writer_mutex))
    {
    }

    Pod5RepackerOutputThreadState * get_thread_state()
    {
        auto ts = thread_states.synchronize();
        auto it = ts->find(std::this_thread::get_id());
        if (it == ts->end()) {
            it = ts->emplace(std::this_thread::get_id(), dict_manager).first;
        }
        return &it->second;
    }

    std::shared_ptr<pod5::FileWriter> output_file;
    bool check_duplicate_read_ids;
    arrow::MemoryPool * memory_pool;
    std::mutex read_table_writer_mutex;
    std::mutex signal_table_writer_mutex;
    std::shared_ptr<ReadsTableDictionaryManager> dict_manager;
    boost::synchronized_value<std::shared_ptr<states::read_split_signal_table_batch_rows>>
        partial_signal_batch;
    std::atomic<std::size_t> reads_completed{0};

    boost::synchronized_value<std::unordered_map<std::thread::id, Pod5RepackerOutputThreadState>>
        thread_states;

    boost::synchronized_value<std::unordered_set<boost::uuids::uuid>> output_read_ids;
};

namespace {

struct StateProgressResult {
    StateProgressResult() = default;

    StateProgressResult(std::vector<states::shared_variant> && _new_states)
    : new_states(_new_states)
    {
    }

    std::vector<states::shared_variant> new_states;
};

struct StateOperator : boost::static_visitor<arrow::Result<StateProgressResult>> {
    StateOperator(Pod5RepackerOutputState * _progress_state) : progress_state(_progress_state) {}

    arrow::Result<StateProgressResult> operator()(
        std::shared_ptr<states::unread_read_table_rows> & batch) const
    {
        POD5_TRACE_FUNCTION();

        // Read out the read table data from the source file
        ARROW_ASSIGN_OR_RAISE(
            auto read_result,
            read_read_data(progress_state->get_thread_state()->dict_cache, std::move(*batch)));
        batch.reset();

        auto read_table_rows = std::make_shared<states::read_read_table_rows_no_signal>();
        read_table_rows->reads = std::move(read_result.reads);
        read_table_rows->signal_durations = std::move(read_result.signal_durations);
        read_table_rows->signal_row_sizes = std::move(read_result.signal_row_sizes);
        read_table_rows->signal_row_indices.resize(read_result.signal_rows.size());

        if (progress_state->check_duplicate_read_ids) {
            auto read_ids = progress_state->output_read_ids.synchronize();
            ARROW_RETURN_NOT_OK(check_duplicate_read_ids(*read_ids, read_table_rows->reads));
        }

        // Split the read table rows into new signal table batches:
        {
            auto partial_signal_batch = progress_state->partial_signal_batch.synchronize();
            ARROW_ASSIGN_OR_RAISE(
                auto signal_request_result,
                request_signal_reads(
                    read_result.input,
                    progress_state->output_file->signal_type(),
                    progress_state->output_file->signal_table_batch_size(),
                    read_result.signal_rows_read_ids,
                    read_result.signal_rows,
                    *partial_signal_batch,
                    read_table_rows,
                    progress_state->memory_pool));

            *partial_signal_batch = signal_request_result.partial_request;
            return StateProgressResult{std::move(signal_request_result.complete_requests)};
        }
    }

    arrow::Result<StateProgressResult> operator()(
        std::shared_ptr<states::read_split_signal_table_batch_rows> & batch) const
    {
        POD5_TRACE_FUNCTION();

        ARROW_ASSIGN_OR_RAISE(auto read_signal_result, read_signal_data(*batch));

        std::pair<pod5::SignalTableRowIndex, pod5::SignalTableRowIndex> inserted_signal_rows;
        {
            std::lock_guard<std::mutex> l(progress_state->signal_table_writer_mutex);
            ARROW_ASSIGN_OR_RAISE(
                inserted_signal_rows,
                progress_state->output_file->add_signal_batch(
                    read_signal_result.row_count,
                    std::move(read_signal_result.columns),
                    read_signal_result.final_batch));
        }

        std::vector<states::shared_variant> result_new_states;

        for (std::size_t i = 0; i < batch->patch_rows.size(); ++i) {
            auto const & row = batch->patch_rows[i];

            auto const & dest_read_table = row.dest_read_table;
            assert(dest_read_table);
            assert(row.dest_batch_row_index < dest_read_table->signal_row_indices.size());
            dest_read_table->signal_row_indices[row.dest_batch_row_index] =
                inserted_signal_rows.first + i;
            dest_read_table->written_row_indices += 1;

            // Check if this read table is completed!
            if (dest_read_table->written_row_indices > dest_read_table->signal_row_indices.size()) {
                assert(false);
            }
            if (dest_read_table->written_row_indices == dest_read_table->signal_row_indices.size())
            {
                result_new_states.push_back(dest_read_table);
            }
        }

        return StateProgressResult{std::move(result_new_states)};
    }

    arrow::Result<StateProgressResult> operator()(
        std::shared_ptr<states::read_read_table_rows_no_signal> & batch) const
    {
        POD5_TRACE_FUNCTION();
        assert(batch->written_row_indices == batch->signal_row_indices.size());

        std::lock_guard<std::mutex> l(progress_state->read_table_writer_mutex);
        ARROW_RETURN_NOT_OK(write_reads(
            progress_state->output_file,
            batch->reads,
            batch->signal_durations,
            batch->signal_row_sizes,
            batch->signal_row_indices));
        progress_state->reads_completed += batch->reads.size();

        return StateProgressResult{{}};
    }

    arrow::Result<StateProgressResult> operator()(std::shared_ptr<states::finished> & batch) const
    {
        POD5_TRACE_FUNCTION();

        std::vector<states::shared_variant> final_states;
        // No further reads expected, flush all partial state:
        auto partial_signal_batch = progress_state->partial_signal_batch.synchronize();
        if (*partial_signal_batch) {
            (*partial_signal_batch)->final_batch = true;

            final_states.emplace_back(std::move(*partial_signal_batch));
            partial_signal_batch->reset();
        }

        return StateProgressResult{std::move(final_states)};
    }

    Pod5RepackerOutputState * progress_state;
};

}  // namespace

Pod5RepackerOutput::Pod5RepackerOutput(
    std::shared_ptr<Pod5Repacker> const & repacker,
    boost::asio::io_context & context,
    std::shared_ptr<pod5::FileWriter> const & output,
    bool check_duplicate_read_ids)
: m_repacker(repacker)
, m_context(context)
, m_output(output)
, m_progress_state(std::make_unique<Pod5RepackerOutputState>(
      output,
      check_duplicate_read_ids,
      arrow::default_memory_pool()))
{
}

Pod5RepackerOutput::~Pod5RepackerOutput() {}

bool Pod5RepackerOutput::has_tasks() const
{
    return m_in_flight > 0 || m_active_read_table_states->size() > 0;
}

void Pod5RepackerOutput::set_finished()
{
    if (!m_finished) {
        // Wait for all other tasks to flush through the output.
        while (!m_has_error) {
            if (!has_tasks()) {
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        m_active_read_table_states->emplace_front(std::make_shared<states::finished>());
        post_try_work();

        m_finished = true;
    }
}

bool Pod5RepackerOutput::is_complete() const
{
    return m_finished && m_active_read_table_states->empty();
}

std::size_t Pod5RepackerOutput::reads_completed() const
{
    return m_progress_state->reads_completed;
}

void Pod5RepackerOutput::register_new_reads(
    std::shared_ptr<pod5::FileReader> const & input,
    std::size_t batch_index,
    std::vector<std::uint32_t> && batch_rows)
{
    if (m_finished) {
        throw std::runtime_error("Failed to add reads to finished output");
    }

    m_active_read_table_states->emplace_front(std::make_shared<states::unread_read_table_rows>(
        input, batch_index, std::move(batch_rows)));

    post_try_work();
}

void Pod5RepackerOutput::post_try_work()
{
    m_context.post([&]() {
        POD5_TRACE_FUNCTION();

        auto get_next_work = [](auto & synced_states) -> states::shared_variant {
            if (synced_states->empty()) {
                return {};
            }

            auto work = synced_states->back();
            synced_states->pop_back();
            return work;
        };

        StateOperator state_operator{m_progress_state.get()};

        states::shared_variant next_work;
        while (!m_has_error) {
            m_in_flight += 1;
            // Its important we don't release this until any new states
            // are in `m_active_read_table_states`
            auto remove_in_flight = gsl::finally([&] { m_in_flight -= 1; });

            if (!boost::apply_visitor(is_not_nullptr{}, next_work)) {
                auto states = m_active_read_table_states.synchronize();
                next_work = get_next_work(states);
                if (!boost::apply_visitor(is_not_nullptr{}, next_work)) {
                    return;
                }
            }

            auto result = boost::apply_visitor(state_operator, next_work);
            if (!result.ok()) {
                set_error(result.status());
                return;
            }
            next_work = {};

            {
                auto states = m_active_read_table_states.synchronize();
                states->insert(states->end(), result->new_states.begin(), result->new_states.end());

                next_work = get_next_work(states);
            }
        }
    });
}

}  // namespace repack
