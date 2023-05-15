#pragma once

#include "pod5_format/async_signal_loader.h"
#include "pod5_format/c_api.h"
#include "pod5_format/file_reader.h"
#include "pod5_format/file_updater.h"
#include "pod5_format/file_writer.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_compression.h"
#include "pod5_format/signal_table_reader.h"
#include "pod5_format/thread_pool.h"
#include "utils.h"

#include <arrow/memory_pool.h>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

inline std::shared_ptr<pod5::FileWriter> create_file(
    char const * path,
    std::string const & writer_name,
    pod5::FileWriterOptions const * options)
{
    pod5::FileWriterOptions dummy;
    POD5_PYTHON_ASSIGN_OR_RAISE(
        auto writer,
        pod5::create_file_writer(
            path, writer_name, options ? *options : pod5::FileWriterOptions{}));
    return writer;
}

inline std::shared_ptr<pod5::FileWriter> recover_file(
    char const * src_filename,
    char const * dest_filename)
{
    POD5_PYTHON_ASSIGN_OR_RAISE(
        auto writer, pod5::recover_file_writer(src_filename, dest_filename));
    return writer;
}

class Pod5SignalCacheBatch {
public:
    Pod5SignalCacheBatch(
        pod5::AsyncSignalLoader::SamplesMode samples_mode,
        pod5::CachedBatchSignalData && cached_data)
    : m_samples_mode(samples_mode)
    , m_cached_data(std::move(cached_data))
    {
    }

    py::array_t<std::uint64_t> sample_count() const
    {
        return py::array_t<std::uint64_t>(
            m_cached_data.sample_count().size(), m_cached_data.sample_count().data());
    }

    py::list samples() const
    {
        py::list py_samples;
        if (m_samples_mode != pod5::AsyncSignalLoader::SamplesMode::Samples) {
            return py_samples;
        }
        for (auto const & row_samples : m_cached_data.samples()) {
            py_samples.append(py::array_t<std::int16_t>(row_samples.size(), row_samples.data()));
        }

        return py_samples;
    }

    std::uint32_t batch_index() const { return m_cached_data.batch_index(); }

private:
    pod5::AsyncSignalLoader::SamplesMode m_samples_mode;
    pod5::CachedBatchSignalData m_cached_data;
};

class Pod5AsyncSignalLoader {
public:
    // Make an async loader for all reads in the file
    Pod5AsyncSignalLoader(
        std::shared_ptr<pod5::FileReader> const & reader,
        pod5::AsyncSignalLoader::SamplesMode samples_mode,
        std::size_t worker_count = std::thread::hardware_concurrency(),
        std::size_t max_pending_batches = 10)
    : m_samples_mode(samples_mode)
    , m_batch_counts_ref({})
    , m_batch_rows_ref({})
    , m_async_loader(reader, samples_mode, {}, {}, worker_count, max_pending_batches)
    {
    }

    // Make an async loader for specific batches
    Pod5AsyncSignalLoader(
        std::shared_ptr<pod5::FileReader> const & reader,
        pod5::AsyncSignalLoader::SamplesMode samples_mode,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batches,
        std::size_t worker_count = std::thread::hardware_concurrency(),
        std::size_t max_pending_batches = 10)
    : m_samples_mode(samples_mode)
    , m_batch_sizes(make_batch_counts(reader, batches))
    , m_async_loader(
          reader,
          samples_mode,
          gsl::make_span(m_batch_sizes),
          {},
          worker_count,
          max_pending_batches)
    {
    }

    // Make an async loader for specific reads in specific batches
    Pod5AsyncSignalLoader(
        std::shared_ptr<pod5::FileReader> const & reader,
        pod5::AsyncSignalLoader::SamplesMode samples_mode,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batch_counts,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batch_rows,
        std::size_t worker_count = std::thread::hardware_concurrency(),
        std::size_t max_pending_batches = 10)
    : m_samples_mode(samples_mode)
    , m_batch_counts_ref(std::move(batch_counts))
    , m_batch_rows_ref(std::move(batch_rows))
    , m_async_loader(
          reader,
          samples_mode,
          gsl::make_span(m_batch_counts_ref.data(), m_batch_counts_ref.size()),
          gsl::make_span(m_batch_rows_ref.data(), m_batch_rows_ref.size()),
          worker_count,
          max_pending_batches)
    {
    }

    std::shared_ptr<Pod5SignalCacheBatch> release_next_batch()
    {
        auto batch = m_async_loader.release_next_batch();
        if (!batch.ok()) {
            throw std::runtime_error(batch.status().ToString());
        }

        if (!*batch) {
            assert(m_async_loader.is_finished());
            throw pybind11::stop_iteration();
        }

        return std::make_shared<Pod5SignalCacheBatch>(m_samples_mode, std::move(**batch));
    }

    std::vector<std::uint32_t> make_batch_counts(
        std::shared_ptr<pod5::FileReader> const & reader,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const & batches)
    {
        std::vector<std::uint32_t> batch_counts(reader->num_read_record_batches(), 0);
        for (auto const & batch_idx : gsl::make_span(batches.data(), batches.shape(0))) {
            auto read_batch = reader->read_read_record_batch(batch_idx);
            if (!read_batch.ok()) {
                throw std::runtime_error(
                    "Failed to query read batch count: " + read_batch.status().ToString());
            }

            batch_counts[batch_idx] = read_batch->num_rows();
        }
        return batch_counts;
    }

    pod5::AsyncSignalLoader::SamplesMode m_samples_mode;
    std::vector<std::uint32_t> m_batch_sizes;
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> m_batch_counts_ref;
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> m_batch_rows_ref;
    pod5::AsyncSignalLoader m_async_loader;
};

struct Pod5FileReaderPtr {
    std::shared_ptr<pod5::FileReader> reader = nullptr;

    Pod5FileReaderPtr(std::shared_ptr<pod5::FileReader> && reader_) : reader(std::move(reader_)) {}

    pod5::FileLocation get_file_run_info_table_location() const
    {
        return reader->run_info_table_location();
    }

    pod5::FileLocation get_file_read_table_location() const
    {
        return reader->read_table_location();
    }

    pod5::FileLocation get_file_signal_table_location() const
    {
        return reader->signal_table_location();
    }

    std::string get_file_version_pre_migration() const
    {
        return reader->file_version_pre_migration().to_string();
    }

    void close() { reader = nullptr; }

    std::size_t plan_traversal(
        py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const & read_id_data,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> & batch_counts,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> & batch_rows)
    {
        auto const read_id_count = read_id_data.shape(0);
        auto search_input = pod5::ReadIdSearchInput(gsl::make_span(
            reinterpret_cast<boost::uuids::uuid const *>(read_id_data.data()), read_id_count));

        POD5_PYTHON_ASSIGN_OR_RAISE(
            auto find_success_count,
            reader->search_for_read_ids(
                search_input,
                gsl::make_span(batch_counts.mutable_data(), reader->num_read_record_batches()),
                gsl::make_span(batch_rows.mutable_data(), read_id_count)));

        return find_success_count;
    }

    std::shared_ptr<Pod5AsyncSignalLoader> batch_get_signal(bool get_samples, bool get_sample_count)
    {
        return std::make_shared<Pod5AsyncSignalLoader>(
            reader,
            get_samples ? pod5::AsyncSignalLoader::SamplesMode::Samples
                        : pod5::AsyncSignalLoader::SamplesMode::NoSamples);
    }

    std::shared_ptr<Pod5AsyncSignalLoader> batch_get_signal_batches(
        bool get_samples,
        bool get_sample_count,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batches)
    {
        return std::make_shared<Pod5AsyncSignalLoader>(
            reader,
            get_samples ? pod5::AsyncSignalLoader::SamplesMode::Samples
                        : pod5::AsyncSignalLoader::SamplesMode::NoSamples,
            std::move(batches));
    }

    std::shared_ptr<Pod5AsyncSignalLoader> batch_get_signal_selection(
        bool get_samples,
        bool get_sample_count,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batch_counts,
        py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> && batch_rows)
    {
        return std::make_shared<Pod5AsyncSignalLoader>(
            reader,
            get_samples ? pod5::AsyncSignalLoader::SamplesMode::Samples
                        : pod5::AsyncSignalLoader::SamplesMode::NoSamples,
            std::move(batch_counts),
            std::move(batch_rows));
    }
};

inline Pod5FileReaderPtr open_file(char const * filename)
{
    POD5_PYTHON_ASSIGN_OR_RAISE(auto reader, pod5::open_file_reader(filename, {}));
    return Pod5FileReaderPtr(std::move(reader));
}

inline void write_updated_file_to_dest(Pod5FileReaderPtr source, char const * dest_filename)
{
    POD5_PYTHON_RETURN_NOT_OK(
        pod5::update_file(arrow::default_memory_pool(), source.reader, dest_filename));
}

inline pod5::RunInfoDictionaryIndex FileWriter_add_run_info(
    pod5::FileWriter & w,
    std::string & acquisition_id,
    std::int64_t acquisition_start_time,
    std::int16_t adc_max,
    std::int16_t adc_min,
    std::vector<std::pair<std::string, std::string>> && context_tags,
    std::string & experiment_name,
    std::string & flow_cell_id,
    std::string & flow_cell_product_code,
    std::string & protocol_name,
    std::string & protocol_run_id,
    std::int64_t protocol_start_time,
    std::string & sample_id,
    std::uint16_t sample_rate,
    std::string & sequencing_kit,
    std::string & sequencer_position,
    std::string & sequencer_position_type,
    std::string & software,
    std::string & system_name,
    std::string & system_type,
    std::vector<std::pair<std::string, std::string>> && tracking_id)
{
    return throw_on_error(w.add_run_info(
        {std::move(acquisition_id),
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

inline pod5::ReadData make_read_data(
    std::size_t row_id,
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const & read_id_data,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const & read_numbers,
    py::array_t<std::uint64_t, py::array::c_style | py::array::forcecast> const & start_samples,
    py::array_t<std::uint16_t, py::array::c_style | py::array::forcecast> const & channels,
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const & wells,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & pore_types,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & calibration_offsets,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & calibration_scales,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & median_befores,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & end_reasons,
    py::array_t<bool, py::array::c_style | py::array::forcecast> const & end_reason_forceds,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & run_infos,
    py::array_t<std::uint64_t, py::array::c_style | py::array::forcecast> const &
        num_minknow_events,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & tracked_scaling_scale,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & tracked_scaling_shift,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & predicted_scaling_scale,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & predicted_scaling_shift,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const &
        num_reads_since_mux_change,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & time_since_mux_change)
{
    auto read_ids = reinterpret_cast<boost::uuids::uuid const *>(read_id_data.data(0));
    return pod5::ReadData{
        read_ids[row_id],
        *read_numbers.data(row_id),
        *start_samples.data(row_id),
        *channels.data(row_id),
        *wells.data(row_id),
        *pore_types.data(row_id),
        *calibration_offsets.data(row_id),
        *calibration_scales.data(row_id),
        *median_befores.data(row_id),
        *end_reasons.data(row_id),
        *end_reason_forceds.data(row_id),
        *run_infos.data(row_id),
        *num_minknow_events.data(row_id),
        *tracked_scaling_scale.data(row_id),
        *tracked_scaling_shift.data(row_id),
        *predicted_scaling_scale.data(row_id),
        *predicted_scaling_shift.data(row_id),
        *num_reads_since_mux_change.data(row_id),
        *time_since_mux_change.data(row_id)};
}

inline void FileWriter_add_reads(
    pod5::FileWriter & w,
    std::size_t count,
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const & read_id_data,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const & read_numbers,
    py::array_t<std::uint64_t, py::array::c_style | py::array::forcecast> const & start_samples,
    py::array_t<std::uint16_t, py::array::c_style | py::array::forcecast> const & channels,
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const & wells,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & pore_types,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & calibration_offsets,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & calibration_scales,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & median_befores,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & end_reasons,
    py::array_t<bool, py::array::c_style | py::array::forcecast> const & end_reason_forceds,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & run_infos,
    py::array_t<std::uint64_t, py::array::c_style | py::array::forcecast> const &
        num_minknow_events,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & tracked_scaling_scales,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & tracked_scaling_shifts,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & predicted_scaling_scales,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & predicted_scaling_shifts,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const &
        num_reads_since_mux_changes,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & time_since_mux_changes,
    py::list signal_ptrs)
{
    if (read_id_data.shape(1) != 16) {
        throw std::runtime_error("Read id array is of unexpected size");
    }

    auto signal_it = signal_ptrs.begin();
    for (std::size_t i = 0; i < count; ++i, ++signal_it) {
        if (signal_it == signal_ptrs.end()) {
            throw std::runtime_error("Missing signal data");
        }
        auto signal =
            signal_it->cast<py::array_t<std::int16_t, py::array::c_style | py::array::forcecast>>();
        auto signal_span = gsl::make_span(signal.data(), signal.size());

        auto read_data = make_read_data(
            i,
            read_id_data,
            read_numbers,
            start_samples,
            channels,
            wells,
            pore_types,
            calibration_offsets,
            calibration_scales,
            median_befores,
            end_reasons,
            end_reason_forceds,
            run_infos,
            num_minknow_events,
            tracked_scaling_scales,
            tracked_scaling_shifts,
            predicted_scaling_scales,
            predicted_scaling_shifts,
            num_reads_since_mux_changes,
            time_since_mux_changes);

        throw_on_error(w.add_complete_read(read_data, signal_span));
    }
}

inline void FileWriter_add_reads_pre_compressed(
    pod5::FileWriter & w,
    std::size_t count,
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const & read_id_data,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const & read_numbers,
    py::array_t<std::uint64_t, py::array::c_style | py::array::forcecast> const & start_samples,
    py::array_t<std::uint16_t, py::array::c_style | py::array::forcecast> const & channels,
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const & wells,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & pore_types,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & calibration_offsets,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & calibration_scales,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & median_befores,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & end_reasons,
    py::array_t<bool, py::array::c_style | py::array::forcecast> const & end_reason_forceds,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & run_infos,
    py::array_t<std::uint64_t, py::array::c_style | py::array::forcecast> const &
        num_minknow_events,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & tracked_scaling_scales,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & tracked_scaling_shifts,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & predicted_scaling_scales,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & predicted_scaling_shifts,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const &
        num_reads_since_mux_changes,
    py::array_t<float, py::array::c_style | py::array::forcecast> const & time_since_mux_changes,
    py::list compressed_signal_ptrs,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const & sample_counts,
    py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const &
        signal_chunk_counts)
{
    if (read_id_data.shape(1) != 16) {
        throw std::runtime_error("Read id array is of unexpected size");
    }

    auto read_ids = reinterpret_cast<boost::uuids::uuid const *>(read_id_data.data(0));
    auto compressed_signal_it = compressed_signal_ptrs.begin();
    auto sample_counts_it = sample_counts.data();
    for (std::size_t i = 0; i < count; ++i) {
        auto const read_id = read_ids[i];

        auto const signal_chunk_count = *signal_chunk_counts.data(i);
        std::uint64_t signal_duration_count = 0;
        std::vector<std::uint64_t> signal_rows(signal_chunk_count);
        for (std::size_t signal_chunk_idx = 0; signal_chunk_idx < signal_chunk_count;
             ++signal_chunk_idx) {
            if (compressed_signal_it == compressed_signal_ptrs.end()) {
                throw std::runtime_error("Missing signal data");
            }
            auto compressed_signal =
                compressed_signal_it
                    ->cast<py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast>>();
            auto compressed_signal_span =
                gsl::make_span(compressed_signal.data(), compressed_signal.size());

            auto signal_row = throw_on_error(
                w.add_pre_compressed_signal(read_id, compressed_signal_span, *sample_counts_it));
            signal_rows[signal_chunk_idx] = signal_row;

            signal_duration_count += *sample_counts_it;
            ++compressed_signal_it;
            ++sample_counts_it;
        }

        auto read_data = make_read_data(
            i,
            read_id_data,
            read_numbers,
            start_samples,
            channels,
            wells,
            pore_types,
            calibration_offsets,
            calibration_scales,
            median_befores,
            end_reasons,
            end_reason_forceds,
            run_infos,
            num_minknow_events,
            tracked_scaling_scales,
            tracked_scaling_shifts,
            predicted_scaling_scales,
            predicted_scaling_shifts,
            num_reads_since_mux_changes,
            time_since_mux_changes);

        throw_on_error(w.add_complete_read(read_data, signal_rows, signal_duration_count));
    }
}

inline void decompress_signal_wrapper(
    py::array_t<uint8_t, py::array::c_style | py::array::forcecast> const & compressed_signal,
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> & signal_out)
{
    throw_on_error(pod5::decompress_signal(
        gsl::make_span(compressed_signal.data(0), compressed_signal.shape(0)),
        arrow::system_memory_pool(),
        gsl::make_span(signal_out.mutable_data(0), signal_out.shape(0))));
}

inline std::size_t compress_signal_wrapper(
    py::array_t<std::int16_t, py::array::c_style | py::array::forcecast> const & signal,
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> & compressed_signal_out)
{
    auto size = throw_on_error(pod5::compress_signal(
        gsl::make_span(signal.data(), signal.shape(0)),
        arrow::system_memory_pool(),
        gsl::make_span(compressed_signal_out.mutable_data(), compressed_signal_out.shape(0))));

    return size;
}

inline std::size_t vbz_compressed_signal_max_size(std::size_t sample_count)
{
    return pod5::compressed_signal_max_size(sample_count);
}

inline std::size_t load_read_id_iterable(
    py::iterable const & read_ids_str,
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> & read_id_data_out)
{
    std::size_t out_idx = 0;
    auto read_ids = reinterpret_cast<boost::uuids::uuid *>(read_id_data_out.mutable_data());
    auto read_ids_out_len = read_id_data_out.shape(0);

    std::string temp_uuid;
    for (auto & read_id : read_ids_str) {
        if (out_idx >= read_ids_out_len) {
            throw std::runtime_error("Too many input uuids for output container");
        }

        temp_uuid = read_id.cast<py::str>();
        try {
            auto found_uuid = boost::lexical_cast<boost::uuids::uuid>(temp_uuid);
            read_ids[out_idx++] = found_uuid;

        } catch (boost::bad_lexical_cast const & e) {
            // Ignore - we will return one fewer read ids than expected and the caller can deal with it.
            continue;
        }
    }

    return out_idx;
}

inline py::list format_read_id_to_str(
    py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> & read_id_data_out)
{
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
