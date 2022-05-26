#include "pod5_format/async_signal_loader.h"
#include "pod5_format/c_api.h"
#include "pod5_format/file_reader.h"
#include "pod5_format/file_writer.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_compression.h"
#include "pod5_format/signal_table_reader.h"

#include <arrow/memory_pool.h>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <iostream>

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
    Pod5SignalCacheBatch(pod5::AsyncSignalLoader::SamplesMode samples_mode,
                         pod5::CachedBatchSignalData&& cached_data)
            : m_samples_mode(samples_mode), m_cached_data(std::move(cached_data)) {}

    py::array_t<std::uint64_t> sample_count() const {
        return py::array_t<std::uint64_t>(m_cached_data.sample_count().size(),
                                          m_cached_data.sample_count().data());
    }

    py::list samples() const {
        py::list py_samples;
        if (m_samples_mode != pod5::AsyncSignalLoader::SamplesMode::Samples) {
            return py_samples;
        }
        for (auto const& row_samples : m_cached_data.samples()) {
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
    Pod5AsyncSignalLoader(std::shared_ptr<pod5::FileReader> const& reader,
                          pod5::AsyncSignalLoader::SamplesMode samples_mode,
                          std::size_t worker_count = std::thread::hardware_concurrency(),
                          std::size_t max_pending_batches = 10)
            : m_samples_mode(samples_mode),
              m_batch_counts_ref({}),
              m_batch_rows_ref({}),
              m_async_loader(reader, samples_mode, {}, {}, worker_count, max_pending_batches) {}

    // Make an async loader for specific batches
    Pod5AsyncSignalLoader(
            std::shared_ptr<pod5::FileReader> const& reader,
            pod5::AsyncSignalLoader::SamplesMode samples_mode,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batches,
            std::size_t worker_count = std::thread::hardware_concurrency(),
            std::size_t max_pending_batches = 10)
            : m_samples_mode(samples_mode),
              m_batch_sizes(make_batch_counts(reader, batches)),
              m_async_loader(reader,
                             samples_mode,
                             gsl::make_span(m_batch_sizes),
                             {},
                             worker_count,
                             max_pending_batches) {}

    // Make an async loader for specific reads in specific batches
    Pod5AsyncSignalLoader(
            std::shared_ptr<pod5::FileReader> const& reader,
            pod5::AsyncSignalLoader::SamplesMode samples_mode,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_counts,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_rows,
            std::size_t worker_count = std::thread::hardware_concurrency(),
            std::size_t max_pending_batches = 10)
            : m_samples_mode(samples_mode),
              m_batch_counts_ref(std::move(batch_counts)),
              m_batch_rows_ref(std::move(batch_rows)),
              m_async_loader(reader,
                             samples_mode,
                             gsl::make_span(m_batch_counts_ref.data(), m_batch_counts_ref.size()),
                             gsl::make_span(m_batch_rows_ref.data(), m_batch_rows_ref.size()),
                             worker_count,
                             max_pending_batches) {}

    std::shared_ptr<Pod5SignalCacheBatch> release_next_batch() {
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
            std::shared_ptr<pod5::FileReader> const& reader,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast> const& batches) {
        std::vector<std::uint32_t> batch_counts(reader->num_read_record_batches(), 0);
        for (auto const& batch_idx : gsl::make_span(batches.data(), batches.shape(0))) {
            auto read_batch = reader->read_read_record_batch(batch_idx);
            if (!read_batch.ok()) {
                throw std::runtime_error("Failed to query read batch count: " +
                                         read_batch.status().ToString());
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

    Pod5FileReaderPtr(std::shared_ptr<pod5::FileReader>&& reader_) : reader(std::move(reader_)) {}

    pod5::FileLocation get_combined_file_read_table_location() const {
        POD5_PYTHON_ASSIGN_OR_RAISE(auto file_location, reader->read_table_location());
        return file_location;
    }

    pod5::FileLocation get_combined_file_signal_table_location() const {
        POD5_PYTHON_ASSIGN_OR_RAISE(auto file_location, reader->signal_table_location());
        return file_location;
    }

    void close() { reader = nullptr; }

    std::size_t plan_traversal(
            py::array_t<std::uint8_t, py::array::c_style | py::array::forcecast> const&
                    read_id_data,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>& batch_counts,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>& batch_rows) {
        auto const read_id_count = read_id_data.shape(0);
        auto search_input = pod5::ReadIdSearchInput(gsl::make_span(
                reinterpret_cast<boost::uuids::uuid const*>(read_id_data.data()), read_id_count));

        POD5_PYTHON_ASSIGN_OR_RAISE(
                auto find_success_count,
                reader->search_for_read_ids(
                        search_input,
                        gsl::make_span(batch_counts.mutable_data(),
                                       reader->num_read_record_batches()),
                        gsl::make_span(batch_rows.mutable_data(), read_id_count)));

        return find_success_count;
    }

    std::shared_ptr<Pod5AsyncSignalLoader> batch_get_signal(bool get_samples,
                                                            bool get_sample_count) {
        return std::make_shared<Pod5AsyncSignalLoader>(
                reader, get_samples ? pod5::AsyncSignalLoader::SamplesMode::Samples
                                    : pod5::AsyncSignalLoader::SamplesMode::NoSamples);
    }

    std::shared_ptr<Pod5AsyncSignalLoader> batch_get_signal_batches(
            bool get_samples,
            bool get_sample_count,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batches) {
        return std::make_shared<Pod5AsyncSignalLoader>(
                reader,
                get_samples ? pod5::AsyncSignalLoader::SamplesMode::Samples
                            : pod5::AsyncSignalLoader::SamplesMode::NoSamples,
                std::move(batches));
    }

    std::shared_ptr<Pod5AsyncSignalLoader> batch_get_signal_selection(
            bool get_samples,
            bool get_sample_count,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_counts,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&& batch_rows) {
        return std::make_shared<Pod5AsyncSignalLoader>(
                reader,
                get_samples ? pod5::AsyncSignalLoader::SamplesMode::Samples
                            : pod5::AsyncSignalLoader::SamplesMode::NoSamples,
                std::move(batch_counts), std::move(batch_rows));
    }
};

Pod5FileReaderPtr open_combined_file(char const* filename) {
    POD5_PYTHON_ASSIGN_OR_RAISE(auto reader, pod5::open_combined_file_reader(filename, {}));
    return Pod5FileReaderPtr(std::move(reader));
}

Pod5FileReaderPtr open_split_file(char const* signal_filename, char const* reads_filename) {
    POD5_PYTHON_ASSIGN_OR_RAISE(auto reader,
                                pod5::open_split_file_reader(signal_filename, reads_filename, {}));
    return Pod5FileReaderPtr(std::move(reader));
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

    py::class_<pod5::FileLocation>(m, "EmbeddedFileData")
            .def_readonly("offset", &pod5::FileLocation::offset)
            .def_readonly("length", &pod5::FileLocation::size);

    py::class_<Pod5AsyncSignalLoader, std::shared_ptr<Pod5AsyncSignalLoader>>(
            m, "Pod5AsyncSignalLoader")
            .def("release_next_batch", &Pod5AsyncSignalLoader::release_next_batch);

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
            .def("batch_get_signal_selection", &Pod5FileReaderPtr::batch_get_signal_selection)
            .def("batch_get_signal_batches", &Pod5FileReaderPtr::batch_get_signal_batches)
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
