#include "pod5_format/c_api.h"
#include "pod5_format/file_writer.h"
#include "pod5_format/signal_compression.h"

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
            int sort_order,
            py::array_t<std::uint32_t, py::array::c_style | py::array::forcecast>&
                    traversal_steps) {
        std::size_t find_success_count = 0;
        auto error = pod5_plan_traversal(
                reader, read_id_data.data(), read_id_data.shape(0),
                (pod5_traversal_sort_type_t)sort_order,
                reinterpret_cast<TraversalStep_t*>(traversal_steps.mutable_data()),
                &find_success_count);
        if (error != POD5_OK) {
            throw std::runtime_error(pod5_get_error_string());
        }
        return find_success_count;
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

    py::class_<Pod5FileReaderPtr>(m, "Pod5FileReader")
            .def("get_combined_file_read_table_location",
                 &Pod5FileReaderPtr::get_combined_file_read_table_location)
            .def("get_combined_file_signal_table_location",
                 &Pod5FileReaderPtr::get_combined_file_signal_table_location)
            .def("plan_traversal", &Pod5FileReaderPtr::plan_traversal)
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
}