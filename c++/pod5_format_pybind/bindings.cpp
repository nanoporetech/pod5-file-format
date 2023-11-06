#include "api.h"
#include "pod5_format/c_api.h"
#include "repack/repack_output.h"
#include "repack/repacker.h"

PYBIND11_MODULE(pod5_format_pybind, m)
{
    using namespace pod5;
    pod5_init();

    m.doc() = "POD5 Format Raw Bindings";

    auto thread_pool = pod5::make_thread_pool(std::thread::hardware_concurrency());

    py::class_<FileWriterOptions>(m, "FileWriterOptions")
        .def(py::init([thread_pool]() {
            FileWriterOptions options;
            options.set_thread_pool(thread_pool);
            return options;
        }))
        .def_property(
            "max_signal_chunk_size",
            &FileWriterOptions::max_signal_chunk_size,
            &FileWriterOptions::set_max_signal_chunk_size)
        .def_property(
            "signal_table_batch_size",
            &FileWriterOptions::signal_table_batch_size,
            &FileWriterOptions::set_signal_table_batch_size)
        .def_property(
            "read_table_batch_size",
            &FileWriterOptions::read_table_batch_size,
            &FileWriterOptions::set_read_table_batch_size)
        .def_property(
            "signal_compression_type",
            &FileWriterOptions::signal_type,
            &FileWriterOptions::set_signal_type);

    py::class_<FileWriter, std::shared_ptr<FileWriter>>(m, "FileWriter")
        .def("close", [](pod5::FileWriter & w) { throw_on_error(w.close()); })
        .def(
            "add_pore",
            [](pod5::FileWriter & w, std::string pore_type) {
                return throw_on_error(w.add_pore_type(std::move(pore_type)));
            })
        .def(
            "add_end_reason",
            [](pod5::FileWriter & w, int name) {
                return throw_on_error(w.lookup_end_reason((pod5::ReadEndReason)name));
            })
        .def("add_run_info", FileWriter_add_run_info)
        .def("add_reads", FileWriter_add_reads)
        .def("add_reads_pre_compressed", FileWriter_add_reads_pre_compressed);

    py::class_<pod5::FileLocation>(m, "EmbeddedFileData")
        .def_readonly("file_path", &pod5::FileLocation::file_path)
        .def_readonly("offset", &pod5::FileLocation::offset)
        .def_readonly("length", &pod5::FileLocation::size);

    py::class_<Pod5AsyncSignalLoader, std::shared_ptr<Pod5AsyncSignalLoader>>(
        m, "Pod5AsyncSignalLoader")
        .def("release_next_batch", &Pod5AsyncSignalLoader::release_next_batch);

    py::class_<Pod5SignalCacheBatch, std::shared_ptr<Pod5SignalCacheBatch>>(
        m, "Pod5SignalCacheBatch")
        .def_property_readonly("batch_index", &Pod5SignalCacheBatch::batch_index)
        .def_property_readonly("sample_count", &Pod5SignalCacheBatch::sample_count)
        .def_property_readonly("samples", &Pod5SignalCacheBatch::samples);

    py::class_<Pod5FileReaderPtr>(m, "Pod5FileReader")
        .def(
            "get_file_run_info_table_location",
            &Pod5FileReaderPtr::get_file_run_info_table_location)
        .def("get_file_read_table_location", &Pod5FileReaderPtr::get_file_read_table_location)
        .def("get_file_signal_table_location", &Pod5FileReaderPtr::get_file_signal_table_location)
        .def("get_file_version_pre_migration", &Pod5FileReaderPtr::get_file_version_pre_migration)
        .def("plan_traversal", &Pod5FileReaderPtr::plan_traversal)
        .def("batch_get_signal", &Pod5FileReaderPtr::batch_get_signal)
        .def("batch_get_signal_selection", &Pod5FileReaderPtr::batch_get_signal_selection)
        .def("batch_get_signal_batches", &Pod5FileReaderPtr::batch_get_signal_batches)
        .def("close", &Pod5FileReaderPtr::close);

    // Errors API
    m.def("get_error_string", &pod5_get_error_string, "Get the most recent error as a string");

    // Creating files
    m.def(
        "create_file",
        &create_file,
        "Create a POD5 file for writing",
        py::arg("filename"),
        py::arg("writer_name"),
        py::arg("options") = nullptr);

    // Opening files
    m.def("open_file", &open_file, "Open a POD5 file for reading");
    m.def("recover_file", &recover_file, "Recover a POD5 file which was not closed correctly");

    m.def(
        "update_file",
        &write_updated_file_to_dest,
        "Update a POD5 file to the latest writer format");

    // Signal API
    m.def("decompress_signal", &decompress_signal_wrapper, "Decompress a numpy array of signal");
    m.def("compress_signal", &compress_signal_wrapper, "Compress a numpy array of signal");
    m.def("vbz_compressed_signal_max_size", &vbz_compressed_signal_max_size);

    // Repacker API
    py::class_<repack::Pod5RepackerOutput, std::shared_ptr<repack::Pod5RepackerOutput>>(
        m, "Pod5RepackerOutput");

    py::class_<repack::Pod5Repacker, std::shared_ptr<repack::Pod5Repacker>>(m, "Repacker")
        .def(py::init<>())
        .def("add_output", &repack::Pod5Repacker::add_output)
        .def("set_output_finished", &repack::Pod5Repacker::set_output_finished)
        .def("add_all_reads_to_output", &repack::Pod5Repacker::add_all_reads_to_output)
        .def("add_selected_reads_to_output", &repack::Pod5Repacker::add_selected_reads_to_output)
        .def("finish", &repack::Pod5Repacker::finish)
        .def_property_readonly("is_complete", &repack::Pod5Repacker::is_complete)
        .def_property_readonly(
            "currently_open_file_reader_count",
            &repack::Pod5Repacker::currently_open_file_reader_count)
        .def_property_readonly("reads_completed", &repack::Pod5Repacker::reads_completed);

    // Util API
    m.def(
        "load_read_id_iterable",
        &load_read_id_iterable,
        "Load an iterable of read ids into a numpy array of data");
    m.def("format_read_id_to_str", &format_read_id_to_str, "Format an array of read ids to string");
}
