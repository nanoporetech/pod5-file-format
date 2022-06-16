#include "api.h"
#include "pod5_format/c_api.h"
#include "repacker.h"

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

    // Repacker API
    py::class_<Pod5RepackerOutput, std::shared_ptr<Pod5RepackerOutput>>(m, "Pod5RepackerOutput");

    py::class_<Pod5Repacker, std::shared_ptr<Pod5Repacker>>(m, "Repacker")
            .def(py::init<>())
            .def("add_output", &Pod5Repacker::add_output)
            .def("add_all_reads_to_output", &Pod5Repacker::add_all_reads_to_output)
            .def("add_selected_reads_to_output", &Pod5Repacker::add_selected_reads_to_output)
            .def("finish", &Pod5Repacker::finish)
            .def_property_readonly("is_complete", &Pod5Repacker::is_complete)
            .def_property_readonly("pending_batch_writes", &Pod5Repacker::pending_batch_writes)
            .def_property_readonly("reads_completed", &Pod5Repacker::reads_completed)
            .def_property_readonly("reads_sample_bytes_completed",
                                   &Pod5Repacker::reads_sample_bytes_completed)
            .def_property_readonly("batches_requested", &Pod5Repacker::batches_requested)
            .def_property_readonly("batches_completed", &Pod5Repacker::batches_completed);

    // Util API
    m.def("load_read_id_iterable", &load_read_id_iterable,
          "Load an iterable of read ids into a numpy array of data");
    m.def("format_read_id_to_str", &format_read_id_to_str, "Format an array of read ids to string");
}
