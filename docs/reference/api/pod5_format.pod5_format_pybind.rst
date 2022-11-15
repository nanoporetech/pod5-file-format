
pod5\_format\_pybind 
========================================

..
 .. autoclass:: lib_pod5_format.pod5_format_pybind
    :members:
    :undoc-members:
    :show-inheritance:


.. py:class:: EmbeddedFileData(*args, **kwargs)

   .. py:method:: length() -> int
      :property:


   .. py:method:: offset() -> int
      :property:



.. py:class:: FileWriter(*args, **kwargs)

   .. py:method:: add_calibration(offset: float, scale: float) -> int


   .. py:method:: add_end_reason(end_reason_enum: int, forced: bool) -> int


   .. py:method:: add_pore(channel: int, well: int, pore_type: str) -> int


   .. py:method:: add_reads(count: int, read_ids: numpy.typing.NDArray[numpy.uint8], pores: numpy.typing.NDArray[numpy.int16], calibrations: numpy.typing.NDArray[numpy.int16], read_numbers: numpy.typing.NDArray[numpy.uint32], start_samples: numpy.typing.NDArray[numpy.uint64], median_befores: numpy.typing.NDArray[numpy.float32], end_reasons: numpy.typing.NDArray[numpy.int16], run_infos: numpy.typing.NDArray[numpy.int16], signals: List[numpy.typing.NDArray[numpy.int16]]) -> None


   .. py:method:: add_reads_pre_compressed(count: int, read_ids: numpy.typing.NDArray[numpy.uint8], pores: numpy.typing.NDArray[numpy.int16], calibrations: numpy.typing.NDArray[numpy.int16], read_numbers: numpy.typing.NDArray[numpy.uint32], start_samples: numpy.typing.NDArray[numpy.uint64], median_befores: numpy.typing.NDArray[numpy.float32], end_reasons: numpy.typing.NDArray[numpy.int16], run_infos: numpy.typing.NDArray[numpy.int16], signal_chunks: List[numpy.typing.NDArray[numpy.uint8]], signal_chunk_lengths: numpy.typing.NDArray[numpy.uint32], signal_chunk_counts: numpy.typing.NDArray[numpy.uint32]) -> None


   .. py:method:: add_run_info(acquisition_id: str, acquisition_start_time: int, adc_max: int, adc_min: int, context_tags: List[Tuple[str, str]], experiment_name: str, flow_cell_id: str, flow_cell_product_code: str, protocol_name: str, protocol_run_id: str, protocol_start_time: int, sample_id: str, sample_rate: int, sequencing_kit: str, sequencer_position: str, sequencer_position_type: str, software: str, system_name: str, system_type: str, tracking_id: List[Tuple[str, str]]) -> int


   .. py:method:: close() -> None



.. py:class:: FileWriterOptions(*args, **kwargs)

   .. py:attribute:: max_signal_chunk_size
      :annotation: :int

      

   .. py:attribute:: read_table_batch_size
      :annotation: :int

      

   .. py:attribute:: signal_compression_type
      :annotation: :Any

      

   .. py:attribute:: signal_table_batch_size
      :annotation: :int

      


.. py:class:: Pod5AsyncSignalLoader(*args, **kwargs)

   .. py:method:: release_next_batch() -> Pod5SignalCacheBatch



.. py:class:: Pod5FileReader(*args, **kwargs)

   .. py:method:: batch_get_signal(get_samples: bool, get_sample_count: bool) -> Pod5AsyncSignalLoader


   .. py:method:: batch_get_signal_batches(get_samples: bool, get_samples_count: bool, batches: numpy.typing.NDArray[numpy.uint32]) -> Pod5AsyncSignalLoader


   .. py:method:: batch_get_signal_selection(get_samples: bool, get_sample_count: bool, batch_counts: numpy.typing.NDArray[numpy.uint32], batch_rows: numpy.typing.NDArray[numpy.uint32]) -> Pod5AsyncSignalLoader


   .. py:method:: close() -> None


   .. py:method:: get_combined_file_read_table_location() -> EmbeddedFileData


   .. py:method:: get_combined_file_signal_table_location() -> EmbeddedFileData


   .. py:method:: plan_traversal(read_id_data: numpy.typing.NDArray[numpy.uint8], batch_counts: numpy.typing.NDArray[numpy.uint32], batch_rows: numpy.typing.NDArray[numpy.uint32]) -> int



.. py:class:: Pod5RepackerOutput(*args, **kwargs)


.. py:class:: Pod5SignalCacheBatch(*args, **kwargs)

   .. py:method:: batch_index() -> int
      :property:


   .. py:method:: sample_count() -> numpy.typing.NDArray[numpy.uint64]
      :property:


   .. py:method:: samples() -> List[numpy.typing.NDArray[numpy.int16]]
      :property:



.. py:class:: Repacker

   .. py:method:: add_all_reads_to_output(output: Pod5RepackerOutput, input: Pod5FileReader) -> None


   .. py:method:: add_output(output: FileWriter) -> Pod5RepackerOutput


   .. py:method:: add_selected_reads_to_output(output: Pod5RepackerOutput, input: Pod5FileReader, batch_counts: numpy.typing.NDArray[numpy.uint32], all_batch_rows: numpy.typing.NDArray[numpy.uint32]) -> None


   .. py:method:: finish() -> None


   .. py:method:: batches_completed() -> int
      :property:


   .. py:method:: batches_requested() -> int
      :property:


   .. py:method:: is_complete() -> bool
      :property:


   .. py:method:: pending_batch_writes() -> int
      :property:


   .. py:method:: reads_completed() -> int
      :property:


   .. py:method:: reads_sample_bytes_completed() -> int
      :property:



.. py:function:: compress_signal(signal: numpy.typing.NDArray[numpy.int16], compressed_signal_out: numpy.typing.NDArray[numpy.uint8]) -> int


.. py:function:: create_combined_file(filename: str, writer_name: str, options: FileWriterOptions = ...) -> FileWriter


.. py:function:: create_split_file(signal_filename: str, reads_filename: str, writer_name: str, options: FileWriterOptions = ...) -> FileWriter


.. py:function:: decompress_signal(compressed_signal: numpy.typing.NDArray[numpy.uint8], signal_out: numpy.typing.NDArray[numpy.int16]) -> None


.. py:function:: format_read_id_to_str(read_id_data_out: numpy.typing.NDArray[numpy.uint8]) -> List[numpy.typing.NDArray[numpy.uint8]]


.. py:function:: get_error_string() -> str


.. py:function:: load_read_id_iterable(read_ids_str: Iterable, read_id_data_out: numpy.typing.NDArray[numpy.uint8]) -> None


.. py:function:: open_combined_file(filename: str) -> Pod5FileReader


.. py:function:: open_split_file(signal_filename: str, reads_filename: str) -> Pod5FileReader


.. py:function:: vbz_compressed_signal_max_size(sample_count: int) -> int


