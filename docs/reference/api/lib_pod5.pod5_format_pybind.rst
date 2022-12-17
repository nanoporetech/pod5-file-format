
pod5\_format\_pybind
========================================


.. .. automodule:: lib_pod5.pod5_format_pybind
..    :members:
..    :undoc-members:
..    :show-inheritance:


.. py:class:: EmbeddedFileData(*args, **kwargs)

   .. py:property:: length() -> int

   .. py:property:: offset() -> int

   .. py:property:: file_path() -> str


.. py:class:: FileWriter(*args, **kwargs)

   .. py:method:: add_end_reason(end_reason_enum: int) -> int

   .. py:method:: add_pore(pore_type: str) -> int

   .. py:method:: add_reads(count: int, read_ids: numpy.typing.NDArray[numpy.uint8], read_numbers: numpy.typing.NDArray[np.uint32], start_samples: numpy.typing.NDArray[np.uint64], channels: numpy.typing.NDArray[np.uint16], wells: numpy.typing.NDArray[np.uint8], pore_types: numpy.typing.NDArray[np.int16], calibration_offsets: numpy.typing.NDArray[np.float32], calibration_scales: numpy.typing.NDArray[np.float32], median_befores: numpy.typing.NDArray[np.float32], end_reasons: numpy.typing.NDArray[np.int16], end_reason_forceds: numpy.typing.NDArray[bool], run_infos: numpy.typing.NDArray[np.int16], num_minknow_events: numpy.typing.NDArray[np.uint64], tracked_scaling_scales: numpy.typing.NDArray[np.float32], tracked_scaling_shifts: numpy.typing.NDArray[np.float32], predicted_scaling_scales: numpy.typing.NDArray[np.float32], predicted_scaling_shifts: numpy.typing.NDArray[np.float32], num_reads_since_mux_changes: numpy.typing.NDArray[np.uint32], time_since_mux_changes: numpy.typing.NDArray[np.float32], signals: List[npt.NDArray[np.int16]) -> None

   .. py:method:: add_reads_pre_compressed(count: int, read_ids: numpy.typing.NDArray[np.uint8], read_numbers: numpy.typing.NDArray[np.uint32], start_samples: numpy.typing.NDArray[np.uint64], channels: numpy.typing.NDArray[np.uint16], wells: numpy.typing.NDArray[np.uint8], pore_types: numpy.typing.NDArray[np.int16], calibration_offsets: numpy.typing.NDArray[np.float32], calibration_scales: numpy.typing.NDArray[np.float32], median_befores: numpy.typing.NDArray[np.float32], end_reasons: numpy.typing.NDArray[np.int16], end_reason_forceds: numpy.typing.NDArray[bool], run_infos: numpy.typing.NDArray[np.int16], num_minknow_events: numpy.typing.NDArray[np.uint64], tracked_scaling_scales: numpy.typing.NDArray[np.float32], tracked_scaling_shifts: numpy.typing.NDArray[np.float32], predicted_scaling_scales: numpy.typing.NDArray[np.float32], predicted_scaling_shifts: numpy.typing.NDArray[np.float32], num_reads_since_mux_changes: numpy.typing.NDArray[np.uint32], time_since_mux_changes: numpy.typing.NDArray[np.float32], signal_chunks: List[npt.NDArray[np.uint8]], signal_chunk_lengths: npt.NDArray[np.uint32], signal_chunk_counts: npt.NDArray[np.uint32]) -> None

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

   .. py:method:: get_file_read_table_location() -> EmbeddedFileData

   .. py:method:: get_file_run_info_table_location() -> EmbeddedFileData

   .. py:method:: get_file_signal_table_location() -> EmbeddedFileData

   .. py:method:: plan_traversal(read_id_data: numpy.typing.NDArray[numpy.uint8], batch_counts: numpy.typing.NDArray[numpy.uint32], batch_rows: numpy.typing.NDArray[numpy.uint32]) -> int


.. py:class:: Pod5RepackerOutput(*args, **kwargs)


.. py:class:: Pod5SignalCacheBatch(*args, **kwargs)

   .. py:property:: batch_index() -> int

   .. py:property:: sample_count() -> numpy.typing.NDArray[numpy.uint64]

   .. py:property:: samples() -> List[numpy.typing.NDArray[numpy.int16]]



.. py:class:: Repacker

   .. py:method:: add_all_reads_to_output(output: Pod5RepackerOutput, input: Pod5FileReader) -> None

   .. py:method:: add_output(output: FileWriter) -> Pod5RepackerOutput

   .. py:method:: add_selected_reads_to_output(output: Pod5RepackerOutput, input: Pod5FileReader, batch_counts: numpy.typing.NDArray[numpy.uint32], all_batch_rows: numpy.typing.NDArray[numpy.uint32]) -> None

   .. py:method:: finish() -> None

   .. py:property:: batches_completed() -> int

   .. py:property:: batches_requested() -> int

   .. py:property:: is_complete() -> bool

   .. py:property:: pending_batch_writes() -> int

   .. py:property:: reads_completed() -> int

   .. py:property:: reads_sample_bytes_completed() -> int


.. py:function:: create_file(filename: str, writer_name: str, options: FileWriterOptions = ...) -> FileWriter

.. py:function:: open_file(filename: str) -> Pod5FileReader

.. py:function:: get_error_string() -> str

.. py:function:: format_read_id_to_str(read_id_data_out: numpy.typing.NDArray[numpy.uint8]) -> List[numpy.typing.NDArray[numpy.uint8]]

.. py:function:: load_read_id_iterable(read_ids_str: Iterable, read_id_data_out: numpy.typing.NDArray[numpy.uint8]) -> None

.. py:function:: compress_signal(signal: numpy.typing.NDArray[numpy.int16], compressed_signal_out: numpy.typing.NDArray[numpy.uint8]) -> int

.. py:function:: decompress_signal(compressed_signal: numpy.typing.NDArray[numpy.uint8], signal_out: numpy.typing.NDArray[numpy.int16]) -> None

.. py:function:: vbz_compressed_signal_max_size(sample_count: int) -> int
