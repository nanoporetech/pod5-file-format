#pragma once

#include "pod5_format/read_table_reader.h"

#include <arrow/array/array_dict.h>

#include <mutex>
#include <unordered_map>

namespace repack {

struct pair_hasher {
    template <class T1, class T2>
    std::size_t operator()(std::pair<T1, T2> const & pair) const
    {
        return std::hash<T1>{}(pair.first) ^ std::hash<T2>{}(pair.second);
    }
};

struct run_info_hasher {
    std::size_t operator()(pod5::RunInfoData const & run_info) const
    {
        return std::hash<std::string>{}(run_info.acquisition_id);
    }
};

class ReadsTableDictionaryManager {
public:
    ReadsTableDictionaryManager(
        std::shared_ptr<pod5::FileWriter> const & output_file,
        std::mutex & writer_mutex)
    : m_output_file(output_file)
    , m_writer_mutex(writer_mutex)
    {
    }

    // Find or create a pore index in the output file - expects to run on strand.
    arrow::Result<pod5::PoreDictionaryIndex> find_pore_index(
        std::shared_ptr<pod5::FileReader> const & source_file,
        pod5::ReadTableRecordBatch const & source_batch,
        pod5::PoreDictionaryIndex source_index)
    {
        std::lock_guard<std::mutex> l(m_writer_mutex);

        ARROW_ASSIGN_OR_RAISE(auto source_data, source_batch.get_pore_type(source_index));
        pod5::PoreDictionaryIndex dest_index = 0;

        // See if we have the same run info by value stored in the file:
        auto data_lookup_it = m_pore_data_indexes.find(source_data);
        if (data_lookup_it != m_pore_data_indexes.end()) {
            dest_index = data_lookup_it->second;
        } else {
            ARROW_ASSIGN_OR_RAISE(dest_index, m_output_file->add_pore_type(source_data));
        }

        m_pore_data_indexes[source_data] = dest_index;
        return dest_index;
    }

    // Find or create a run_info index in the output file - expects to run on strand.
    arrow::Result<pod5::RunInfoDictionaryIndex> find_run_info_index(
        std::shared_ptr<pod5::FileReader> const & source_file,
        pod5::ReadTableRecordBatch const & source_batch,
        pod5::RunInfoDictionaryIndex source_index)
    {
        std::lock_guard<std::mutex> l(m_writer_mutex);

        ARROW_ASSIGN_OR_RAISE(auto source_data, source_batch.get_run_info(source_index));
        ARROW_ASSIGN_OR_RAISE(auto const run_info, source_file->find_run_info(source_data));
        pod5::RunInfoDictionaryIndex dest_index = 0;

        // See if we have the same run info by value stored in the file:
        auto data_lookup_it = m_run_info_data_indexes.find(*run_info);
        if (data_lookup_it != m_run_info_data_indexes.end()) {
            dest_index = data_lookup_it->second;
        } else {
            ARROW_ASSIGN_OR_RAISE(dest_index, m_output_file->add_run_info(*run_info));
        }

        m_run_info_data_indexes[*run_info] = dest_index;
        return dest_index;
    }

private:
    std::shared_ptr<pod5::FileWriter> m_output_file;

    std::mutex & m_writer_mutex;

    std::unordered_map<std::string, pod5::PoreDictionaryIndex> m_pore_data_indexes;
    std::unordered_map<pod5::RunInfoData, pod5::RunInfoDictionaryIndex, run_info_hasher>
        m_run_info_data_indexes;
};

class ReadsTableDictionaryThreadCache {
public:
    ReadsTableDictionaryThreadCache(std::shared_ptr<ReadsTableDictionaryManager> const & main_cache)
    : m_main_cache(main_cache)
    {
    }

    // Find or create a pore index in the output file - expects to run on strand.
    arrow::Result<pod5::PoreDictionaryIndex> find_pore_index(
        std::shared_ptr<pod5::FileReader> const & source_file,
        pod5::ReadTableRecordBatch const & source_batch,
        pod5::PoreDictionaryIndex source_index)
    {
        auto const key = std::make_pair(make_file_key(source_file), source_index);
        auto const it = m_pore_indexes.find(key);
        if (it != m_pore_indexes.end()) {
            return it->second;
        }

        ARROW_ASSIGN_OR_RAISE(
            auto dest_index,
            m_main_cache->find_pore_index(source_file, source_batch, source_index));
        m_pore_indexes[key] = dest_index;
        return dest_index;
    }

    // Find or create a run_info index in the output file - expects to run on strand.
    arrow::Result<pod5::RunInfoDictionaryIndex> find_run_info_index(
        std::shared_ptr<pod5::FileReader> const & source_file,
        pod5::ReadTableRecordBatch const & source_batch,
        pod5::RunInfoDictionaryIndex source_index)
    {
        auto const key = std::make_pair(make_file_key(source_file), source_index);
        auto const it = m_run_info_indexes.find(key);
        if (it != m_run_info_indexes.end()) {
            return it->second;
        }

        ARROW_ASSIGN_OR_RAISE(
            auto dest_index,
            m_main_cache->find_run_info_index(source_file, source_batch, source_index));
        m_run_info_indexes[key] = dest_index;
        return dest_index;
    }

private:
    using FileKey = std::uint64_t;

    FileKey make_file_key(std::shared_ptr<pod5::FileReader> const & file)
    {
        return reinterpret_cast<FileKey>(file.get());
    }

    template <typename IndexType>
    using DictionaryLookup =
        std::unordered_map<std::pair<FileKey, IndexType>, IndexType, pair_hasher>;

    std::shared_ptr<ReadsTableDictionaryManager> m_main_cache;

    DictionaryLookup<pod5::PoreDictionaryIndex> m_pore_indexes;
    DictionaryLookup<pod5::RunInfoDictionaryIndex> m_run_info_indexes;
};

}  // namespace repack
