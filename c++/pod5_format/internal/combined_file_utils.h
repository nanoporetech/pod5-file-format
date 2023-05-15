#pragma once

#include "footer_generated.h"
#include "pod5_format/file_reader.h"
#include "pod5_format/result.h"
#include "pod5_format/version.h"

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/util/endian.h>
#include <arrow/util/io_util.h>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <flatbuffers/flatbuffers.h>

#include <array>

namespace pod5 { namespace combined_file_utils {

static constexpr std::array<char, 8>
    FILE_SIGNATURE{'\213', 'P', 'O', 'D', '\r', '\n', '\032', '\n'};

static constexpr std::size_t header_size = 24;  // signature 8 bytes, section marker 16 bytes

inline pod5::Status pad_file(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    std::uint32_t pad_to_size)
{
    ARROW_ASSIGN_OR_RAISE(auto const current_byte_location, sink->Tell());
    auto const bytes_to_write = pad_to_size - (current_byte_location % pad_to_size);
    if (bytes_to_write == pad_to_size) {
        return pod5::Status::OK();
    }

    std::array<char, 128> zeroes{};
    return sink->Write(zeroes.data(), bytes_to_write);
}

inline pod5::Status write_file_signature(std::shared_ptr<arrow::io::OutputStream> const & sink)
{
    return sink->Write(FILE_SIGNATURE.data(), FILE_SIGNATURE.size());
}

inline pod5::Status write_section_marker(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    boost::uuids::uuid const & section_marker)
{
    return sink->Write(
        section_marker.begin(), std::distance(section_marker.begin(), section_marker.end()));
}

inline pod5::Status write_combined_header(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    boost::uuids::uuid const & section_marker)
{
    ARROW_RETURN_NOT_OK(write_file_signature(sink));
    return write_section_marker(sink, section_marker);
}

inline pod5::Status write_footer_magic(std::shared_ptr<arrow::io::OutputStream> const & sink)
{
    return sink->Write("FOOTER\0\0", 8);
}

struct FileInfo {
    std::int64_t file_start_offset = 0;
    std::int64_t file_length = 0;
};

struct ParsedFileInfo : FileInfo {
    std::string file_path;
    std::shared_ptr<arrow::io::RandomAccessFile> file;

    arrow::Status from_full_file(std::string in_file_path)
    {
        file_path = in_file_path;
        ARROW_ASSIGN_OR_RAISE(
            file, arrow::io::MemoryMappedFile::Open(in_file_path, arrow::io::FileMode::READ));
        file_start_offset = 0;
        ARROW_ASSIGN_OR_RAISE(file_length, file->GetSize());
        return arrow::Status::OK();
    }
};

inline pod5::Result<std::int64_t> write_footer_flatbuffer(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    boost::uuids::uuid const & file_identifier,
    std::string const & software_name,
    FileInfo const & signal_table,
    FileInfo const & run_info_table,
    FileInfo const & reads_table)
{
    flatbuffers::FlatBufferBuilder builder(1024);

    auto signal_file = Minknow::ReadsFormat::CreateEmbeddedFile(
        builder,
        signal_table.file_start_offset,
        signal_table.file_length,
        Minknow::ReadsFormat::Format_FeatherV2,
        Minknow::ReadsFormat::ContentType_SignalTable);

    auto run_info_file = Minknow::ReadsFormat::CreateEmbeddedFile(
        builder,
        run_info_table.file_start_offset,
        run_info_table.file_length,
        Minknow::ReadsFormat::Format_FeatherV2,
        Minknow::ReadsFormat::ContentType_RunInfoTable);

    auto reads_file = Minknow::ReadsFormat::CreateEmbeddedFile(
        builder,
        reads_table.file_start_offset,
        reads_table.file_length,
        Minknow::ReadsFormat::Format_FeatherV2,
        Minknow::ReadsFormat::ContentType_ReadsTable);

    const std::vector<flatbuffers::Offset<Minknow::ReadsFormat::EmbeddedFile>> files{
        signal_file, run_info_file, reads_file};
    auto footer = Minknow::ReadsFormat::CreateFooterDirect(
        builder,
        boost::uuids::to_string(file_identifier).c_str(),
        software_name.c_str(),
        Pod5Version.c_str(),
        &files);

    builder.Finish(footer);
    ARROW_RETURN_NOT_OK(sink->Write(builder.GetBufferPointer(), builder.GetSize()));
    return builder.GetSize();
}

inline pod5::Status write_footer(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    boost::uuids::uuid const & section_marker,
    boost::uuids::uuid const & file_identifier,
    std::string const & software_name,
    FileInfo const & signal_table,
    FileInfo const & run_info_table,
    FileInfo const & reads_table)
{
    ARROW_RETURN_NOT_OK(write_footer_magic(sink));
    ARROW_ASSIGN_OR_RAISE(
        std::int64_t length,
        write_footer_flatbuffer(
            sink, file_identifier, software_name, signal_table, run_info_table, reads_table));
    ARROW_RETURN_NOT_OK(pad_file(sink, 8));

    std::int64_t paded_flatbuffer_size = arrow::bit_util::ToLittleEndian(length);
    ARROW_RETURN_NOT_OK(sink->Write(&paded_flatbuffer_size, sizeof(paded_flatbuffer_size)));

    ARROW_RETURN_NOT_OK(write_section_marker(sink, section_marker));
    return write_file_signature(sink);
}

struct ParsedFooter {
    boost::uuids::uuid file_identifier;
    std::string software_name;
    std::string writer_pod5_version;

    ParsedFileInfo run_info_table;
    ParsedFileInfo reads_table;
    ParsedFileInfo signal_table;
};

inline pod5::Status check_signature(
    std::shared_ptr<arrow::io::RandomAccessFile> const & file,
    std::int64_t offset_in_file)
{
    std::array<char, sizeof(FILE_SIGNATURE)> read_signature;
    ARROW_ASSIGN_OR_RAISE(
        auto read_bytes,
        file->ReadAt(offset_in_file, read_signature.size(), read_signature.data()));
    if (read_bytes != (std::int16_t)read_signature.size() || read_signature != FILE_SIGNATURE) {
        return arrow::Status::IOError("Invalid signature in file");
    }

    return arrow::Status::OK();
}

inline pod5::Result<Minknow::ReadsFormat::Footer const *> read_footer_flatbuffer(
    std::vector<std::uint8_t> const & footer_data)
{
    auto verifier = flatbuffers::Verifier(footer_data.data(), footer_data.size());
    if (!verifier.VerifyBuffer<Minknow::ReadsFormat::Footer>()) {
        return arrow::Status::IOError("Invalid footer found in file");
    }
    return flatbuffers::GetRoot<Minknow::ReadsFormat::Footer>(footer_data.data());
}

inline pod5::Result<ParsedFooter> read_footer(
    std::string const & file_path,
    std::shared_ptr<arrow::io::RandomAccessFile> const & file)
{
    // Verify signature at start and end of file:
    ARROW_RETURN_NOT_OK(check_signature(file, 0));
    ARROW_ASSIGN_OR_RAISE(auto const file_size, file->GetSize());
    ARROW_RETURN_NOT_OK(check_signature(file, file_size - FILE_SIGNATURE.size()));

    auto footer_length_data_end = file_size;
    footer_length_data_end -= FILE_SIGNATURE.size();
    footer_length_data_end -= sizeof(boost::uuids::uuid);

    std::int64_t footer_length = 0;
    ARROW_RETURN_NOT_OK(file->ReadAt(
        footer_length_data_end - sizeof(footer_length), sizeof(footer_length), &footer_length));
    footer_length = arrow::bit_util::FromLittleEndian(footer_length);
    if (footer_length < 0
        || static_cast<std::size_t>(footer_length) > footer_length_data_end - sizeof(footer_length))
    {
        return arrow::Status::IOError("Invalid footer length");
    }

    std::vector<std::uint8_t> footer_data;
    footer_data.resize(footer_length);
    ARROW_ASSIGN_OR_RAISE(
        auto read_bytes,
        file->ReadAt(
            footer_length_data_end - sizeof(footer_length) - footer_length,
            footer_length,
            footer_data.data()));
    if (read_bytes != footer_length) {
        return arrow::Status::IOError("Failed to read footer data");
    }
    ARROW_ASSIGN_OR_RAISE(auto fb_footer, read_footer_flatbuffer(footer_data));

    ParsedFooter footer;
    if (!fb_footer->file_identifier()) {
        return arrow::Status::IOError("Invalid footer file_identifier");
    }
    try {
        footer.file_identifier =
            boost::lexical_cast<boost::uuids::uuid>(fb_footer->file_identifier()->str());
    } catch (boost::bad_lexical_cast const &) {
        return Status::IOError(
            "Invalid file_identifier in file: '", fb_footer->file_identifier()->str(), "'");
    }

    if (!fb_footer->software()) {
        return arrow::Status::IOError("Invalid footer software");
    }
    footer.software_name = fb_footer->software()->str();

    if (!fb_footer->pod5_version()) {
        return arrow::Status::IOError("Invalid footer pod5_version");
    }
    footer.writer_pod5_version = fb_footer->pod5_version()->str();

    if (!fb_footer->contents()) {
        return arrow::Status::IOError("Invalid footer contents");
    }
    for (auto const embedded_file : *fb_footer->contents()) {
        if (embedded_file->format() != Minknow::ReadsFormat::Format_FeatherV2) {
            return arrow::Status::IOError("Invalid embedded file format");
        }
        switch (embedded_file->content_type()) {
        case Minknow::ReadsFormat::ContentType_RunInfoTable:
            footer.run_info_table.file_start_offset = embedded_file->offset();
            footer.run_info_table.file_length = embedded_file->length();
            footer.run_info_table.file = file;
            footer.run_info_table.file_path = file_path;
            break;
        case Minknow::ReadsFormat::ContentType_ReadsTable:
            footer.reads_table.file_start_offset = embedded_file->offset();
            footer.reads_table.file_length = embedded_file->length();
            footer.reads_table.file = file;
            footer.reads_table.file_path = file_path;
            break;
        case Minknow::ReadsFormat::ContentType_SignalTable:
            footer.signal_table.file_start_offset = embedded_file->offset();
            footer.signal_table.file_length = embedded_file->length();
            footer.signal_table.file = file;
            footer.signal_table.file_path = file_path;
            break;

        default:
            return arrow::Status::IOError("Unknown embedded file type");
        }
    }

    return footer;
}

class SubFile : public arrow::io::internal::RandomAccessFileConcurrencyWrapper<SubFile> {
public:
    SubFile(
        std::shared_ptr<arrow::io::RandomAccessFile> main_file,
        std::int64_t sub_file_offset,
        std::int64_t sub_file_length)
    : m_file(std::move(main_file))
    , m_sub_file_offset(sub_file_offset)
    , m_sub_file_length(sub_file_length)
    {
    }

protected:
    arrow::Status DoClose() { return m_file->Close(); }

    bool closed() const override { return m_file->closed(); }

    arrow::Result<std::int64_t> DoTell() const
    {
        ARROW_ASSIGN_OR_RAISE(auto t, m_file->Tell());
        return t - m_sub_file_offset;
    }

    arrow::Status DoSeek(int64_t offset)
    {
        if (offset < 0 || offset > m_sub_file_length) {
            return arrow::Status::IOError("Invalid offset into SubFile");
        }
        offset += m_sub_file_offset;
        return m_file->Seek(offset);
    }

    arrow::Result<std::int64_t> DoRead(int64_t length, void * data)
    {
        ARROW_ASSIGN_OR_RAISE(auto pos, m_file->Tell());
        int64_t const remaining = m_sub_file_offset + m_sub_file_length - pos;
        length = std::min(remaining, length);
        return m_file->Read(length, data);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> DoRead(int64_t length)
    {
        ARROW_ASSIGN_OR_RAISE(auto pos, m_file->Tell());
        int64_t const remaining = m_sub_file_offset + m_sub_file_length - pos;
        length = std::min(remaining, length);
        return m_file->Read(length);
    }

    Result<int64_t> DoReadAt(int64_t position, int64_t nbytes, void * out)
    {
        if (position < 0 || position > m_sub_file_length) {
            return arrow::Status::IOError("Invalid offset into SubFile");
        }
        int64_t const remaining = m_sub_file_length - position;
        nbytes = std::min(nbytes, remaining);
        return m_file->ReadAt(position + m_sub_file_offset, nbytes, out);
    }

    Result<std::shared_ptr<arrow::Buffer>> DoReadAt(int64_t position, int64_t nbytes)
    {
        if (position < 0 || position > m_sub_file_length) {
            return arrow::Status::IOError("Invalid offset into SubFile");
        }
        int64_t const remaining = m_sub_file_length - position;
        nbytes = std::min(nbytes, remaining);
        return m_file->ReadAt(position + m_sub_file_offset, nbytes);
    }

    arrow::Result<std::int64_t> DoGetSize() { return m_sub_file_length; }

private:
    friend RandomAccessFileConcurrencyWrapper<SubFile>;

    std::shared_ptr<arrow::io::RandomAccessFile> m_file;
    std::int64_t m_sub_file_offset;
    std::int64_t m_sub_file_length;
};

inline arrow::Result<std::shared_ptr<SubFile>> open_sub_file(ParsedFileInfo file_info)
{
    if (!file_info.file) {
        return arrow::Status::Invalid("Failed to open file from footer");
    }
    ARROW_ASSIGN_OR_RAISE(auto file_size, file_info.file->GetSize());
    if (file_info.file_length < 0 || file_info.file_length > file_size
        || file_info.file_start_offset > file_size - file_info.file_length)
    {
        return arrow::Status::Invalid("Bad footer info");
    }
    // Restrict our open file to just the run info section:
    auto sub_file = std::make_shared<SubFile>(
        file_info.file, file_info.file_start_offset, file_info.file_length);
    ARROW_RETURN_NOT_OK(sub_file->Seek(0));
    return sub_file;
}

inline arrow::Result<std::shared_ptr<SubFile>> open_sub_file(
    std::shared_ptr<arrow::io::RandomAccessFile> const & file,
    std::size_t offset)
{
    if (!file) {
        return arrow::Status::Invalid("Failed to open file from footer");
    }

    ARROW_ASSIGN_OR_RAISE(auto file_size, file->GetSize());

    // Restrict our open file to just the run info section:
    auto sub_file = std::make_shared<SubFile>(file, offset, file_size - offset);
    ARROW_RETURN_NOT_OK(sub_file->Seek(0));
    return sub_file;
}

enum class SubFileCleanup { CleanupOriginalFile, LeaveOrignalFile };

inline arrow::Result<combined_file_utils::FileInfo> write_file(
    arrow::MemoryPool * pool,
    std::shared_ptr<arrow::io::FileOutputStream> const & file,
    FileLocation const & file_location,
    SubFileCleanup cleanup_mode)
{
    combined_file_utils::FileInfo table_data;
    // Record file start location in bytes within the main file:
    ARROW_ASSIGN_OR_RAISE(table_data.file_start_offset, file->Tell());

    {
        // Stream out the reads table into the main file:
        ARROW_ASSIGN_OR_RAISE(
            auto reads_table_file_in, arrow::io::ReadableFile::Open(file_location.file_path, pool));
        ARROW_RETURN_NOT_OK(reads_table_file_in->Seek(file_location.offset));
        std::int64_t copied_bytes = 0;
        std::int64_t target_chunk_size = 10 * 1024 * 1024;  // Read in 10MB of data at a time
        while (copied_bytes < std::int64_t(file_location.size)) {
            std::size_t const to_read =
                std::min<std::int64_t>(file_location.size - copied_bytes, target_chunk_size);
            ARROW_ASSIGN_OR_RAISE(auto const read_buffer, reads_table_file_in->Read(to_read));
            copied_bytes += read_buffer->size();
            ARROW_RETURN_NOT_OK(file->Write(read_buffer));
        }

        // Store the reads file length for later reading:
        ARROW_ASSIGN_OR_RAISE(table_data.file_length, file->Tell());
        table_data.file_length -= table_data.file_start_offset;
    }

    if (cleanup_mode == SubFileCleanup::CleanupOriginalFile) {
        // Clean up the tmp read path:
        ARROW_ASSIGN_OR_RAISE(
            auto arrow_path,
            ::arrow::internal::PlatformFilename::FromString(file_location.file_path));
        ARROW_RETURN_NOT_OK(arrow::internal::DeleteFile(arrow_path));
    }

    return table_data;
}

inline arrow::Result<combined_file_utils::FileInfo> write_file_and_marker(
    arrow::MemoryPool * pool,
    std::shared_ptr<arrow::io::FileOutputStream> const & file,
    FileLocation const & file_location,
    SubFileCleanup cleanup_mode,
    boost::uuids::uuid const & section_marker)
{
    ARROW_ASSIGN_OR_RAISE(auto file_info, write_file(pool, file, file_location, cleanup_mode));
    // Pad file to 8 bytes and mark section:
    ARROW_RETURN_NOT_OK(combined_file_utils::pad_file(file, 8));
    ARROW_RETURN_NOT_OK(combined_file_utils::write_section_marker(file, section_marker));
    return file_info;
}

}}  // namespace pod5::combined_file_utils
