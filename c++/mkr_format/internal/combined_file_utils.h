#pragma once

#include "footer_generated.h"
#include "mkr_format/version.h"

#include <arrow/io/file.h>
#include <arrow/util/endian.h>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <flatbuffers/flatbuffers.h>

#include <array>

namespace mkr {

namespace combined_file_utils {

static constexpr std::array<char, 8> FILE_SIGNATURE{'\213', 'M',  'K',    'R',
                                                    '\r',   '\n', '\032', '\n'};

inline mkr::Status padd_file(std::shared_ptr<arrow::io::OutputStream> const& sink,
                             std::uint32_t padd_to_size) {
    ARROW_ASSIGN_OR_RAISE(auto const current_byte_location, sink->Tell());
    auto const bytes_to_write = padd_to_size - (current_byte_location % padd_to_size);
    if (bytes_to_write == padd_to_size) {
        return mkr::Status::OK();
    }

    std::array<char, 128> zeroes{};
    return sink->Write(zeroes.data(), bytes_to_write);
}

inline mkr::Status write_file_signature(std::shared_ptr<arrow::io::OutputStream> const& sink) {
    return sink->Write(FILE_SIGNATURE.data(), FILE_SIGNATURE.size());
}
inline mkr::Status write_section_marker(std::shared_ptr<arrow::io::OutputStream> const& sink,
                                        boost::uuids::uuid const& section_marker) {
    return sink->Write(section_marker.begin(),
                       std::distance(section_marker.begin(), section_marker.end()));
}

inline mkr::Status write_combined_header(std::shared_ptr<arrow::io::OutputStream> const& sink,
                                         boost::uuids::uuid const& section_marker) {
    ARROW_RETURN_NOT_OK(write_file_signature(sink));
    return write_section_marker(sink, section_marker);
}

inline mkr::Status write_footer_magic(std::shared_ptr<arrow::io::OutputStream> const& sink) {
    return sink->Write("FOOTER\0\0", 8);
}

struct FileInfo {
    std::int64_t file_start_offset;
    std::int64_t file_length;
};

inline mkr::Result<std::int64_t> write_footer_flatbuffer(
        std::shared_ptr<arrow::io::OutputStream> const& sink,
        boost::uuids::uuid const& file_identifier,
        std::string const& software_name,
        FileInfo const& signal_table,
        FileInfo const& reads_table) {
    flatbuffers::FlatBufferBuilder builder(1024);

    auto signal_file = Minknow::ReadsFormat::CreateEmbeddedFile(
            builder, signal_table.file_start_offset, signal_table.file_length,
            Minknow::ReadsFormat::Format_FeatherV2, Minknow::ReadsFormat::ContentType_SignalTable);

    auto reads_file = Minknow::ReadsFormat::CreateEmbeddedFile(
            builder, reads_table.file_start_offset, reads_table.file_length,
            Minknow::ReadsFormat::Format_FeatherV2, Minknow::ReadsFormat::ContentType_ReadsTable);

    const std::vector<flatbuffers::Offset<Minknow::ReadsFormat::EmbeddedFile>> files{signal_file,
                                                                                     reads_file};
    auto footer = Minknow::ReadsFormat::CreateFooterDirect(
            builder, boost::uuids::to_string(file_identifier).c_str(), software_name.c_str(),
            MkrVersion.c_str(), &files);

    builder.Finish(footer);
    ARROW_RETURN_NOT_OK(sink->Write(builder.GetBufferPointer(), builder.GetSize()));
    return builder.GetSize();
}

inline mkr::Status write_footer(std::shared_ptr<arrow::io::OutputStream> const& sink,
                                boost::uuids::uuid const& section_marker,
                                boost::uuids::uuid const& file_identifier,
                                std::string const& software_name,
                                FileInfo const& signal_table,
                                FileInfo const& reads_table) {
    ARROW_RETURN_NOT_OK(write_footer_magic(sink));
    ARROW_ASSIGN_OR_RAISE(std::int64_t length,
                          write_footer_flatbuffer(sink, file_identifier, software_name,
                                                  signal_table, reads_table));
    ARROW_RETURN_NOT_OK(padd_file(sink, 8));

    std::int64_t padded_flatbuffer_size = arrow::bit_util::ToLittleEndian(length);
    ARROW_RETURN_NOT_OK(sink->Write(&padded_flatbuffer_size, sizeof(padded_flatbuffer_size)));

    ARROW_RETURN_NOT_OK(write_section_marker(sink, section_marker));
    return write_file_signature(sink);
}

struct ParsedFooter {
    boost::uuids::uuid file_identifier;
    std::string software_name;
    std::string writer_mkr_version;

    FileInfo reads_table;
    FileInfo signal_table;
};

inline mkr::Status check_signature(std::shared_ptr<arrow::io::RandomAccessFile> const& file,
                                   std::int64_t offset_in_file) {
    std::array<char, sizeof(FILE_SIGNATURE)> read_signature;
    ARROW_ASSIGN_OR_RAISE(auto read_bytes, file->ReadAt(offset_in_file, read_signature.size(),
                                                        read_signature.data()));
    if (read_bytes != (std::int16_t)read_signature.size() || read_signature != FILE_SIGNATURE) {
        return arrow::Status::IOError("Invalid signature in file");
    }

    return arrow::Status::OK();
}

inline mkr::Result<Minknow::ReadsFormat::Footer const*> read_footer_flatbuffer(
        std::vector<std::uint8_t> const& footer_data) {
    auto verifier = flatbuffers::Verifier(footer_data.data(), footer_data.size());
    if (!verifier.VerifyBuffer<Minknow::ReadsFormat::Footer>()) {
        return arrow::Status::IOError("Invalid footer found in file");
    }
    return flatbuffers::GetRoot<Minknow::ReadsFormat::Footer>(footer_data.data());
}

inline mkr::Result<ParsedFooter> read_footer(
        std::shared_ptr<arrow::io::RandomAccessFile> const& file) {
    // Verify signature at start and end of file:
    ARROW_RETURN_NOT_OK(check_signature(file, 0));
    ARROW_ASSIGN_OR_RAISE(auto const file_size, file->GetSize());
    ARROW_RETURN_NOT_OK(check_signature(file, file_size - FILE_SIGNATURE.size()));

    auto footer_length_data_end = file_size;
    footer_length_data_end -= FILE_SIGNATURE.size();
    footer_length_data_end -= sizeof(boost::uuids::uuid);

    std::int64_t footer_length = 0;
    ARROW_RETURN_NOT_OK(file->ReadAt(footer_length_data_end - sizeof(footer_length),
                                     sizeof(footer_length), &footer_length));
    footer_length = arrow::bit_util::FromLittleEndian(footer_length);

    std::vector<std::uint8_t> footer_data;
    footer_data.resize(footer_length);
    ARROW_ASSIGN_OR_RAISE(
            auto read_bytes,
            file->ReadAt(footer_length_data_end - sizeof(footer_length) - footer_length,
                         footer_length, footer_data.data()));
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
    } catch (boost::bad_lexical_cast const&) {
        return Status::IOError("Invalid file_identifier in file: '",
                               fb_footer->file_identifier()->str(), "'");
    }

    if (!fb_footer->software()) {
        return arrow::Status::IOError("Invalid footer software");
    }
    footer.software_name = fb_footer->software()->str();

    if (!fb_footer->mkr_version()) {
        return arrow::Status::IOError("Invalid footer mkr_version");
    }
    footer.writer_mkr_version = fb_footer->mkr_version()->str();

    if (!fb_footer->contents()) {
        return arrow::Status::IOError("Invalid footer contents");
    }
    for (auto const embedded_file : *fb_footer->contents()) {
        if (embedded_file->format() != Minknow::ReadsFormat::Format_FeatherV2) {
            return arrow::Status::IOError("Invalid embedded file format");
        }
        switch (embedded_file->content_type()) {
        case Minknow::ReadsFormat::ContentType_ReadsTable:
            footer.reads_table.file_start_offset = embedded_file->offset();
            footer.reads_table.file_length = embedded_file->length();
            break;
        case Minknow::ReadsFormat::ContentType_SignalTable:
            footer.signal_table.file_start_offset = embedded_file->offset();
            footer.signal_table.file_length = embedded_file->length();
            break;

        default:
            return arrow::Status::IOError("Unknown embedded file type");
        }
    }

    if (footer.reads_table.file_start_offset == 0 || footer.reads_table.file_length == 0) {
        return arrow::Status::IOError("Invalid reads table found in file.");
    }
    if (footer.signal_table.file_start_offset == 0 || footer.signal_table.file_length == 0) {
        return arrow::Status::IOError("Invalid signal table found in file.");
    }

    return footer;
}

}  // namespace combined_file_utils
}  // namespace mkr