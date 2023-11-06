#include "pod5_format/types.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/util/logging.h>

namespace pod5 {

boost::uuids::uuid const * UuidArray::raw_values() const
{
    auto const array = std::static_pointer_cast<arrow::FixedSizeBinaryArray const>(storage());
    return reinterpret_cast<boost::uuids::uuid const *>(array->GetValue(0));
}

boost::uuids::uuid UuidArray::Value(int64_t i) const
{
    auto const array = std::static_pointer_cast<arrow::FixedSizeBinaryArray const>(storage());
    return *reinterpret_cast<boost::uuids::uuid const *>(array->GetValue(i));
}

bool UuidType::ExtensionEquals(ExtensionType const & other) const
{
    // no parameters to consider
    return other.extension_name() == extension_name();
}

std::shared_ptr<arrow::Array> UuidType::MakeArray(std::shared_ptr<arrow::ArrayData> data) const
{
    DCHECK_EQ(data->type->id(), arrow::Type::EXTENSION);
    DCHECK_EQ(
        static_cast<arrow::ExtensionType const &>(*data->type).extension_name(), extension_name());
    return std::make_shared<UuidArray>(data);
}

std::string UuidType::Serialize() const { return ""; }

arrow::Result<std::shared_ptr<arrow::DataType>> UuidType::Deserialize(
    std::shared_ptr<arrow::DataType> storage_type,
    std::string const & serialized_data) const
{
    if (serialized_data != "") {
        return arrow::Status::Invalid("Unexpected type metadata: '", serialized_data, "'");
    }
    if (!storage_type->Equals(*arrow::fixed_size_binary(16))) {
        return arrow::Status::Invalid(
            "Incorrect storage for UuidType: '", storage_type->ToString(), "'");
    }
    return std::make_shared<UuidType>();
}

gsl::span<std::uint8_t const> VbzSignalArray::Value(int64_t i) const
{
    auto const array = std::static_pointer_cast<arrow::LargeBinaryArray const>(storage());

    arrow::LargeBinaryArray::offset_type value_length = 0;
    auto value_ptr = array->GetValue(i, &value_length);
    return gsl::make_span(value_ptr, value_length);
}

std::shared_ptr<arrow::Buffer> VbzSignalArray::ValueAsBuffer(int64_t i) const
{
    auto const array = std::static_pointer_cast<arrow::LargeBinaryArray const>(storage());

    auto offset = array->value_offset(i);
    auto length = array->value_length(i);
    auto const value_data = array->value_data();

    return arrow::SliceBuffer(value_data, offset, length);
}

bool VbzSignalType::ExtensionEquals(ExtensionType const & other) const
{
    // no parameters to consider
    return other.extension_name() == extension_name();
}

std::shared_ptr<arrow::Array> VbzSignalType::MakeArray(std::shared_ptr<arrow::ArrayData> data) const
{
    DCHECK_EQ(data->type->id(), arrow::Type::EXTENSION);
    DCHECK_EQ(
        static_cast<arrow::ExtensionType const &>(*data->type).extension_name(), extension_name());
    return std::make_shared<VbzSignalArray>(data);
}

std::string VbzSignalType::Serialize() const { return ""; }

arrow::Result<std::shared_ptr<arrow::DataType>> VbzSignalType::Deserialize(
    std::shared_ptr<arrow::DataType> storage_type,
    std::string const & serialized_data) const
{
    if (serialized_data != "") {
        return arrow::Status::Invalid("Unexpected type metadata: '", serialized_data, "'");
    }
    if (!storage_type->Equals(*arrow::large_binary())) {
        return arrow::Status::Invalid(
            "Incorrect storage for VbzSignalType: '", storage_type->ToString(), "'");
    }
    return std::make_shared<VbzSignalType>();
}

std::unique_ptr<arrow::FixedSizeBinaryBuilder> make_read_id_builder(arrow::MemoryPool * pool)
{
    auto uuid_type = uuid();
    assert(uuid_type->id() == arrow::Type::EXTENSION);
    auto uuid_extension = std::static_pointer_cast<arrow::ExtensionType>(uuid_type);
    auto result =
        std::make_unique<arrow::FixedSizeBinaryBuilder>(uuid_extension->storage_type(), pool);
    assert(result->byte_width() == 16);
    return result;
}

std::shared_ptr<VbzSignalType> vbz_signal()
{
    static auto vbz_signal = std::make_shared<VbzSignalType>();
    return vbz_signal;
}

std::shared_ptr<UuidType> uuid()
{
    static auto uuid = std::make_shared<UuidType>();
    return uuid;
}

std::atomic<std::size_t> g_pod5_register_count(0);

pod5::Status register_extension_types()
{
    if (++g_pod5_register_count == 1) {
        ARROW_RETURN_NOT_OK(arrow::RegisterExtensionType(uuid()));
        ARROW_RETURN_NOT_OK(arrow::RegisterExtensionType(vbz_signal()));
    }
    return pod5::Status::OK();
}

pod5::Status unregister_extension_types()
{
    auto register_count = --g_pod5_register_count;
    if (register_count == 0) {
        if (arrow::GetExtensionType("minknow.uuid")) {
            ARROW_RETURN_NOT_OK(arrow::UnregisterExtensionType("minknow.uuid"));
        }
        if (arrow::GetExtensionType("minknow.vbz")) {
            ARROW_RETURN_NOT_OK(arrow::UnregisterExtensionType("minknow.vbz"));
        }
    }
    return pod5::Status::OK();
}

}  // namespace pod5
