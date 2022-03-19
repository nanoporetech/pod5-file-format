#include "mkr_format/types.h"

#include <arrow/array/array_binary.h>
#include <arrow/util/logging.h>

namespace mkr {

boost::uuids::uuid UuidArray::Value(int64_t i) const {
    auto const array = static_cast<arrow::FixedSizeBinaryArray const *>(storage().get());
    return *reinterpret_cast<boost::uuids::uuid const *>(array->GetValue(i));
}

bool UuidType::ExtensionEquals(const ExtensionType &other) const {
    // no parameters to consider
    return other.extension_name() == extension_name();
}
std::shared_ptr<arrow::Array> UuidType::MakeArray(std::shared_ptr<arrow::ArrayData> data) const {
    DCHECK_EQ(data->type->id(), arrow::Type::EXTENSION);
    DCHECK_EQ(static_cast<arrow::ExtensionType const &>(*data->type).extension_name(),
              extension_name());
    return std::make_shared<UuidArray>(data);
}
std::string UuidType::Serialize() const { return ""; }
arrow::Result<std::shared_ptr<arrow::DataType>> UuidType::Deserialize(
        std::shared_ptr<arrow::DataType> storage_type,
        const std::string &serialized_data) const {
    if (serialized_data != "") {
        return arrow::Status::Invalid("Unexpected type metadata: '", serialized_data, "'");
    }
    if (!storage_type->Equals(*arrow::fixed_size_binary(16))) {
        return arrow::Status::Invalid("Incorrect storage for UuidType: '", storage_type->ToString(),
                                      "'");
    }
    return std::make_shared<UuidType>();
}

std::shared_ptr<UuidType> uuid() { return std::make_shared<UuidType>(); }

std::shared_ptr<void> register_extension_types() {
    struct ExtensionTypes {
        ExtensionTypes() { arrow::RegisterExtensionType(uuid()); }

        ~ExtensionTypes() {
            if (arrow::GetExtensionType("minknow.uuid")) {
                arrow::UnregisterExtensionType("minknow.uuid");
            }
        }
    };

    return std::make_shared<ExtensionTypes>();
}

}  // namespace mkr