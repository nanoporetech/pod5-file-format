#pragma once

#include "mkr_format/mkr_format_export.h"

#include <arrow/extension_type.h>
#include <arrow/stl_iterator.h>
#include <boost/uuid/uuid.hpp>
#include <gsl/gsl-lite.hpp>

namespace mkr {

class MKR_FORMAT_EXPORT UuidArray : public arrow::ExtensionArray {
public:
    using IteratorType = arrow::stl::ArrayIterator<UuidArray>;

    using ExtensionArray::ExtensionArray;

    boost::uuids::uuid Value(int64_t i) const;

    // this isn't actually a view - it copies the data - but
    // (a) it's only 16 bytes, which is what a view (pointer + size) would require anyway
    // (b) arrow::std::ArrayIterator hard-codes the name of this method (even though it is supposed
    //     to be configurable via the ValueAccessor template parameter)
    boost::uuids::uuid GetView(int64_t i) const { return Value(i); }

    arrow::util::optional<boost::uuids::uuid> operator[](int64_t i) const {
        return *IteratorType(*this, i);
    }

    IteratorType begin() const { return IteratorType(*this); }
    IteratorType end() const { return IteratorType(*this, length()); }
};

class MKR_FORMAT_EXPORT UuidType : public arrow::ExtensionType {
public:
    UuidType() : ExtensionType(arrow::fixed_size_binary(16)) {}

    std::string extension_name() const override { return "minknow.uuid"; }
    bool ExtensionEquals(const ExtensionType &other) const override;
    std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;
    std::string Serialize() const override;
    arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
            std::shared_ptr<arrow::DataType> storage_type,
            const std::string &serialized_data) const override;
};

class MKR_FORMAT_EXPORT VbzSignalArray : public arrow::ExtensionArray {
public:
    using IteratorType = arrow::stl::ArrayIterator<VbzSignalArray>;

    gsl::span<std::uint8_t const> Value(int64_t i) const;

    using ExtensionArray::ExtensionArray;
};

class MKR_FORMAT_EXPORT VbzSignalType : public arrow::ExtensionType {
public:
    VbzSignalType() : ExtensionType(arrow::large_binary()) {}

    std::string extension_name() const override { return "minknow.vbz"; }
    bool ExtensionEquals(const ExtensionType &other) const override;
    std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;
    std::string Serialize() const override;
    arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
            std::shared_ptr<arrow::DataType> storage_type,
            const std::string &serialized_data) const override;
};

std::shared_ptr<VbzSignalType> vbz_signal();
std::shared_ptr<UuidType> uuid();

/// \brief Register all required extension types, lifetime
///        of types is maintained by the returned shared pointer.
MKR_FORMAT_EXPORT std::shared_ptr<void> register_extension_types();

}  // namespace mkr