#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"

#include <arrow/extension_type.h>
#include <arrow/stl_iterator.h>
#include <boost/uuid/uuid.hpp>
#include <gsl/gsl-lite.hpp>

namespace pod5 {

class POD5_FORMAT_EXPORT UuidArray : public arrow::ExtensionArray {
public:
    using IteratorType = arrow::stl::ArrayIterator<UuidArray>;

    using ExtensionArray::ExtensionArray;

    boost::uuids::uuid const * raw_values() const;

    boost::uuids::uuid Value(int64_t i) const;

    // this isn't actually a view - it copies the data - but
    // (a) it's only 16 bytes, which is what a view (pointer + size) would require anyway
    // (b) arrow::std::ArrayIterator hard-codes the name of this method (even though it is supposed
    //     to be configurable via the ValueAccessor template parameter)
    boost::uuids::uuid GetView(int64_t i) const { return Value(i); }

    arrow::util::optional<boost::uuids::uuid> operator[](int64_t i) const
    {
        return *IteratorType(*this, i);
    }

    IteratorType begin() const { return IteratorType(*this); }

    IteratorType end() const { return IteratorType(*this, length()); }
};

class POD5_FORMAT_EXPORT UuidType : public arrow::ExtensionType {
public:
    UuidType() : ExtensionType(arrow::fixed_size_binary(16)) {}

    std::string extension_name() const override { return "minknow.uuid"; }

    bool ExtensionEquals(ExtensionType const & other) const override;
    std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;
    std::string Serialize() const override;
    arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
        std::shared_ptr<arrow::DataType> storage_type,
        std::string const & serialized_data) const override;
};

class POD5_FORMAT_EXPORT VbzSignalArray : public arrow::ExtensionArray {
public:
    using IteratorType = arrow::stl::ArrayIterator<VbzSignalArray>;

    gsl::span<std::uint8_t const> Value(int64_t i) const;
    std::shared_ptr<arrow::Buffer> ValueAsBuffer(int64_t i) const;

    using ExtensionArray::ExtensionArray;
};

class POD5_FORMAT_EXPORT VbzSignalType : public arrow::ExtensionType {
public:
    VbzSignalType() : ExtensionType(arrow::large_binary()) {}

    std::string extension_name() const override { return "minknow.vbz"; }

    bool ExtensionEquals(ExtensionType const & other) const override;
    std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;
    std::string Serialize() const override;
    arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
        std::shared_ptr<arrow::DataType> storage_type,
        std::string const & serialized_data) const override;
};

std::shared_ptr<VbzSignalType> vbz_signal();
std::shared_ptr<UuidType> uuid();

/// \brief Register all required extension types.
POD5_FORMAT_EXPORT pod5::Status register_extension_types();

/// \brief Unregister all required extension types.
POD5_FORMAT_EXPORT pod5::Status unregister_extension_types();

}  // namespace pod5
