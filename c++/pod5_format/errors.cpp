#include "pod5_format/errors.h"

namespace pod5 {

class ErrorCategory : public std::error_category {
public:
    // Work around AppleClang issue:
    // https://stackoverflow.com/questions/7411515/why-does-c-require-a-user-provided-default-constructor-to-default-construct-a
    ErrorCategory() {}

    char const * name() const noexcept override { return "usb"; }

    std::string message(int code) const override
    {
        if (code == 0) {
            return "Success";
        }
        auto const error = static_cast<Errors>(code);

        switch (error) {
        case Errors::failed_to_append_data_to_batch:
            return "Failed to append data to batch";
        case Errors::failed_to_finish_building_column:
            return "Failed to finish building an arrow column";
        case Errors::failed_to_write_record_batch:
            return "Failed to write an arrow record batch";
        }

        return "unknown error";
    }

    std::error_condition default_error_condition(int code) const noexcept override
    {
        if (code == 0) {
            return std::error_condition(0, std::generic_category());
        }
        auto const error = static_cast<Errors>(code);

        switch (error) {
        case Errors::failed_to_append_data_to_batch:
            return std::errc::io_error;
        case Errors::failed_to_finish_building_column:
            return std::errc::io_error;
        case Errors::failed_to_write_record_batch:
            return std::errc::io_error;
        }

        return std::errc::io_error;
    }
};

std::error_category const & error_category()
{
    static const ErrorCategory category;
    return category;
}

}  // namespace pod5
