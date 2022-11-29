#include "pod5_format/read_table_utils.h"

#include <algorithm>

namespace pod5 {

ReadIdSearchInput::ReadIdSearchInput(gsl::span<boost::uuids::uuid const> const & input_ids)
: m_search_read_ids(input_ids.size())
{
    // Copy in search input:
    for (std::size_t i = 0; i < input_ids.size(); ++i) {
        m_search_read_ids[i].id = input_ids[i];
        m_search_read_ids[i].index = i;
    }

    // Sort input based on read id:
    std::sort(
        m_search_read_ids.begin(), m_search_read_ids.end(), [](auto const & a, auto const & b) {
            return a.id < b.id;
        });
}

}  // namespace pod5
