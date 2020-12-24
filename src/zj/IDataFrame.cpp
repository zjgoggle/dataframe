#include "IDataFrame.h"

namespace zj
{

Global &global()
{
    static Global s;
    return s;
}

bool is_null( std::string_view s )
{
    return s == "N/A" || s == "n/a";
}

} // namespace zj
