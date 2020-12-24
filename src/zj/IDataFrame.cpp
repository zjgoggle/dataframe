#include "IDataFrame.h"

namespace zj
{

Global &global()
{
    static Global s;
    return s;
}

} // namespace zj
