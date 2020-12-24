#include "DataFrameIndex.h"

namespace zj
{

// return index handle
std::optional<IndexManager::iterator> IndexManager::addIndex( IndexType indexType,
                                                              std::vector<size_t> icols,
                                                              const std::string &indexName,
                                                              std::ostream *err )
{
    if ( !m_pDataFrame )
    {
        if ( err )
            *err << "AddIndex failed. DataFrame is not set.\n";
        return {};
    }
    if ( !indexName.empty() && m_nameMap.count( indexName ) )
    {
        if ( err )
            *err << "AddIndex failed. IndexName already exists:" << indexName << ".\n";
        return {};
    }

    IndexKey key{IndexCategory::HashCat, icols};
    IndexValue val;
    val.name = indexName;

    if ( indexType == IndexType::ReverseOrderedIndex || indexType == IndexType::OrderedIndex )
    {
        MultiColOrderedIndex index;
        index.create( *m_pDataFrame, std::move( icols ), indexType == IndexType::ReverseOrderedIndex );
        val.value = std::move( index );
        key.indexCategory = IndexCategory::OrderedCat;
    }
    else if ( indexType == IndexType::HashIndex || indexType == IndexType::HashMultiIndex )
    {
        MultiColHashMultiIndex index;
        index.create( *m_pDataFrame, std::move( icols ) );
        if ( indexType == IndexType::HashIndex && index.isMultiValue() )
        {
            if ( err )
                *err << "Failed to create HashIndex on cols:" << to_string( index.m_cols ) << ".\n";
            return {};
        }
        val.value = std::move( index );
    }
    else
    {
        if ( err )
            *err << "AddIndex failed: Invalid Index type: " << char( indexType ) << ".\n";
        return {};
    }
    if ( auto res = m_indexMap.emplace( std::move( key ), std::move( val ) ); res.second )
    {
        m_nameMap[indexName] = res.first;
        return res.first;
    }
    else
    {
        if ( err )
            *err << "AddIndex failed: duplicate key: " << key << ".\n";
        return {};
    }
}

} // namespace zj
