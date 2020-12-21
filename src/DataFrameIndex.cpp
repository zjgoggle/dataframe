#include "DataFrameIndex.h"

namespace df
{

// return index handle
std::optional<IndexManager::IndexMap::iterator> IndexManager::addIndex( const std::string &indexName,
                                                                        IndexType indexType,
                                                                        const std::vector<std::string> &colNames,
                                                                        std::ostream *err )
{
    if ( !m_pDataFrame )
    {
        if ( err )
            *err << "AddIndex failed. DataFrame is not set.\n";
        return {};
    }
    if ( colNames.size() == 0 )
    {
        if ( err )
            *err << "AddIndex failed: empty column names!.\n";
        return {};
    }
    if ( m_indexMap.count( indexName ) )
    {
        if ( err )
            *err << "AddIndex failed: index name exists already:" << indexName << ".\n";
        return {};
    }
    auto icols = m_pDataFrame->colIndice( colNames );
    if ( icols.empty() )
    {
        if ( err )
            *err << "AddIndex failed: couldn't find column names: " << to_string( colNames ) << ".\n";
        return {};
    }
    bool bMultiCol = colNames.size() > 1;
    VarIndex varindex;

    if ( ( indexType == IndexType::ReverseOrderedIndex || indexType == IndexType::OrderedIndex ) && !bMultiCol )
    {
        OrderedIndex index;
        index.create( *m_pDataFrame, icols[0], indexType == IndexType::ReverseOrderedIndex );
        varindex = std::move( index );
    }
    else if ( indexType == IndexType::HashIndex && !bMultiCol )
    {
        HashIndex index;
        if ( !index.create( *m_pDataFrame, icols[0], err ) )
            return {};
        varindex = std::move( index );
    }
    else if ( indexType == IndexType::HashMultiIndex && !bMultiCol )
    {
        HashMultiIndex index;
        index.create( *m_pDataFrame, icols[0] );
        varindex = std::move( index );
    }
    else if ( ( indexType == IndexType::ReverseOrderedIndex || indexType == IndexType::OrderedIndex ) && bMultiCol )
    {
        MultiColOrderedIndex index;
        index.create( *m_pDataFrame, icols );
        varindex = std::move( index );
    }
    else if ( indexType == IndexType::HashIndex && bMultiCol )
    {
        MultiColHashIndex index;
        if ( !index.create( *m_pDataFrame, icols, err ) )
            return {};
        varindex = std::move( index );
    }
    else if ( indexType == IndexType::HashMultiIndex && bMultiCol )
    {
        MultiColHashMultiIndex index;
        if ( !index.create( *m_pDataFrame, icols ) )
            return {};
        varindex = std::move( index );
    }
    else
    {
        if ( err )
            *err << "AddIndex failed: Invalid Index type: " << char( indexType ) << ".\n";
        return {};
    }
    return m_indexMap.emplace( indexName, varindex ).first;
}

} // namespace df
