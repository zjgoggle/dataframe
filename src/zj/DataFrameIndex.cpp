#include "DataFrameIndex.h"
#include <sstream>

namespace zj
{

// return index handle
std::optional<DataFrameWithIndex::iterator> DataFrameWithIndex::addIndex( IndexType indexType,
                                                                          std::vector<size_t> icols,
                                                                          const std::string &indexName,
                                                                          std::ostream *err )
{
    if ( !m_pDataFrame )
    {
        throw std::runtime_error( "AddIndex failed. DataFrame is not set." );
    }
    if ( !indexName.empty() && m_nameMap.count( indexName ) )
    {
        throw std::runtime_error( "AddIndex failed. IndexName already exists:" + indexName );
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
        if ( !indexName.empty() )
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
DataFrameView DataFrameWithIndex::select_rows( std::vector<Rowindex> irows )
{
    std::stringstream err;
    DataFrameView view;
    if ( !view.create_row_view( *m_pDataFrame, std::move( irows ), &err ) )
        throw std::runtime_error( "select_rows Error: " + err.str() );
    return view;
}

DataFrameView DataFrameWithIndex::select_cols( std::vector<Rowindex> icols )
{
    std::stringstream err;
    DataFrameView view;
    if ( !view.create_column_view( *m_pDataFrame, std::move( icols ), &err ) )
        throw std::runtime_error( "select_cols Error: " + err.str() );
    return view;
}

DataFrameView DataFrameWithIndex::select( std::vector<Rowindex> irows, std::vector<size_t> icols )
{
    std::stringstream err;
    DataFrameView view;
    if ( !view.create( *m_pDataFrame, std::move( irows ), std::move( icols ), &err ) )
        throw std::runtime_error( "select rows and cols Error: " + err.str() );
    return view;
}

template<bool ReturnVecOrSet>
auto findRows_Hash_ISIN( const ConditionIsIn *pCondIsin, const MultiColHashMultiIndex *pHashIndex )
{
    std::conditional_t<ReturnVecOrSet, std::vector<Rowindex>, std::unordered_set<Rowindex>> irows;
    // check all the possible values in condition. Usually the size of which is much less than size of dataframe.
    for ( const MultiColFieldsHashDelegate &delg : pCondIsin->m_val )
    {
        assert( delg.m_data.index() == 1 && "It's a Record type not a position!" );
        const Record &rec = std::get<1>( delg.m_data );
        std::cout << " looking for hash key:" << to_string( rec ) << std::endl;
        if ( auto prows = pHashIndex->at( rec ) )
            if constexpr ( ReturnVecOrSet )
                irows.insert( irows.end(), prows->begin(), prows->end() );
            else
                irows.insert( prows->begin(), prows->end() );
    }
    return irows;
}

template<bool ReturnVecOrSet>
auto findRows_Hash_EQ( const ConditionCompare *pCondCompare, const MultiColHashMultiIndex *pHashIndex )
{
    std::conditional_t<ReturnVecOrSet, std::vector<Rowindex>, std::unordered_set<Rowindex>> irows;
    const Record &rec = pCondCompare->m_val;
    if ( auto prows = pHashIndex->at( rec ) )
        if constexpr ( ReturnVecOrSet )
            irows.insert( irows.end(), prows->begin(), prows->end() );
        else
            irows.insert( prows->begin(), prows->end() );
    return irows;
}

std::vector<Rowindex> DataFrameWithIndex::findRows( Expr expr ) const
{
    std::stringstream err;
    IConditionPtr pCond = expr.toCondition( *m_pDataFrame, &err );
    if ( !pCond )
        throw std::runtime_error( "Expression Error: " + err.str() );

    OperatorTag op = pCond->getOperator();
    const std::vector<size_t> &icols = pCond->getColIndices();
    auto [pOrderedIndex, pHashIndex] = findIndex( icols );
    ConditionIsIn *pCondIsin = dynamic_cast<ConditionIsIn *>( pCond.get() );
    ConditionCompare *pCondCompare = dynamic_cast<ConditionCompare *>( pCond.get() );

    if ( pHashIndex )
    {
        if ( op == OperatorTag::ISIN )
        {
            assert( pCondIsin );
            return findRows_Hash_ISIN<true>( pCondIsin, pHashIndex );
        }
        else if ( op == OperatorTag::EQ )
        {
            assert( pCondCompare );
            return findRows_Hash_EQ<true>( pCondCompare, pHashIndex );
        }
        else if ( op == OperatorTag::NOTIN )
        {
            assert( pCondIsin );
            assert( !pCondIsin->m_isinOrNot );
            // find all the rows that are in Expr.values and exclude these rows from all the row numbers.
            // if number of notin values is small, exclude sorted vector; other wise, exclude set.
            auto rowsToExclude = findRows_Hash_ISIN<true>( pCondIsin, pHashIndex );
            std::sort( rowsToExclude.begin(), rowsToExclude.end() ); //  todo: if rowsToExclude is large, convert it to set.
            return getRowsNotInSorted( rowsToExclude );
        }
        else if ( op == OperatorTag::NE )
        {
            assert( pCondCompare );
            auto rowsToExclude = findRows_Hash_EQ<true>( pCondCompare, pHashIndex );
            std::sort( rowsToExclude.begin(), rowsToExclude.end() ); //  todo: if rowsToExclude is large, convert it to set.
            return getRowsNotInSorted( rowsToExclude );
        }
    }
    if ( pOrderedIndex )
    {
        if ( op == OperatorTag::ISIN )
        {
            // todo
        }
    }

    // todo search dataframe by rows.
    {
        throw std::runtime_error( "Not Implemented error: " + to_string( expr ) );
    }
}

} // namespace zj
