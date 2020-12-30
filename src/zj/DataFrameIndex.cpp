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


template<bool ReturnVecOrSet>
auto findRows_Ordered_ISIN( const ConditionIsIn *pCondIsin, const MultiColOrderedIndex *pOrderedIndex )
{
    std::conditional_t<ReturnVecOrSet, std::vector<Rowindex>, std::unordered_set<Rowindex>> irows;
    // check all the possible values in condition. Usually the size of which is much less than size of dataframe.
    for ( const MultiColFieldsHashDelegate &delg : pCondIsin->m_val )
    {
        assert( delg.m_data.index() == 1 && "It's a Record type not a position!" );
        const Record &rec = std::get<1>( delg.m_data );
        auto range = pOrderedIndex->findEqualRange( rec );
        for ( size_t i = range.first, N = range.second; i < N; ++i )
            if constexpr ( ReturnVecOrSet )
                irows.push_back( pOrderedIndex->at( i ) );
            else
                irows.insert( pOrderedIndex->at( i ) );
    }
    return irows;
}
template<bool ReturnVecOrSet>
auto findRows_Ordered_EQ( const ConditionCompare *pCondCompare, const MultiColOrderedIndex *pOrderedIndex )
{
    std::conditional_t<ReturnVecOrSet, std::vector<Rowindex>, std::unordered_set<Rowindex>> irows;
    const Record &rec = pCondCompare->m_val;
    auto range = pOrderedIndex->findEqualRange( rec );
    for ( size_t i = range.first, N = range.second; i < N; ++i )
        if constexpr ( ReturnVecOrSet )
            irows.push_back( pOrderedIndex->at( i ) );
        else
            irows.insert( pOrderedIndex->at( i ) );
    return irows;
}

std::vector<Rowindex> getRowsNotInSorted( const IDataFrame *df, const std::vector<Rowindex> &excludeSortedRows )
{
    size_t N = df->size();
    assert( excludeSortedRows.empty() || excludeSortedRows.back() < N );
    std::vector<Rowindex> res;
    res.resize( df->size() - excludeSortedRows.size() );
    size_t pos = 0, startval = 0;
    for ( auto e : excludeSortedRows )
    {
        auto n = e - startval; // number of values to populate.
        std::iota( &res[pos], &res[pos + n], startval );
        pos += n;
        startval = e + 1;
    }
    std::iota( &res[pos], &res[pos + ( N - startval )], startval );
    return res;
}

template<bool ReturnVecOrSet>
auto getRowsNotInSet( const IDataFrame *df, const std::unordered_set<Rowindex> &excludeRows )
{
    std::conditional_t<ReturnVecOrSet, std::vector<Rowindex>, std::unordered_set<Rowindex>> res;
    res.reserve( df->size() - excludeRows.size() );
    for ( size_t i = 0, N = df->size(); i < N; ++i )
        if ( !excludeRows.count( i ) )
        {
            if constexpr ( ReturnVecOrSet )
                res.push_back( i );
            else
                res.insert( i );
        }
    return res;
}

std::pair<const MultiColOrderedIndex *, const MultiColHashMultiIndex *> findIndex( const DataFrameWithIndex *dfidx,
                                                                                   const std::vector<std::size_t> &icols )
{
    const MultiColOrderedIndex *pOrderIndex = nullptr;
    const MultiColHashMultiIndex *pHashIndex = nullptr;

    if ( auto pIt = dfidx->findIndex( IndexCategory::OrderedCat, icols ) )
    {
        assert( ( *pIt )->second.value.index() == 0 );
        pOrderIndex = &std::get<0>( ( *pIt )->second.value );
    }
    if ( auto pIt = dfidx->findIndex( IndexCategory::HashCat, icols ) )
    {
        assert( ( *pIt )->second.value.index() == 1 );
        pHashIndex = &std::get<1>( ( *pIt )->second.value );
    }
    return {pOrderIndex, pHashIndex};
}

template<bool ReturnVecOrSet>
auto findRowsByCondition( const DataFrameWithIndex *dfidx, ICondition *pCond, bool bEvaluateSlowPath )
{
    const IDataFrame *df = dfidx->getDataFarme();
    OperatorTag op = pCond->getOperator();
    const std::vector<size_t> &icols = pCond->getColIndices();
    auto [pOrderedIndex, pHashIndex] = findIndex( dfidx, icols );
    ConditionIsIn *pCondIsin = dynamic_cast<ConditionIsIn *>( pCond );
    ConditionCompare *pCondCompare = dynamic_cast<ConditionCompare *>( pCond );

    std::conditional_t<ReturnVecOrSet, std::vector<Rowindex>, std::unordered_set<Rowindex>> irows;
    auto addOneResult = [&]( Rowindex idx ) {
        if constexpr ( ReturnVecOrSet )
            irows.push_back( idx );
        else
            irows.insert( idx );
    };
    auto addAllResult = [&]() {
        if constexpr ( ReturnVecOrSet )
        {
            irows.resize( df->size() );
            std::iota( irows.begin(), irows.end(), 0 );
        }
        else
        {
            for ( size_t i = 0, N = df->size(); i < N; ++i )
                irows.insert( i );
        }
    };

    if ( pHashIndex )
    {
        if ( op == OperatorTag::ISIN )
        {
            assert( pCondIsin );
            return findRows_Hash_ISIN<ReturnVecOrSet>( pCondIsin, pHashIndex );
        }
        else if ( op == OperatorTag::EQ )
        {
            assert( pCondCompare );
            return findRows_Hash_EQ<ReturnVecOrSet>( pCondCompare, pHashIndex );
        }
        else if ( op == OperatorTag::NOTIN )
        {
            assert( pCondIsin );
            assert( !pCondIsin->m_isinOrNot );
            // find all the rows that are in Expr.values and exclude these rows from all the row numbers.
            // if number of notin values is small, exclude sorted vector; other wise, exclude set.
            if constexpr ( ReturnVecOrSet )
            {
                auto rowsToExclude = findRows_Hash_ISIN<true>( pCondIsin, pHashIndex ); // vector
                std::sort( rowsToExclude.begin(), rowsToExclude.end() ); //  todo: if rowsToExclude is large, convert it to set.
                return getRowsNotInSorted( df, rowsToExclude );
            }
            else
            {
                return getRowsNotInSet<false>( df, findRows_Hash_ISIN<false>( pCondIsin, pHashIndex ) ); // set
            }
        }
        else if ( op == OperatorTag::NE )
        {
            assert( pCondCompare );
            if constexpr ( ReturnVecOrSet )
            {
                auto rowsToExclude = findRows_Hash_EQ<true>( pCondCompare, pHashIndex );
                std::sort( rowsToExclude.begin(), rowsToExclude.end() ); //  todo: if rowsToExclude is large, convert it to set.
                return getRowsNotInSorted( df, rowsToExclude );
            }
            else
                return getRowsNotInSet<false>( df, findRows_Hash_EQ<false>( pCondCompare, pHashIndex ) ); // set
        }
    }
    if ( pOrderedIndex )
    {
        if ( op == OperatorTag::ISIN )
        {
            return findRows_Ordered_ISIN<ReturnVecOrSet>( pCondIsin, pOrderedIndex );
        }
        else if ( op == OperatorTag::EQ )
        {
            return findRows_Ordered_EQ<ReturnVecOrSet>( pCondCompare, pOrderedIndex );
        }
        else if ( op == OperatorTag::NOTIN )
        {
            if constexpr ( ReturnVecOrSet )
            {
                auto rowsToExclude = findRows_Ordered_ISIN<true>( pCondIsin, pOrderedIndex );
                std::sort( rowsToExclude.begin(), rowsToExclude.end() );
                return getRowsNotInSorted( df, rowsToExclude );
            }
            else
                return getRowsNotInSet<false>( df, findRows_Ordered_ISIN<false>( pCondIsin, pOrderedIndex ) );
        }
        else if ( op == OperatorTag::NE )
        {
            if constexpr ( ReturnVecOrSet )
            {
                auto rowsToExclude = findRows_Ordered_EQ<true>( pCondCompare, pOrderedIndex );
                std::sort( rowsToExclude.begin(), rowsToExclude.end() );
                return getRowsNotInSorted( df, rowsToExclude );
            }
            else
                return getRowsNotInSet<false>( df, findRows_Ordered_EQ<false>( pCondCompare, pOrderedIndex ) );
        }
        else if ( op == OperatorTag::GT )
        {
            if ( auto p0 = pOrderedIndex->findFirstGT( pCondCompare->m_val ) )
                for ( size_t i = *p0, N = pOrderedIndex->size(); i < N; ++i )
                    addOneResult( pOrderedIndex->at( i ) );
            return irows;
        }
        else if ( op == OperatorTag::GE )
        {
            if ( auto p0 = pOrderedIndex->findFirstGE( pCondCompare->m_val ) )
                for ( size_t i = *p0, N = pOrderedIndex->size(); i < N; ++i )
                    addOneResult( pOrderedIndex->at( i ) );
            return irows;
        }
        else if ( op == OperatorTag::LT )
        {
            if ( auto p0 = pOrderedIndex->findFirstGE( pCondCompare->m_val ) )
            {
                for ( size_t i = 0, N = *p0; i < N; ++i )
                    addOneResult( pOrderedIndex->at( i ) );
            }
            else // all elements are less than value.
                addAllResult();
            return irows;
        }
        else if ( op == OperatorTag::LE )
        {
            if ( auto p0 = pOrderedIndex->findFirstGT( pCondCompare->m_val ) )
            {
                for ( size_t i = 0, N = *p0; i < N; ++i )
                    addOneResult( pOrderedIndex->at( i ) );
            }
            else // all elements are LE value.
                addAllResult();
            return irows;
        }
    }
    if ( bEvaluateSlowPath )
        return dfidx->findRowsSlowPath<ReturnVecOrSet>( pCond );
    return irows;
}
std::vector<Rowindex> DataFrameWithIndex::findRows( Expr expr, bool bEvaluateSlowPath ) const
{
    std::stringstream err;
    IConditionPtr pCond = expr.toCondition( *m_pDataFrame, &err );
    if ( !pCond )
        throw std::runtime_error( "Expression Error: " + err.str() );
    return findRowsByCondition<true>( this, pCond.get(), bEvaluateSlowPath );
}
std::vector<Rowindex> DataFrameWithIndex::findRows( AndExpr expr ) const
{
    std::stringstream err;
    std::vector<IConditionPtr> andConds = expr.toCondition( *m_pDataFrame, &err );
    if ( andConds.empty() )
        throw std::runtime_error( "Add Error: " + err.str() );

    std::vector<Rowindex> irows;
    std::unordered_set<Rowindex> rowCandidates;

    auto evaluateFastPath = [&]( ICondition *pCond ) -> bool {
        if ( !rowCandidates.empty() ) // intersect existing candidates.
        {
            auto aCandidate = findRowsByCondition<false>( this, pCond, false ); // fast path only
            if ( aCandidate.empty() )
                return true; // exit
            std::unordered_set<Rowindex> c; // intersection.
            std::copy_if( rowCandidates.begin(), rowCandidates.end(), std::inserter( c, c.begin() ), [&]( const int element ) {
                return aCandidate.count( element ) > 0;
            } );
            if ( c.empty() )
                return true;
            rowCandidates = std::move( c );
        }
        else // no EQ condition
        {
            rowCandidates = findRowsByCondition<false>( this, pCond, false ); // fast path only
            if ( rowCandidates.empty() )
                return true; // exit
        }
        return false;
    }; // return empty() true to exit

    // 1. check EQ in fast path only
    for ( auto &pCond : andConds )
    {
        if ( pCond->getOperator() == OperatorTag::EQ )
        {
            if ( evaluateFastPath( pCond.get() ) )
                return {};
        }
    }
    // 2. check ISIN in fast path only
    for ( auto &pCond : andConds )
    {
        if ( pCond->getOperator() == OperatorTag::ISIN )
        {
            if ( evaluateFastPath( pCond.get() ) )
                return {};
        }
    }

    // if there are candidates, evaluate other conditions with candidates.
    if ( !rowCandidates.empty() )
    {
        for ( auto &pCond : andConds )
        {
            if ( pCond->getOperator() == OperatorTag::EQ || pCond->getOperator() == OperatorTag::ISIN )
                continue;
            for ( auto it = rowCandidates.begin(); it != rowCandidates.end(); ++it )
            {
                // todo
                // evaluate condition with current it
                // if false, remove it.
            }
            if ( rowCandidates.empty() )
                return {};
        }
    }
    //    else
    // otherwise slow path: evaluate row by row.

    // todo
    throw std::runtime_error( "Not Implemented slow path evaluate AndExpr!" );
    //
    return {};
}
std::vector<Rowindex> DataFrameWithIndex::findRows( OrExpr expr ) const
{
    // todo
    return {};
}

} // namespace zj
