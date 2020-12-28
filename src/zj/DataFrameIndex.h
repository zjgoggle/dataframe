/*
 * This file is part of the ftl (Fast Template Library) distribution (https://github.com/zjgoggle/ftl).
 * Copyright (c) 2020 Jack Zhang.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <numeric>
#include <zj/IDataFrame.h>
#include <zj/Indexing.h>
#include <zj/DataFrameView.h>
#include <zj/RowDataFrame.h>
#include <zj/Condition.h>

namespace zj
{

enum class IndexCategory
{
    //        OrderedIndex,
    //        HashIndex,
    //        HashMultiIndex,
    OrderedCat, // MultiColOrderedIndex, // Ordered or ReverseOrdered
                //        MultiColHashIndex,
    HashCat, // MultiColHashMultiIndex // SingleValue or MultiValue
};
template<>
inline std::string to_string( const IndexCategory &v )
{
    if ( v == IndexCategory::OrderedCat )
        return "OrderedIndex";
    return "HashIndex";
}

/// \brief IndexKey : the key to find a index which is unique for a data frame.
/// Based on IndexType and column indicess, system auto determines whether it's SingleCol or MultiCol, HashIndex or HashMultiIndex.
struct IndexKey
{
    IndexCategory indexCategory; // OrderedIndex, HashIndex
    std::vector<size_t> cols; // column indicess

    bool operator==( const IndexKey &a ) const
    {
        return indexCategory == a.indexCategory && cols == a.cols;
    }
};
inline std::ostream &operator<<( std::ostream &os, const IndexKey &v )
{
    os << to_string( v.indexCategory ) << to_string( v.cols );
    return os;
}
template<>
struct hash_code<IndexKey>
{
    size_t operator()( const IndexKey &a ) const
    {
        return hash_combine( hashcode( a.indexCategory ), hashcode( a.cols ) );
    }
};

// df.where( Col("Age") > 10 && isin("Level", Set('A', 'B'))  )
// df.where( Col("A
class DataFrameWithIndex
{
public:
    using VarIndex = std::variant<MultiColOrderedIndex, MultiColHashMultiIndex>;
    struct IndexValue
    {
        std::string name;
        VarIndex value;
    };

    using IndexMap = std::unordered_map<IndexKey, IndexValue, HashCode>; // indexCategory+Cols as key
    using iterator = IndexMap::const_iterator;
    using IndexNameMap = std::unordered_map<std::string, iterator>;

protected:
    IndexMap m_indexMap;
    IndexNameMap m_nameMap; // <indexName, iteratorOfIndexMap>
    IDataFramePtr m_pDataFrame = nullptr;

public:
    DataFrameWithIndex( IDataFramePtr pdf ) : m_pDataFrame( pdf )
    {
    }

    void create( const std::vector<std::vector<std::string>> &rows, const ColumnDefs &columnDefs )
    {
        m_pDataFrame.reset( new RowDataFrame( rows, columnDefs ) );
    }

    template<class... T>
    void create( const std::vector<std::tuple<T...>> &tups, const std::vector<std::string> &colNames )
    {
        m_pDataFrame.reset( new RowDataFrame( tups, colNames ) );
    }

    size_t size() const
    {
        return m_pDataFrame->size();
    }

    /// \param indexName: optional, if it's non-empty, save it as named Index which can be removed.
    /// \return iterator of Index
    std::optional<iterator> addIndex( IndexType indexType,
                                      const std::vector<std::string> &colNames,
                                      const std::string &indexName = "",
                                      std::ostream *err = nullptr )
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
        return addIndex( indexType, m_pDataFrame->colIndex( colNames ), indexName, err );
    }

    std::optional<iterator> addIndex( IndexType indexType,
                                      std::vector<size_t> colIndices,
                                      const std::string &indexName = "",
                                      std::ostream *err = nullptr );

    bool removeIndex( const std::string &indexName )
    {
        if ( auto it = m_nameMap.find( indexName ); it != m_nameMap.end() )
        {
            m_indexMap.erase( it->second );
            m_nameMap.erase( it );
        }
        return false;
    }
    void clearIndex()
    {
        m_nameMap.clear();
        m_indexMap.clear();
    }

    //------------- Evaluate Expressions -----------------------

    DataFrameView select( Expr expr )
    {
        return select( std::vector<size_t>{}, std::move( expr ) );
    }
    DataFrameView select( const std::vector<std::string> &colnames, Expr expr )
    {
        return select( m_pDataFrame->colIndex( colnames ), std::move( expr ) );
    }

protected:
    DataFrameView select( std::vector<Rowindex> irows, std::vector<size_t> icols );
    DataFrameView select_rows( std::vector<Rowindex> irows );
    DataFrameView select_cols( std::vector<Rowindex> icols );

    /// \param colindices column indices to select. Select all columns if it's empty.
    /// throw runtime_error if expr is malformed, out_of_range if any col index is out of range.
    DataFrameView select( std::vector<size_t> colindices, Expr expr )
    {
        auto irows = findRows( std::move( expr ) );
        return colindices.empty() ? select_rows( std::move( irows ) ) : select( std::move( irows ), std::move( colindices ) );
    }

    std::optional<iterator> findIndex( IndexCategory cat, const std::vector<std::size_t> &icols ) const
    {
        if ( auto it = m_indexMap.find( IndexKey{cat, icols} ); it != m_indexMap.end() )
            return it;
        return {};
    }
    std::pair<const MultiColOrderedIndex *, const MultiColHashMultiIndex *> findIndex( const std::vector<std::size_t> &icols ) const
    {
        const MultiColOrderedIndex *pOrderIndex = nullptr;
        const MultiColHashMultiIndex *pHashIndex = nullptr;

        if ( auto pIt = findIndex( IndexCategory::OrderedCat, icols ) )
        {
            assert( ( *pIt )->second.value.index() == 0 );
            pOrderIndex = &std::get<0>( ( *pIt )->second.value );
        }
        if ( auto pIt = findIndex( IndexCategory::HashCat, icols ) )
        {
            assert( ( *pIt )->second.value.index() == 1 );
            pHashIndex = &std::get<1>( ( *pIt )->second.value );
        }
        return {pOrderIndex, pHashIndex};
    }

    /// Find a named index.
    std::optional<iterator> findIndex( const std::string &indexName ) const
    {
        if ( auto it = m_nameMap.find( indexName ); it != m_nameMap.end() )
            return it->second;
        return {};
    }

    std::vector<Rowindex> findRows( Expr expr ) const;

    std::vector<Rowindex> getRowsNotInSorted( const std::vector<Rowindex> &excludeSortedRows ) const
    {
        size_t N = m_pDataFrame->size();
        assert( excludeSortedRows.empty() || excludeSortedRows.back() < N );
        std::vector<Rowindex> res;
        res.resize( m_pDataFrame->size() - excludeSortedRows.size() );
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
    std::vector<Rowindex> getRowsNotInSet( const std::unordered_set<Rowindex> &excludeRows ) const
    {
        std::vector<Rowindex> res;
        res.reserve( m_pDataFrame->size() - excludeRows.size() );
        for ( size_t i = 0, N = m_pDataFrame->size(); i < N; ++i )
            if ( !excludeRows.count( i ) )
                res.push_back( i );
        return res;
    }

    friend std::ostream &operator<<( std::ostream &os, const DataFrameWithIndex &df );
};
inline std::ostream &operator<<( std::ostream &os, const DataFrameWithIndex &df )
{
    os << *df.m_pDataFrame;
    return os;
}
} // namespace zj
