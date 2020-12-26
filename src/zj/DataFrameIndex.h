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

namespace zj
{



///////////////////////////////////////////////////////////////////////////
/// HashMultiIndex
///////////////////////////////////////////////////////////////////////////


// Positions are saved in Index.
template<class FieldHashDelegateT>
struct HashIndexBase // MultiColHashIndex
{
    using IndexSet = std::unordered_set<FieldHashDelegateT, HashCode>;
    using iterator = typename IndexSet::const_iterator;
    using RecordType = typename FieldHashDelegateT::value_type;

protected:
    IndexSet m_indices;

public:
    std::optional<Rowindex> at( const RecordType &key ) const
    {
        if ( auto it = m_indices.find( FieldHashDelegateT{key} ); it != m_indices.end() )
            return it->index();
        return {};
    }
    size_t operator[]( const RecordType &key ) const
    {
        if ( auto v = at( key ) )
            return *v;
        throw std::out_of_range( "MultiColHashIndex:key:" + to_string( key ) );
    }
    size_t size() const
    {
        return m_indices.size();
    }
};

struct MultiColHashIndex : public HashIndexBase<MultiColFieldsHashDelegate>
{
    ICols m_cols;

    // icol will not be verified.
    // return false if there are duplicate values
    bool create( const IDataFrame &df, std::vector<size_t> icols, std::ostream *err = nullptr )
    {
        m_indices.clear();
        m_cols = std::move( icols );
        for ( size_t i = 0u, N = df.countRows(); i < N; ++i )
        {
            MultiColFieldsHashDelegate val{MultiColFieldsHashDelegate::position_type{&df, i, &m_cols}};
            if ( !m_indices.insert( val ).second )
            {
                if ( err )
                    *err << "Failed to create MultiColHashIndex for cols:" << to_string( df.colName( icols ) ) << ". Found dupliate record: at row"
                         << to_string( df.getRowRef( i, m_cols ) ) << ".\n";
                return false;
            }
        }
        return true;
    }
    bool create( const IDataFrame &df, const std::vector<std::string> &colNames, std::ostream *err = nullptr )
    {
        if ( auto icols = df.colIndex( colNames ); !icols.empty() )
        {
            return create( df, std::move( icols ), err );
        }
        return false;
    }
};

// Positions are saved in Index.
struct HashIndex : public HashIndexBase<FieldHashDelegate>
{
    size_t m_cols;
    // icol will not be verified.
    // return false if there are duplicate values
    bool create( const IDataFrame &df, size_t icol, std::ostream *err = nullptr )
    {
        m_indices.clear();
        m_cols = icol;
        for ( auto i = 0u; i < df.countRows(); ++i )
        {
            FieldHashDelegate val{FieldHashDelegate::position_type{&df, i, &m_cols}};
            if ( !m_indices.insert( val ).second )
            {
                if ( err )
                    *err << "Failed to create HashIndex at col:" << df.colName( icol ) << ". Found dupliate field:" << to_string( val.get() )
                         << ".\n";
                return false;
            }
        }
        return true;
    }
    bool create( const IDataFrame &df, const std::string &colName, std::ostream *err = nullptr )
    {
        return create( df, df.colIndex( colName ), err );
    }
};

///////////////////////////////////////////////////////////////////////////
/// HashIndex
///////////////////////////////////////////////////////////////////////////

// Positions are saved in Index.
/// Single-column as key, vector of row index as mapped values.
template<class FieldHashDelegateT>
struct HashMultiIndexBase
{
    using IndexMap = std::unordered_map<FieldHashDelegateT, std::vector<size_t>, HashCode>;
    using iterator = typename IndexMap::const_iterator;
    using RecordType = typename FieldHashDelegateT::value_type;

protected:
    IndexMap m_indices;
    // if true, each key has multi values; otherwise, each key has single value. it's determined when index is created.
    bool m_isMultiValue = false;

public:
    const std::vector<size_t> *at( const RecordType &key ) const
    {
        if ( auto it = m_indices.find( FieldHashDelegateT{key} ); it != m_indices.end() )
            return &it->second;
        return nullptr;
    }
    std::vector<size_t> operator[]( const RecordType &key ) const
    {
        if ( auto v = at( key ) )
            return *v;
        throw std::out_of_range( "MultiColHashIndex:key:" + to_string( key ) );
    }
    size_t size() const
    {
        return m_indices.size();
    }
    bool isMultiValue() const
    {
        return m_isMultiValue;
    }
};

struct HashMultiIndex : public HashMultiIndexBase<FieldHashDelegate>
{
    size_t m_cols;
    // icol will not be verified.
    void create( const IDataFrame &df, size_t icol )
    {
        m_indices.clear();
        m_cols = icol;
        m_isMultiValue = false;
        for ( auto i = 0u; i < df.countRows(); ++i )
        {
            FieldHashDelegate val{typename FieldHashDelegate::position_type{&df, i, &m_cols}};
            auto &mapped = m_indices[val];
            mapped.push_back( i );
            if ( mapped.size() > 1 )
                m_isMultiValue = true;
        }
    }
    void create( const IDataFrame &df, const std::string &colName )
    {
        create( df, df.colIndex( colName ) );
    }
};

// Positions are saved in Index.
/// key:[rowindices]
struct MultiColHashMultiIndex : public HashMultiIndexBase<MultiColFieldsHashDelegate>
{
    ICols m_cols;

    // icol will not be verified.
    void create( const IDataFrame &df, std::vector<size_t> icols )
    {
        m_indices.clear();
        m_cols = std::move( icols );
        for ( auto i = 0u; i < df.size(); ++i )
        {
            MultiColFieldsHashDelegate val{MultiColFieldsHashDelegate::position_type{&df, i, &m_cols}};
            auto &mapped = m_indices[val];
            mapped.push_back( i );
            if ( mapped.size() > 1 )
                m_isMultiValue = true;
        }
    }
    void create( const IDataFrame &df, const std::vector<std::string> &colNames )
    {
        create( df, df.colIndex( colNames ) );
    }
};

///////////////////////////////////////////////////////////////////////////
/// OrderedIndex
///////////////////////////////////////////////////////////////////////////



template<bool isSingleColT>
struct OrderedIndexBase
{
    static constexpr bool isSingleCol = isSingleColT;
    using RecordRef = RecordOrFieldRef<isSingleColT>;
    using RecordType = typename RecordRef::RecordType;
    using ColsType = typename RecordRef::ColsType;

    struct LessThan
    {
        const IDataFrame *m_pDataFrame = nullptr;
        const ColsType *m_colIndices = nullptr;
        bool m_bReverseOrder = false;

        bool operator()( const RecordType &a, const RecordType &b ) const
        {
            if ( m_bReverseOrder )
                return b < a;
            else
                return a < b;
        }
        bool operator()( const RecordType &a, size_t irow ) const
        {
            if ( m_bReverseOrder )
                return RecordRef{m_pDataFrame, irow, m_colIndices} < a;
            else
                return a < RecordRef{m_pDataFrame, irow, m_colIndices};
        }
        bool operator()( size_t irow, const RecordType &b ) const
        {
            if ( m_bReverseOrder )
                return b < RecordRef{m_pDataFrame, irow, m_colIndices};
            else
                return RecordRef{m_pDataFrame, irow, m_colIndices} < b;
        }
        bool operator()( size_t irow1, size_t irow2 ) const
        {
            if ( m_bReverseOrder )
                return RecordRef{m_pDataFrame, irow2, m_colIndices} < RecordRef{m_pDataFrame, irow1, m_colIndices};
            else
                return RecordRef{m_pDataFrame, irow1, m_colIndices} < RecordRef{m_pDataFrame, irow2, m_colIndices};
        }
    };

protected:
    const IDataFrame *m_pDataFrame = nullptr;
    ColsType m_cols; // vector<size_t> or size_t
    std::vector<Rowindex> m_indices;
    bool m_bReverseOrder;

    using iterator = std::vector<Rowindex>::const_iterator;

public:
    void create( const IDataFrame &df, std::conditional_t<isSingleCol, size_t, std::vector<size_t>> icols, bool bReverseOrder = false )
    {
        m_pDataFrame = &df;
        m_cols = std::move( icols );
        m_indices.resize( df.countRows() );
        std::iota( m_indices.begin(), m_indices.end(), 0 );
        m_bReverseOrder = bReverseOrder;
        sortRows();
    }

    const std::vector<Rowindex> &getRowIndices() const
    {
        return m_indices;
    }
    // get the row index of the element that is in n-th position of Index.
    std::optional<Rowindex> at( size_t nth ) const
    {
        if ( nth < size() )
            return getRowIndices()[nth];
        return {};
    }

    /// Find the first element >= val.
    /// \return the index in sorted Index; empty if all elements < val.
    std::optional<size_t> findFirstGE( const Record &val, size_t pos = 0, size_t end = 0 ) const
    {
        if ( pos >= size() || ( end != 0 && pos > end ) )
            throw std::out_of_range( "findFirstGE" );
        // lower_bound.
        iterator itEnd = end == 0 ? m_indices.end() : std::next( m_indices.begin(), end );
        if ( iterator it = lower_bound( val, std::next( m_indices.begin(), pos ), itEnd ); it != itEnd )
            return std::distance( m_indices.begin(), it );
        return {};
    }
    /// Find the first element > val.
    /// \return the index in sorted Index; empty if all elements <= val.
    std::optional<size_t> findFirstGT( const Record &val, size_t pos = 0, size_t end = 0 ) const
    {
        if ( pos >= size() || ( end != 0 && pos > end ) )
            throw std::out_of_range( "findFirstGT" );
        // upper_bound.
        iterator itEnd = end == 0 ? m_indices.end() : std::next( m_indices.begin(), end );
        if ( iterator it = upper_bound( val, std::next( m_indices.begin(), pos ), itEnd ); it != itEnd )
            return std::distance( m_indices.begin(), it );
        return {};
    }

    /// Find the first element == val.
    /// \return index_in_sorted_Index
    std::optional<size_t> findFirst( const RecordType &val, size_t pos = 0, size_t end = 0 ) const
    {
        if ( pos >= size() || ( end != 0 && pos > end ) )
            throw std::out_of_range( "findFirst" );
        iterator itEnd = end == 0 ? m_indices.end() : std::next( m_indices.begin(), end );
        if ( iterator it = lower_bound( val, std::next( m_indices.begin(), pos ), itEnd );
             it != itEnd && RecordRef{m_pDataFrame, *it, &m_cols} == val )
            return std::distance( m_indices.begin(), it );
        return {};
    }
    /// Find the last element == val.
    /// \return index_in_sorted_Index
    std::optional<size_t> findLast( const RecordType &val, size_t pos = 0, size_t end = 0 ) const
    {
        if ( pos >= size() || ( end != 0 && pos > end ) )
            throw std::out_of_range( "findLast" );
        iterator itBegin = std::next( m_indices.begin(), pos ), itEnd = end == 0 ? m_indices.end() : std::next( m_indices.begin(), end );
        iterator it = upper_bound( val, itBegin, itEnd );
        if ( it != itBegin )
        {
            it = std::prev( it, 1 );
            if ( RecordRef{m_pDataFrame, *it, &m_cols} == val )
                return std::distance( m_indices.begin(), it );
        }
        return {};
    }
    /// \return index of i-th record.
    Rowindex operator[]( size_t k ) const
    {
        if ( auto v = at( k ) )
            return *v;
        throw std::out_of_range( "MultiColOrderedIndex:key:" + to_string( k ) );
    }
    size_t size() const
    {
        return m_indices.size();
    }

protected:
    void sortRows()
    {
        std::sort( m_indices.begin(), m_indices.end(), LessThan{m_pDataFrame, &m_cols, m_bReverseOrder} );
    }
    iterator lower_bound( const RecordType &val, iterator itBegin, iterator itEnd ) const
    {
        return std::lower_bound( itBegin, itEnd, val, LessThan{m_pDataFrame, &m_cols, m_bReverseOrder} );
    }
    iterator upper_bound( const RecordType &val, iterator itBegin, iterator itEnd ) const
    {
        return std::upper_bound( itBegin, itEnd, val, LessThan{m_pDataFrame, &m_cols, m_bReverseOrder} );
    }
};

struct MultiColOrderedIndex : public OrderedIndexBase<false>
{
    using BaseType = OrderedIndexBase<false>;
    using BaseType::create;

    void create( const IDataFrame &df, const std::vector<std::string> &colNames, bool bReverseOrder = false )
    {
        create( df, df.colIndex( colNames ), bReverseOrder );
    }
};

struct OrderedIndex : public OrderedIndexBase<true>
{
    using BaseType = OrderedIndexBase<true>;
    using BaseType::create;

    bool create( const IDataFrame &df, const std::string &colName, bool bReverse = false )
    {
        create( df, df.colIndex( colName ), bReverse );
        return true;
    }
};

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
std::string to_string( const IndexCategory &v )
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
class IndexManager
{
public:
    using VarIndex = std::variant<MultiColOrderedIndex, MultiColHashMultiIndex>;
    struct IndexValue
    {
        std::string name;
        VarIndex value;
    };

    using IndexMap = std::unordered_map<IndexKey, IndexValue, HashCode>;
    using iterator = IndexMap::const_iterator;
    using IndexNameMap = std::unordered_map<std::string, iterator>;

protected:
    IndexMap m_indexMap;
    IndexNameMap m_nameMap;
    IDataFrame *m_pDataFrame = nullptr;

public:
    IndexManager( IDataFrame *pDataFrame ) : m_pDataFrame( pDataFrame )
    {
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

    std::optional<iterator> findIndex( IndexCategory cat, const std::vector<std::size_t> &icols ) const
    {
        if ( auto it = m_indexMap.find( IndexKey{cat, icols} ); it != m_indexMap.end() )
            return it;
        return {};
    }

    std::optional<iterator> findIndex( const std::string &indexName ) const
    {
        if ( auto it = m_nameMap.find( indexName ); it != m_nameMap.end() )
            return it->second;
        return {};
    }

    bool removeIndex( const std::string &indexName )
    {
        if ( auto it = m_nameMap.find( indexName ); it != m_nameMap.end() )
        {
            m_indexMap.erase( it->second );
            m_nameMap.erase( it );
        }
        return false;
    }
};
} // namespace zj
