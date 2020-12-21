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
#include <IDataFrame.h>

namespace df
{

using Rowindex = size_t;

struct FieldPos
{
    const IDataFrame *df = nullptr;
    Rowindex irow = 0;
    size_t icol = 0;

    const VarField &getValue() const
    {
        assert( df && "FieldDelegate not initialized!" );
        return df->at( irow, icol );
    }
};
template<>
struct hash_code<FieldPos>
{
    size_t operator()( const FieldPos &v ) const
    {
        return hashcode( v.getValue() );
    }
};
bool operator<( const FieldPos &a, const FieldPos &b )
{
    return a.getValue() < b.getValue();
}
bool operator==( const FieldPos &a, const FieldPos &b )
{
    return ( a.df == b.df && a.icol == b.icol && a.irow == b.irow ) || a.getValue() == b.getValue();
}


struct FieldDelegate
{
    using position_type = FieldPos;
    using value_type = VarField;
    std::variant<FieldPos, VarField> m_data; // VarField is only used for hash lookup

    bool has_value() const
    {
        return m_data.index() == 1 || std::get<0>( m_data ).df;
    }

    // return row index
    size_t index() const
    {
        if ( m_data.index() == 0 )
        {
            const auto &pos = std::get<0>( m_data );
            assert( pos.df && "FieldDelegate not initialized!" );
            return pos.irow;
        }
        else
        {
            throw std::invalid_argument( "FieldDelegate has VarField. Expected Pos!" );
        }
    }
    const VarField &get() const
    {
        if ( m_data.index() == 0 )
        {
            return std::get<0>( m_data ).getValue();
        }
        else
            return std::get<1>( m_data );
    }
    const VarField &operator*() const
    {
        return get();
    }
};
template<>
struct hash_code<FieldDelegate>
{
    size_t operator()( const FieldDelegate &a ) const
    {
        return hashcode( a.m_data );
    }
};

bool operator==( const FieldDelegate &a, const FieldDelegate &b )
{
    return a.get() == b.get();
}
// <
bool operator<( const FieldDelegate &a, const FieldDelegate &b )
{
    return a.get() < b.get();
}
bool operator<( const VarField &a, const FieldDelegate &b )
{
    return a < b.get();
}
bool operator<( const FieldDelegate &a, const VarField &b )
{
    return a.get() < b;
}
// ==

bool operator==( const VarField &a, const FieldDelegate &b )
{
    return a == b.get();
}
bool operator==( const FieldDelegate &a, const VarField &b )
{
    return a.get() == b;
}

struct MultiColPos
{
    const IDataFrame *df = nullptr;
    Rowindex irow = 0;
    std::vector<size_t> icols; // multi-column

    mutable std::optional<Record> m_cache; // cache

    // get or create record
    const Record &getValue() const
    {
        assert( df && "MultiColFieldsDelegate must be inited!" );
        if ( !m_cache )
        {
            Record rec;
            for ( size_t i = 0; i < icols.size(); ++i )
                rec.push_back( df->at( irow, i ) );
            m_cache = std::move( rec );
        }
        return *m_cache;
    }
    const VarField &getNth( size_t nthField ) const
    {
        assert( df && "MultiColFieldsDelegate must be inited!" );
        return df->at( irow, icols[nthField] );
    }
    const VarField &operator[]( size_t i ) const
    {
        return getNth( i );
    }

    size_t size() const
    {
        return icols.size();
    }
};

template<>
struct hash_code<MultiColPos>
{
    size_t operator()( const MultiColPos &v ) const
    {
        return VecHash()( v );
    }
};
bool operator==( const MultiColPos &a, const MultiColPos &b )
{
    return VecEqual()( a, b );
}


// multi-column Fields
struct MultiColFieldsDelegate
{
    using position_type = MultiColPos;
    using value_type = Record;

    std::variant<MultiColPos, Record> m_data; // Record is only used for hash lookup

    bool has_value() const
    {
        return m_data.index() == 1 || std::get<0>( m_data ).df;
    }

    // return row index
    size_t index() const
    {
        if ( m_data.index() == 0 )
        {
            const auto &pos = std::get<0>( m_data );
            assert( pos.df && "FieldDelegate not initialized!" );
            return pos.irow;
        }
        else
        {
            throw std::invalid_argument( "FieldDelegate has VarField. Expected Pos!" );
        }
    }
    const Record &get() const
    {
        if ( m_data.index() == 0 )
        {
            return std::get<0>( m_data ).getValue();
        }
        else
            return std::get<1>( m_data );
    }
    const Record &operator*() const
    {
        return get();
    }
};

template<>
struct hash_code<MultiColFieldsDelegate>
{
    size_t operator()( const MultiColFieldsDelegate &a ) const
    {
        return hashcode( a.m_data );
    }
};
bool operator==( const MultiColFieldsDelegate &a, const MultiColFieldsDelegate &b )
{
    int x = a.m_data.index() | ( b.m_data.index() << 1 );
    switch ( x )
    {
    case 0:
        return VecEqual()( std::get<0>( a.m_data ), std::get<0>( b.m_data ) );
    case 1:
        return VecEqual()( std::get<1>( a.m_data ), std::get<0>( b.m_data ) );
    case 2:
        return VecEqual()( std::get<0>( a.m_data ), std::get<1>( b.m_data ) );
    case 3:
        return VecEqual()( std::get<1>( a.m_data ), std::get<1>( b.m_data ) );
    default:
        assert( false && "Never reach here!" );
    }
}
bool operator<( const MultiColFieldsDelegate &a, const MultiColFieldsDelegate &b )
{
    int x = a.m_data.index() | ( b.m_data.index() << 1 );
    switch ( x )
    {
    case 0:
        return VecLess()( std::get<0>( a.m_data ), std::get<0>( b.m_data ) );
    case 1:
        return VecLess()( std::get<1>( a.m_data ), std::get<0>( b.m_data ) );
    case 2:
        return VecLess()( std::get<0>( a.m_data ), std::get<1>( b.m_data ) );
    case 3:
        return VecLess()( std::get<1>( a.m_data ), std::get<1>( b.m_data ) );
    default:
        assert( false && "Never reach here!" );
    }
}
bool operator<( const MultiColFieldsDelegate &a, const Record &b )
{
    if ( a.m_data.index() == 0 )
        return VecLess()( std::get<0>( a.m_data ), b );
    return VecLess()( std::get<1>( a.m_data ), b );
}
bool operator<( const Record &a, const MultiColFieldsDelegate &b )
{
    if ( b.m_data.index() == 0 )
        return VecLess()( a, std::get<0>( b.m_data ) );
    return VecLess()( a, std::get<1>( b.m_data ) );
}

///////////////////////////////////////////////////////////////////////////
/// HashMultiIndex
///////////////////////////////////////////////////////////////////////////


// Positions are saved in Index.
template<class FieldDelegateT>
struct HashIndexBase // MultiColHashIndex
{
    using IndexSet = std::unordered_set<FieldDelegateT, HashCode>;
    using iterator = typename IndexSet::const_iterator;
    using RecordType = typename FieldDelegateT::value_type;

protected:
    IndexSet m_indice;

public:
    std::optional<Rowindex> at( const RecordType &key ) const
    {
        if ( auto it = m_indice.find( FieldDelegateT{key} ); it != m_indice.end() )
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
        return m_indice.size();
    }
};

struct MultiColHashIndex : public HashIndexBase<MultiColFieldsDelegate>
{
    // icol will not be verified.
    // return false if there are duplicate values
    bool create( const IDataFrame &df, const std::vector<size_t> &icols, std::ostream *err = nullptr )
    {
        m_indice.clear();
        for ( auto i = 0u; i < df.size(); ++i )
        {
            MultiColFieldsDelegate val{MultiColPos{&df, i, icols}};
            if ( m_indice.count( val ) )
            {
                if ( err )
                    *err << "Failed to create MultiColHashIndex for cols:" << to_string( df.colNames( icols ) )
                         << ". Found dupliate record:" << to_string( val.get() ) << ".\n";
                return false;
            }
            m_indice.insert( val );
        }
        return true;
    }
    bool create( const IDataFrame &df, const std::vector<std::string> &colNames, std::ostream *err = nullptr )
    {
        return create( df, df.colIndice( colNames ), err );
    }
};

// Positions are saved in Index.
struct HashIndex : public HashIndexBase<FieldDelegate>
{
    // icol will not be verified.
    // return false if there are duplicate values
    bool create( const IDataFrame &df, size_t icol, std::ostream *err = nullptr )
    {
        m_indice.clear();
        for ( auto i = 0u; i < df.size(); ++i )
        {
            FieldDelegate val{FieldPos{&df, i, icol}};
            if ( m_indice.count( val ) )
            {
                if ( err )
                    *err << "Failed to create HashIndex at col:" << df.colName( icol ) << ". Found dupliate field:" << to_string( val.get() )
                         << ".\n";
                return false;
            }
            m_indice.insert( val );
        }
        return true;
    }
    bool create( const IDataFrame &df, const std::string &colName, std::ostream *err = nullptr )
    {
        return create( df, *df.colIndex( colName ), err );
    }
};

///////////////////////////////////////////////////////////////////////////
/// HashIndex
///////////////////////////////////////////////////////////////////////////

// Positions are saved in Index.
/// Single-column as key, vector of row index as mapped values.
template<class FieldDelegateT>
struct HashMultiIndexBase
{
    using IndexMap = std::unordered_map<FieldDelegateT, std::vector<size_t>, HashCode>;
    using iterator = typename IndexMap::const_iterator;
    using RecordType = typename FieldDelegateT::value_type;

protected:
    IndexMap m_indice;

public:
    const std::vector<size_t> *at( const RecordType &key ) const
    {
        if ( auto it = m_indice.find( FieldDelegateT{key} ); it != m_indice.end() )
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
        return m_indice.size();
    }
};

struct HashMultiIndex : public HashMultiIndexBase<FieldDelegate>
{
    // icol will not be verified.
    bool create( const IDataFrame &df, size_t icol )
    {
        m_indice.clear();
        for ( auto i = 0u; i < df.size(); ++i )
        {
            FieldDelegate val{FieldPos{&df, i, icol}};
            m_indice[val].push_back( i );
        }
        return true;
    }
    bool create( const IDataFrame &df, const std::string &colName )
    {
        return create( df, colName );
    }
};

// Positions are saved in Index.
/// key:[rowIndice]
struct MultiColHashMultiIndex : public HashMultiIndexBase<MultiColFieldsDelegate>
{
    // icol will not be verified.
    bool create( const IDataFrame &df, const std::vector<size_t> &icols )
    {
        m_indice.clear();
        for ( auto i = 0u; i < df.size(); ++i )
        {
            MultiColFieldsDelegate val{MultiColPos{&df, i, icols}};
            m_indice[val].push_back( i );
        }
        return true;
    }
    bool create( const IDataFrame &df, const std::vector<std::string> &colNames )
    {
        return create( df, df.colIndice( colNames ) );
    }
};

///////////////////////////////////////////////////////////////////////////
/// OrderedIndex
///////////////////////////////////////////////////////////////////////////

// Positions are saved in Index.
template<class FieldDelegateT>
struct OrderedIndexBase
{
    using IndexVec = std::vector<FieldDelegateT>;
    using iterator = typename IndexVec::const_iterator;
    using RecordType = typename FieldDelegateT::value_type;

protected:
    IndexVec m_indice;
    bool m_bReverseOrder = false;

public:
    // get the row index of the element that is in n-th position of Index.
    std::optional<Rowindex> at( size_t nth ) const
    {
        if ( nth < size() )
            return m_indice[nth].index();
        return {};
    }

    /// Find the first element >= val.
    /// \return the index in sorted Index; empty if all elements < val.
    std::optional<size_t> findFirstGE( const Record &val, size_t pos = 0, size_t end = 0 ) const
    {
        if ( pos >= size() || ( end != 0 && pos > end ) )
            throw std::out_of_range( "findFirstGE" );
        // lower_bound.
        iterator itEnd = end == 0 ? m_indice.end() : std::next( m_indice.begin(), end );
        if ( iterator it = lower_bound( val, std::next( m_indice.begin(), pos ), itEnd ); it != itEnd )
            return std::distance( m_indice.begin(), it );
        return {};
    }
    /// Find the first element > val.
    /// \return the index in sorted Index; empty if all elements <= val.
    std::optional<size_t> findFirstGT( const Record &val, size_t pos = 0, size_t end = 0 ) const
    {
        if ( pos >= size() || ( end != 0 && pos > end ) )
            throw std::out_of_range( "findFirstGT" );
        // upper_bound.
        iterator itEnd = end == 0 ? m_indice.end() : std::next( m_indice.begin(), end );
        if ( iterator it = upper_bound( val, std::next( m_indice.begin(), pos ), itEnd ); it != itEnd )
            return std::distance( m_indice.begin(), it );
        return {};
    }

    /// Find the first element == val.
    /// \return index_in_sorted_Index
    std::optional<size_t> findFirst( const RecordType &val, size_t pos = 0, size_t end = 0 ) const
    {
        if ( pos >= size() || ( end != 0 && pos > end ) )
            throw std::out_of_range( "findFirst" );
        iterator itEnd = end == 0 ? m_indice.end() : std::next( m_indice.begin(), end );
        if ( iterator it = lower_bound( val, std::next( m_indice.begin(), pos ), itEnd ); it != itEnd && it->get() == val )
            return std::distance( m_indice.begin(), it );
        return {};
    }
    /// Find the last element == val.
    /// \return index_in_sorted_Index
    std::optional<size_t> findLast( const RecordType &val, size_t pos = 0, size_t end = 0 ) const
    {
        if ( pos >= size() || ( end != 0 && pos > end ) )
            throw std::out_of_range( "findLast" );
        iterator itBegin = std::next( m_indice.begin(), pos ), itEnd = end == 0 ? m_indice.end() : std::next( m_indice.begin(), end );
        iterator it = upper_bound( val, itBegin, itEnd );
        if ( it != itBegin )
        {
            it = std::prev( it, 1 );
            if ( it->get() == val )
                return std::distance( m_indice.begin(), it );
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
        return m_indice.size();
    }

protected:
    void sortRows()
    {
        if ( m_bReverseOrder )
            std::sort( m_indice.begin(), m_indice.end(), GreaterThan() );
        else
            std::sort( m_indice.begin(), m_indice.end() );
    }
    iterator lower_bound( const RecordType &val, iterator itBegin, iterator itEnd ) const
    {
        if ( m_bReverseOrder )
            return std::lower_bound( itBegin, itEnd, val, GreaterThan() );
        else
            return std::lower_bound( itBegin, itEnd, val );
    }
    iterator upper_bound( const RecordType &val, iterator itBegin, iterator itEnd ) const
    {
        if ( m_bReverseOrder )
            return std::upper_bound( itBegin, itEnd, val, GreaterThan() );
        else
            return std::upper_bound( itBegin, itEnd, val );
    }
};

struct MultiColOrderedIndex : public OrderedIndexBase<MultiColFieldsDelegate>
{
    // icol will not be verified.
    // return false if there are duplicate values
    bool create( const IDataFrame &df, const std::vector<size_t> &icols, bool bReverseOrder = false )
    {
        m_indice.clear();
        for ( auto i = 0u; i < df.size(); ++i )
            m_indice.push_back( MultiColFieldsDelegate{MultiColPos{&df, i, icols}} );
        m_bReverseOrder = bReverseOrder;
        sortRows();
        return true;
    }
    bool create( const IDataFrame &df, const std::vector<std::string> &colNames, bool bReverseOrder = false )
    {
        return create( df, df.colIndice( colNames ), bReverseOrder );
    }
};

struct OrderedIndex : public OrderedIndexBase<FieldDelegate>
{
    // icol will not be verified.
    // return false if there are duplicate values
    bool create( const IDataFrame &df, size_t icol, bool bReverse = false )
    {
        m_indice.clear();
        for ( auto i = 0u; i < df.size(); ++i )
            m_indice.push_back( FieldDelegate{FieldPos{&df, i, icol}} );
        m_bReverseOrder = bReverse;
        sortRows();
        return true;
    }
    bool create( const IDataFrame &df, const std::string &colName, bool bReverse = false )
    {
        return create( df, *df.colIndex( colName ), bReverse );
    }
};

class IndexManager
{
protected:
    using VarIndex = std::variant<OrderedIndex, HashIndex, HashMultiIndex, MultiColOrderedIndex, MultiColHashIndex, MultiColHashMultiIndex>;

    using IndexMap = std::unordered_map<std::string, VarIndex>;
    IndexMap m_indexMap;
    IDataFrame *m_pDataFrame = nullptr;

public:
    IndexManager( IDataFrame *pDataFrame ) : m_pDataFrame( pDataFrame )
    {
    }

    // return index handle
    std::optional<IndexMap::iterator> addIndex( const std::string &indexName,
                                                IndexType indexType,
                                                const std::vector<std::string> &colNames,
                                                std::ostream *err = nullptr );
};
} // namespace df
