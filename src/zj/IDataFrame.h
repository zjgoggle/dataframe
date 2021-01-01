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

#include <zj/VarField.h>

namespace zj
{

/**

DataFrame Storage Layout:

4-bytes TypeCode | 4-bytes length of Fields data | 4 bytes    | 127-bytes-length  | 4-bytes number of Fields | Field0 Type | Field 0 |....|
------------------------------------------------------------------------------------------------------------
DFBC             |  length (in bytes)            | NumCols    |  Column Names     |  Number of Fields        |  Type       |  Value  |....|


Ordered Index Layout:

1 Byte    | 4-byte cols | Column indices |4 bytes                       | 4 bytes         | .....
--------------------------------------------------------------------------------
IndexType | NumColumns  | Col0, Col1... | Index of First sorted fields | Index of second | .....


Hash Index Layout:

1 Byte        | 4-byte cols | Column indices
--------------------------------------------
HashIndex/'H' | NumColumns  | Col0, Col1...

**/


enum class IndexType : char
{
    OrderedIndex = 'O',
    ReverseOrderedIndex = 'R',
    HashIndex = 'H', // key: SingleValue
    HashMultiIndex = 'M', // key:MultiValues
};

class IDataFrame;

template<bool isSingleT>
struct RecordOrFieldRef
{
    static constexpr auto isSingle = isSingleT;
    using RecordType = std::conditional_t<isSingle, VarField, Record>;
    using ColsType = std::conditional_t<isSingle, size_t, std::vector<size_t>>;

    const IDataFrame *df = nullptr;
    Rowindex irow = 0;
    const ColsType *icols = nullptr; // vector<size_t>* or size_t*. Map to underlying df columns. If it's null, use underlying columns.

    size_t size() const;
    const VarField &at( size_t nthField ) const;
    const VarField &operator[]( size_t nthField ) const;
    const VarField &at( const std::string &colname ) const;
    const VarField &operator[]( const std::string &colname ) const;
};
using RowRef = RecordOrFieldRef<false>;

template<class PrimitiveT>
struct VectorRef
{
    const IDataFrame *df = nullptr;
    size_t icol = 0;
    std::vector<size_t> *irows = nullptr; // Map to underlying df columns. If it's null, use underlying columns.

    size_t size() const;
    /// Convert the underlying VarField to PrimitiveT at position nthRow
    /// \note throw std::bad_variant_access if the underlying type is not PrimitiveT.
    const PrimitiveT &at( size_t nthRow ) const;
    const PrimitiveT &operator[]( size_t nthRow ) const;
};

using ColumnRef = VectorRef<VarField>;

///////////////////////////////////////////////////////////////
/// IDataFrame
///////////////////////////////////////////////////////////////

class IDataFrame
{
public:
    /// \return <rows, cols>
    virtual size_t countRows() const = 0;
    virtual size_t countCols() const = 0;
    virtual const VarField &at( size_t irow, size_t icol ) const = 0;
    virtual const VarField &at( size_t irow, const std::string &col ) const = 0;

    virtual size_t colIndex( const std::string &colName ) const = 0;
    virtual const std::string &colName( size_t icol ) const = 0;
    virtual const ColumnDef &columnDef( size_t icol ) const = 0;
    virtual const ColumnDef &columnDef( const std::string &colName ) const = 0;

    virtual ~IDataFrame() = default;

    /// \return a concrete DataFrame.
    virtual IDataFrame *deepCopy() const = 0;

    virtual bool isView() const
    {
        return false;
    }

    /// \return rows/records
    virtual size_t size() const
    {
        return countRows();
    }
    std::array<size_t, 2> shape() const
    {
        return std::array<size_t, 2>{countRows(), countCols()};
    }
    virtual std::vector<std::string> colName( const std::vector<size_t> &icols ) const
    {
        std::vector<std::string> res;
        auto N = countCols();
        for ( auto c : icols )
        {
            if ( c >= N )
                throw std::out_of_range( "Column index is out of range: " + to_string( c ) + ">=" + to_string( N ) );
            res.push_back( colName( c ) );
        }
        return res;
    }
    virtual std::vector<size_t> colIndex( const std::vector<std::string> &colNames ) const
    {
        std::vector<size_t> res;
        for ( const auto &n : colNames )
            res.push_back( colIndex( n ) );
        return res;
    }
    RowRef getRowRef( size_t irow ) const
    {
        return RowRef{this, irow};
    }
    ColumnRef getColumnRef( size_t icol ) const
    {
        return ColumnRef{this, icol};
    }
    ColumnRef getColumnRef( const std::string &colname ) const
    {
        return ColumnRef{this, this->colIndex( colname )};
    }
    template<class PrimitiveT>
    VectorRef<PrimitiveT> getColumnRefAsType( std::in_place_type_t<PrimitiveT>, const std::string &colname ) const
    {
        return {this, this->colIndex( colname )};
    }

    std::ostream &print( std::ostream &os, bool bHeader = true, char sepField = '|', char sepRow = '\n' ) const
    {
        size_t NC = countCols(), NR = countRows();
        if ( bHeader )
        {
            for ( size_t i = 0; i < NC; ++i )
            {
                if ( i )
                    os << sepField;
                os << columnDef( i ).colName;
            }
            os << sepRow;
        }
        for ( size_t i = 0; i < NR; ++i )
        {
            for ( size_t j = 0; j < NC; ++j )
            {
                if ( j )
                    os << sepField;
                os << to_string( at( i, j ) );
            }
            os << sepRow;
        }
        return os;
    }
};

using IDataFramePtr = std::shared_ptr<IDataFrame>;

inline std::ostream &operator<<( std::ostream &os, const IDataFrame &df )
{
    return df.print( os );
}

///////////////////////////////////////////////////////////////
/// ColumnRef used by Indexing.
///////////////////////////////////////////////////////////////

template<class PrimitiveT>
size_t VectorRef<PrimitiveT>::size() const
{
    return irows ? irows->size() : df->countRows();
}
template<class PrimitiveT>
const PrimitiveT &VectorRef<PrimitiveT>::at( size_t nthRow ) const
{
    if ( irows )
    {
        if ( nthRow >= irows->size() )
            throw std::out_of_range( "ColumnRef nthRow:" + std::to_string( nthRow ) + " >= " + std::to_string( irows->size() ) );
        if constexpr ( std::is_same_v<PrimitiveT, VarField> )
            return df->at( irows->at( nthRow ), icol );
        else
        {
            const VarField &var = df->at( irows->at( nthRow ), icol );
            return std::get<FieldValue<PrimitiveT>>( var ).value; // std::bad_variant_access
        }
    }
    else
    {
        if constexpr ( std::is_same_v<PrimitiveT, VarField> )
            return df->at( nthRow, icol );
        else
        {
            const VarField &var = df->at( nthRow, icol );
            return std::get<FieldValue<PrimitiveT>>( var ).value; // std::bad_variant_access
        }
    }
}
template<class PrimitiveT>
const PrimitiveT &VectorRef<PrimitiveT>::operator[]( size_t nthRow ) const
{
    return at( nthRow );
}

template<class PrimitiveT>
std::string to_string( const VectorRef<PrimitiveT> &rec )
{
    return to_string_vec( rec );
}

template<class PrimitiveT>
std::ostream &operator<<( std::ostream &os, const VectorRef<PrimitiveT> &rec )
{
    return os << to_string( rec );
}

template<class PrimitiveT>
struct hash_code<VectorRef<PrimitiveT>>
{
    size_t operator()( const VectorRef<PrimitiveT> &v ) const
    {
        return VecHash()( v );
    }
};

template<class PrimitiveT>
bool operator==( const VectorRef<PrimitiveT> &a, const VectorRef<PrimitiveT> &b )
{
    if ( a.df == b.df && a.irows == b.irows && a.icol == b.icol )
        return true;
    return VecEqual()( a, b );
}
template<class PrimitiveT>
bool operator==( const Record &a, const VectorRef<PrimitiveT> &b )
{
    return VecEqual()( a, b );
}
template<class PrimitiveT>
bool operator==( const VectorRef<PrimitiveT> &a, const Record &b )
{
    return b == a;
}
template<class PrimitiveT>
bool operator<( const VectorRef<PrimitiveT> &a, const VectorRef<PrimitiveT> &val )
{
    return VecLess()( a, val );
}
template<class PrimitiveT>
bool operator<( const VectorRef<PrimitiveT> &a, const Record &val )
{
    return VecLess()( a, val );
}
template<class PrimitiveT>
bool operator<( const Record &val, const VectorRef<PrimitiveT> &a )
{
    return VecLess()( val, a );
}

///////////////////////////////////////////////////////////////
/// RecordOrFieldRef used by Indexing.
///////////////////////////////////////////////////////////////

template<bool isSingleT>
size_t RecordOrFieldRef<isSingleT>::size() const
{
    if constexpr ( isSingle )
        return 1;
    else
        return icols ? icols->size() : df->countCols();
}
template<bool isSingleT>
const VarField &RecordOrFieldRef<isSingleT>::at( size_t nthField ) const
{
    if constexpr ( isSingle )
    {
        assert( nthField == 0 );
        return df->at( irow, *icols );
    }
    else if ( icols )
    {
        if ( nthField >= icols->size() )
            throw std::out_of_range( "RecordRef nthField:" + std::to_string( nthField ) + " >= " + std::to_string( icols->size() ) );
        return df->at( irow, icols->at( nthField ) );
    }
    else
        return df->at( irow, nthField );
}
template<bool isSingleT>
const VarField &RecordOrFieldRef<isSingleT>::operator[]( size_t nthField ) const
{
    return at( nthField );
}
template<bool isSingleT>
const VarField &RecordOrFieldRef<isSingleT>::at( const std::string &colname ) const
{
    return df->at( irow, colname );
}
template<bool isSingleT>
const VarField &RecordOrFieldRef<isSingleT>::operator[]( const std::string &colname ) const
{
    return df->at( irow, colname );
}

template<bool isSingleT>
std::string to_string( const RecordOrFieldRef<isSingleT> &rec )
{
    return to_string_vec( rec );
}
template<bool isSingleT>
std::ostream &operator<<( std::ostream &os, const RecordOrFieldRef<isSingleT> &rec )
{
    return os << to_string( rec );
}

template<bool isSingle>
struct hash_code<RecordOrFieldRef<isSingle>>
{
    size_t operator()( const RecordOrFieldRef<isSingle> &v ) const
    {
        return VecHash()( v );
    }
};

template<bool isSingle>
bool operator==( const RecordOrFieldRef<isSingle> &a, const RecordOrFieldRef<isSingle> &b )
{
    if ( a.df == b.df && a.icols == b.icols && a.irow == b.irow )
        return true;
    if constexpr ( isSingle )
        return a.at( 0 ) == b.at( 0 );
    else
        return VecEqual()( a, b );
}
template<bool isSingle>
bool operator==( const typename RecordOrFieldRef<isSingle>::RecordType &a, const RecordOrFieldRef<isSingle> &b )
{
    if constexpr ( isSingle )
        return a == b.at( 0 );
    else
        return VecEqual()( a, b );
}
template<bool isSingle>
bool operator==( const RecordOrFieldRef<isSingle> &a, const typename RecordOrFieldRef<isSingle>::RecordType &b )
{
    return b == a;
}

template<bool isSingle>
bool operator<( const RecordOrFieldRef<isSingle> &a, const RecordOrFieldRef<isSingle> &val )
{
    if constexpr ( isSingle )
        return a.at( 0 ) < val.at( 0 );
    else
        return VecLess()( a, val );
}
template<bool isSingle>
bool operator<( const RecordOrFieldRef<isSingle> &a, const typename RecordOrFieldRef<isSingle>::RecordType &val )
{
    if constexpr ( isSingle )
        return a.at( 0 ) < val;
    else
        return VecLess()( a, val );
}
template<bool isSingle>
bool operator<( const typename RecordOrFieldRef<isSingle>::RecordType &val, const RecordOrFieldRef<isSingle> &a )
{
    if constexpr ( isSingle )
        return val < a.at( 0 );
    else
        return VecLess()( val, a );
}

///////////////////////////////////////////////////////////////
/// FieldHashDelegate MultiColFieldsHashDelegate used by Hash Indexing.
///////////////////////////////////////////////////////////////

struct FieldHashDelegate
{
    using position_type = RecordOrFieldRef<true>;
    using value_type = VarField;
    std::variant<position_type, VarField> m_data; // VarField is only used for hash lookup

    bool has_value() const
    {
        return m_data.index() == 1 || std::get<0>( m_data ).df;
    }

    // return row index
    size_t index() const
    {
        if ( m_data.index() == 0 )
        {
            const position_type &pos = std::get<0>( m_data );
            assert( pos.df && "FieldHashDelegate not initialized!" );
            return pos.irow;
        }
        else
        {
            throw std::invalid_argument( "FieldHashDelegate has VarField. Expected Pos!" );
        }
    }
    const VarField &get() const
    {
        if ( m_data.index() == 0 )
        {
            return std::get<0>( m_data ).at( 0 );
        }
        else
            return std::get<1>( m_data );
    }
    const VarField &operator*() const
    {
        return get();
    }
};
inline std::string to_string( const FieldHashDelegate &val )
{
    return to_string( val.m_data );
}
template<>
struct hash_code<FieldHashDelegate>
{
    size_t operator()( const FieldHashDelegate &a ) const
    {
        auto r = hashcode( a.m_data );
        return r;
    }
};

inline bool operator==( const FieldHashDelegate &a, const FieldHashDelegate &b )
{
    return a.get() == b.get();
}


// multi-column Fields
struct MultiColFieldsHashDelegate
{
    using position_type = RecordOrFieldRef<false>;
    using value_type = Record;

    std::variant<position_type, Record> m_data; // Record is only used for hash lookup

    bool has_value() const
    {
        return m_data.index() == 1 || std::get<0>( m_data ).df;
    }

    // return row index
    size_t index() const
    {
        if ( m_data.index() == 0 )
        {
            const position_type &pos = std::get<0>( m_data );
            assert( pos.df && "FieldHashDelegate not initialized!" );
            return pos.irow;
        }
        else
        {
            throw std::invalid_argument( "FieldHashDelegate has VarField. Expected Pos!" );
        }
    }
};
inline std::string to_string( const MultiColFieldsHashDelegate &val )
{
    return to_string( val.m_data );
}

template<>
struct hash_code<MultiColFieldsHashDelegate>
{
    size_t operator()( const MultiColFieldsHashDelegate &a ) const
    {
        auto r = hashcode( a.m_data );
        return r;
    }
};
inline bool operator==( const MultiColFieldsHashDelegate &a, const MultiColFieldsHashDelegate &b )
{
    size_t anyConcrete = a.m_data.index() | ( b.m_data.index() << 1 );
    switch ( anyConcrete )
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

} // namespace zj
