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

#include <zj/IDataFrame.h>

namespace zj
{

struct ICondition
{
    virtual bool evalAtRow( Rowindex irow ) const = 0;
    virtual ~ICondition() = default;
};

enum class OperatorTag
{
    EQ, // ==
    NE, // !=
    LT, // <
    LE, // <=
    GT, // >
    GE, // >=

    ISIN,
    NOTIN,
    AND, // &&
    OR, // ||
};

inline const char *to_cstr( OperatorTag v )
{
    switch ( v )
    {
    case OperatorTag::EQ:
        return "==";
    case OperatorTag::NE:
        return "!=";
    case OperatorTag::LT:
        return "<";
    case OperatorTag::LE:
        return "<=";
    case OperatorTag::GT:
        return ">";
    case OperatorTag::GE:
        return ">=";
    case OperatorTag::ISIN:
        return "isin";
    case OperatorTag::NOTIN:
        return "notin";
    case OperatorTag::AND:
        return "&&";
    case OperatorTag::OR:
        return "||";
    default:
        throw std::range_error( "to_cstr Invalid OperatorTag:" + std::to_string( int( v ) ) );
        return "NotOperator";
    }
}

inline OperatorTag logicOpposite( OperatorTag v )
{
    switch ( v )
    {
    case OperatorTag::EQ:
        return OperatorTag::NE;
    case OperatorTag::NE:
        return OperatorTag::EQ;
    case OperatorTag::LT:
        return OperatorTag::GE;
    case OperatorTag::LE:
        return OperatorTag::GT;
    case OperatorTag::GT:
        return OperatorTag::LE;
    case OperatorTag::GE:
        return OperatorTag::LT;
    case OperatorTag::ISIN:
        return OperatorTag::NOTIN;
    case OperatorTag::NOTIN:
        return OperatorTag::ISIN;
    case OperatorTag::AND:
        return OperatorTag::OR;
    case OperatorTag::OR:
        return OperatorTag::AND;
    default:
        throw std::range_error( "logicOpposite Invalid OperatorTag:" + std::to_string( int( v ) ) );
    }
}
// only < and == are invoked.
template<class T, class U>
bool invoke_compare( OperatorTag comp, T &&a, U &&b )
{
    switch ( comp )
    {
    case OperatorTag::EQ:
        return a == b;
    case OperatorTag::NE:
        return !( a == b );
    case OperatorTag::LT:
        return a < b;
    case OperatorTag::LE:
        return !( b < a );
    case OperatorTag::GT:
        return b < a;
    case OperatorTag::GE:
        return !( a < b );
    default:
        throw std::runtime_error( "Invalid CompareTag:" + std::to_string( int( comp ) ) );
    }
}
inline bool checkFieldCompatible( const IDataFrame *df, size_t icol, const VarField &field, std::ostream *err )
{
    const ColumnDef &colDef = df->columnDef( icol );
    if ( !is_field_compatible( field, colDef ) )
    {
        if ( err )
            *err << "Field value:" << to_string( field ) << " is not compatible with (col:" << colDef.colName
                 << ", type:" << typeName( colDef.colTypeTag ) << ".\n";
        return false;
    }
    return true;
}
inline bool checkFieldCompatible( const IDataFrame *df, const std::vector<std::size_t> &icols, const Record &fields, std::ostream *err )
{
    assert( icols.size() == fields.size() );

    for ( size_t i = 0, N = fields.size(); i < N; ++i )
    {
        const ColumnDef &colDef = df->columnDef( icols[i] );
        if ( !is_field_compatible( fields[i], colDef ) )
        {
            if ( err )
                *err << "Field value:" << to_string( fields[i] ) << " is not compatible with (col:" << colDef.colName
                     << ", type:" << typeName( colDef.colTypeTag ) << " in record " << to_string( fields ) << ".\n";
            return false;
        }
    }
    return true;
}

template<bool bSingleCol>
struct ConditionCompare : public ICondition
{
    using ColIndex = std::conditional_t<bSingleCol, size_t, std::vector<std::size_t>>;
    using RecordType = std::conditional_t<bSingleCol, VarField, Record>;
    using RecordRef = RecordOrFieldRef<bSingleCol>;
    using ColNames = std::conditional_t<bSingleCol, std::string, std::vector<std::string>>;

    const IDataFrame *m_df;
    ColIndex m_col; // column indices
    OperatorTag m_compareTag;
    RecordType m_val;

    bool init( const IDataFrame *df, ColNames colnames, OperatorTag compareTag, RecordType val, std::ostream *err = nullptr )
    {
        m_df = df;
        m_compareTag = compareTag;

        if constexpr ( !bSingleCol )
        {
            if ( colnames.size() != val.size() )
            {
                if ( err )
                    *err << "ColumnCount:" << colnames.size() << " != "
                         << " FieldCount:" << val.size() << " .\n";
                return false;
            }
        }
        m_col = std::move( m_df->colIndex( colnames ) );
        if ( !checkFieldCompatible( df, m_col, val, err ) )
            return false;
        m_val = std::move( val );
        return true;
    }
    void setLogicNot()
    {
        m_compareTag = logicOpposite( m_compareTag );
    }
    bool evalAtRow( Rowindex irow ) const override
    {
        return invoke_compare( m_compareTag, RecordRef{m_df, irow, &m_col}, m_val );
    }
};

template<bool bSingleCol>
struct ConditionIsIn : public ICondition
{
    using ColIndex = std::conditional_t<bSingleCol, size_t, std::vector<std::size_t>>;
    using RecordType = std::conditional_t<bSingleCol, VarField, Record>;
    using RecordRef = RecordOrFieldRef<bSingleCol>;
    using ColNames = std::conditional_t<bSingleCol, std::string, std::vector<std::string>>;

    using ValueType = std::conditional_t<bSingleCol, FieldHashDelegate, MultiColFieldsHashDelegate>;

    const IDataFrame *m_df;
    ColIndex m_col; // column indices
    std::unordered_set<ValueType, HashCode> m_val; // todo use hash delegate
    bool m_isinOrNot; // or not in

    bool init( const IDataFrame *df, ColNames colnames, std::vector<RecordType> records, bool isInOrNot = true, std::ostream *err = nullptr )
    {
        m_df = df;
        m_isinOrNot = isInOrNot;

        if constexpr ( !bSingleCol )
        {
            if ( colnames.size() != records.at( 0 ).size() )
            {
                if ( err )
                    *err << "ColumnCount:" << colnames.size() << " != "
                         << " FieldCount:" << records.at( 0 ).size() << " .\n";
                return false;
            }
        }
        m_col = std::move( m_df->colIndex( colnames ) );

        for ( const auto &e : records )
            if ( !checkFieldCompatible( df, m_col, e, err ) )
                return false;
        for ( auto &&e : records )
            m_val.emplace( ValueType{std::move( e )} );
        return true;
    }
    void setLogicNot()
    {
        m_isinOrNot = !m_isinOrNot;
    }
    bool evalAtRow( Rowindex irow ) const override
    {
        if ( m_isinOrNot )
            return m_val.count( ValueType{typename ValueType::position_type{m_df, irow, &m_col}} );
        else
            return !m_val.count( ValueType{typename ValueType::position_type{m_df, irow, &m_col}} );
    }
};

// expression
// Expr1 && Expr2
// Expr1 || Expr2
// Expr1 || Expr3 && Expr4 || AndExpr
// !Expr
// !AddExpr
// \note !OrExpr is not allowed.
class Expr;
struct ColNames
{
    std::vector<std::string> cols;

    // cols are inited already
    Expr isin( std::vector<Record> vals );
    Expr isin( Record vals );

    Expr notin( std::vector<Record> vals );
    Expr notin( Record vals );
};

struct Expr
{
    std::vector<std::string> cols; // column names
    OperatorTag compareOrIn; // compare, isin, notin
    std::variant<Record, std::vector<Record>> val;

    bool has_value() const
    {
        return !cols.empty();
    }
};

inline Expr operator!( Expr &&e )
{
    Expr r = std::move( e );
    r.compareOrIn = logicOpposite( r.compareOrIn );
    return r;
}
struct AndExpr
{
    std::vector<Expr> ops;
};
struct OrExpr
{
    std::vector<AndExpr> ops;
};

// ! OrEpr is not allowed.
inline OrExpr operator!( AndExpr &&e )
{
    OrExpr r;
    for ( auto &&expr : e.ops )
    {
        r.ops.emplace_back( AndExpr{std::vector<Expr>{!std::move( expr )}} );
    }
    return r;
}

inline std::string to_string( const Expr &v )
{
    std::string s = to_string( v.cols, std::string( ", " ), "[]" );
    return s + " " + to_cstr( v.compareOrIn ) + ( " " + to_string( v.val ) );
}
inline std::string to_string( const AndExpr &v, const char *quotes = "()" )
{
    return to_string( v.ops, " && ", quotes );
}
inline std::string to_string( const OrExpr &v, const char *quotes = "()" )
{
    return to_string( v.ops, " || ", quotes );
}

inline std::ostream &operator<<( std::ostream &os, const Expr &v )
{
    assert( v.has_value() );
    return os << to_string( v );
}
inline std::ostream &operator<<( std::ostream &os, const AndExpr &v )
{
    assert( v.ops.size() );
    return os << to_string( v );
}
inline std::ostream &operator<<( std::ostream &os, const OrExpr &v )
{
    assert( v.ops.size() );
    return os << to_string( v );
}

template<class... T>
ColNames Col( T &&... args )
{
    static_assert( ( std::is_constructible_v<std::string, T> && ... ) );

    SCols cols{std::forward<T>( args )...};
    return {std::move( cols )};
}

// Col < val
template<class T>
Expr operator<( ColNames &&cols, T &&val )
{
    assert( cols.cols.size() == 1 );
    return Expr{std::move( cols.cols ), OperatorTag::LT, Record{field( std::move( val ) )}};
}

// Cols < vals
template<class... Args>
Expr operator<( ColNames &&cols, std::tuple<Args...> &&val )
{
    static_assert( CompatibleFieldTypes<Args...>() );
    assert( cols.cols.size() == sizeof...( Args ) );
    return Expr{std::move( cols.cols ), OperatorTag::LT, recordtup( val )};
}
// Col == val
template<class T>
Expr operator==( ColNames &&cols, T &&val )
{
    assert( cols.cols.size() == 1 );
    return Expr{std::move( cols.cols ), OperatorTag::EQ, Record{field( val )}};
}

// Cols == vals
template<class... Args>
Expr operator==( ColNames &&cols, std::tuple<Args...> &&val )
{
    static_assert( CompatibleFieldTypes<Args...>() );
    assert( cols.cols.size() == sizeof...( Args ) );
    return Expr{std::move( cols.cols ), OperatorTag::EQ, record( val )};
}

//---------------- is in ----------------------

Expr ColNames::isin( std::vector<Record> vals )
{
    assert( cols.size() && cols.size() == vals.at( 0 ).size() && "Multi-value element" );
    if ( cols.size() != vals.at( 0 ).size() )
        throw std::range_error( "isin Error! columns size: " + std::to_string( cols.size() ) + " != " + std::to_string( vals.at( 0 ).size() ) );
    return {cols, OperatorTag::ISIN, std::move( vals )};
}

Expr ColNames::isin( Record vals )
{
    assert( cols.size() == 1 && "Multi" );
    if ( cols.size() != 1 )
        throw std::range_error( "isin expecting single col: " + std::to_string( cols.size() ) );
    std::vector<Record> v;
    for ( auto &&e : vals )
        v.push_back( Record{std::move( e )} );
    return {cols, OperatorTag::ISIN, std::move( v )};
}
Expr ColNames::notin( std::vector<Record> vals )
{
    auto r = isin( std::move( vals ) );
    r.compareOrIn = OperatorTag::NOTIN;
    return r;
}
Expr ColNames::notin( Record vals )
{
    auto r = isin( std::move( vals ) );
    r.compareOrIn = OperatorTag::NOTIN;
    return r;
}

//---------------- logic and ----------------------
inline AndExpr operator&&( Expr &&a, Expr &&b )
{
    return AndExpr{{std::move( a ), std::move( b )}};
}
inline AndExpr operator&&( AndExpr &&a, Expr &&b )
{
    a.ops.emplace_back( std::move( b ) );
    return a;
}
//---------------- logic and ----------------------
inline OrExpr operator||( Expr &&a, Expr &&b )
{
    AndExpr x{{a}}, y{{b}};
    return OrExpr{{x, y}};
}
inline OrExpr operator||( AndExpr &&a, Expr &&b )
{
    AndExpr t{{b}};
    return OrExpr{{a, t}};
}
inline OrExpr operator||( AndExpr &&a, AndExpr &&b )
{
    return OrExpr{{a, b}};
}
inline OrExpr operator||( Expr &&a, AndExpr &&b )
{
    return AndExpr{{a}} || std::move( b );
}

inline OrExpr operator||( OrExpr &&a, OrExpr &&b )
{
    for ( auto &&e : b.ops )
        a.ops.emplace_back( std::move( e ) );
    return a;
}
inline OrExpr operator||( OrExpr &&a, AndExpr &&b )
{
    a.ops.emplace_back( std::move( b ) );
    return a;
}
inline OrExpr operator||( AndExpr &&a, OrExpr &&b )
{
    return std::move( b ) || std::move( a );
}
inline OrExpr operator||( OrExpr &&a, Expr &&b )
{
    a.ops.emplace_back( AndExpr{{b}} );
    return a;
}

} // namespace zj
