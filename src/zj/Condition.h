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

enum class CompareTag
{
    EQ, // ==
    NE, // !=
    LT, // <
    LE, // <=
    GT, // >
    GE, // >=
};

// only < and == are invoked.
template<class T, class U>
bool invoke_compare( CompareTag comp, T &&a, U &&b )
{
    switch ( comp )
    {
    case CompareTag::EQ:
        return a == b;
    case CompareTag::NE:
        return !( a == b );
    case CompareTag::LT:
        return a < b;
    case CompareTag::LE:
        return !( b < a );
    case CompareTag::GT:
        return b < a;
    case CompareTag::GE:
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
    CompareTag m_compareTag;
    RecordType m_val;

    bool init( const IDataFrame *df, ColNames colnames, CompareTag compareTag, RecordType val, std::ostream *err = nullptr )
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

    bool init( const IDataFrame *df, ColNames colnames, std::vector<RecordType> records, std::ostream *err = nullptr )
    {
        m_df = df;

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
    bool evalAtRow( Rowindex irow ) const override
    {
        return m_val.count( ValueType{typename ValueType::position_type{m_df, irow, &m_col}} );
    }
};

} // namespace zj
