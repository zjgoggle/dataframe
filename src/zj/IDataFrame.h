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

#include <string>
#include <vector>
#include <variant>
#include <chrono>
#include <iostream>
#include <charconv>
#include <cassert>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <tuple>
#include <functional>
#include <memory>

#include <zj/DateTime.h>

namespace zj
{

// using Null = std::nullptr_t;
using Str = std::string;
using StrVec = std::vector<std::string>;
using IntVec = std::vector<int32_t>;
using LongVec = std::vector<int64_t>;
using ULongVec = std::vector<size_t>;
using Timestamp = DateTime;


using SCols = StrVec; // String Columns
using ICols = ULongVec; // Int Columns
using IRows = ULongVec; // Int Rows;

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


///////////////////////////////////////////////////////////////
/// Field and Record
///////////////////////////////////////////////////////////////

enum class FieldTypeTag : u_char
{
    Null = 0, // default type
    Str, // |4-byte length| null terminated string
    Bool, // 1 byte: 1/0
    Char, // 1 byte
    Int32, // 4 bytes little endian
    Int64,
    Float32,
    Float64,
    Timestamp, // 64 bits int, nano seconds since epoch

    StrVec,
    BoolVec,
    CharVec,
    Int32Vec,
    Int64Vec,
    Float32Vec,
    Float64Vec,
    TimestampVec,

    End,
    VectorFlag = StrVec,
};

constexpr bool is_vec_field( FieldTypeTag type )
{
    return u_char( type ) >= u_char( FieldTypeTag::VectorFlag );
}

constexpr FieldTypeTag element_field_type( FieldTypeTag type )
{
    return is_vec_field( type ) ? FieldTypeTag( u_char( type ) - u_char( FieldTypeTag::VectorFlag ) ) : type;
}

struct Null
{
};
inline bool operator==( const Null &, const Null & )
{
    return true;
}
inline bool operator<( const Null &, const Null & )
{
    return false;
}
enum class NullPolicy
{
    Allow,
    Ignore,
    Error,
};

struct Global
{
    static constexpr const char *defaultNullStr = "N/A";
    std::string nullstr = defaultNullStr;
    bool bParseNull = true; // auto parse null field.
};

Global &global();

bool is_null( std::string_view s );

template<class T>
auto Set( const T &v )
{
    return std::unordered_set<T>{v};
}
template<class T>
auto Set( const std::vector<T> &v )
{
    return std::unordered_set<T>{v.begin(), v.end()};
}

static constexpr int32_t VAR_LENGTH = 0xFFFFFFFF;

struct ColumnDef
{
    FieldTypeTag colTypeTag;
    std::string colName;
};

using ColumnDefs = std::vector<ColumnDef>;

template<class UnderlyingType>
struct FieldValue
{
    static constexpr FieldTypeTag type = FieldTypeTag::End;
};

template<class UnderlyingType>
bool operator==( const FieldValue<UnderlyingType> &a, const FieldValue<UnderlyingType> &b )
{
    return a.value == b.value;
}

template<class UnderlyingType>
bool operator<( const FieldValue<UnderlyingType> &a, const FieldValue<UnderlyingType> &b )
{
    return a.value < b.value;
}

#define DEFINE_FIELDVALUE( FIELDTYPE, UNDERLYINGTYPE, LEN )                                                                                         \
    template<>                                                                                                                                      \
    struct FieldValue<UNDERLYINGTYPE>                                                                                                               \
    {                                                                                                                                               \
        using value_type = UNDERLYINGTYPE;                                                                                                          \
        static constexpr bool is_vec = is_vec_field( FieldTypeTag::FIELDTYPE );                                                                     \
        static constexpr FieldTypeTag type = FieldTypeTag::FIELDTYPE;                                                                               \
        static constexpr FieldTypeTag element_type = element_field_type( FieldTypeTag::FIELDTYPE );                                                 \
        static constexpr int32_t fieldLength = LEN;                                                                                                 \
        static constexpr const char *typeName = #FIELDTYPE;                                                                                         \
        value_type value;                                                                                                                           \
    };                                                                                                                                              \
    using FIELDTYPE##Field = FieldValue<UNDERLYINGTYPE>;                                                                                            \
    inline ColumnDef FIELDTYPE##Col( std::string name )                                                                                             \
    {                                                                                                                                               \
        return ColumnDef{FieldTypeTag::FIELDTYPE, std::move( name )};                                                                               \
    }


DEFINE_FIELDVALUE( Null, Null, 0 );
DEFINE_FIELDVALUE( Str, Str, VAR_LENGTH );
DEFINE_FIELDVALUE( Bool, bool, 1 );
DEFINE_FIELDVALUE( Char, char, 1 );
DEFINE_FIELDVALUE( Int32, int32_t, 4 );
DEFINE_FIELDVALUE( Int64, int64_t, 8 );
DEFINE_FIELDVALUE( Float32, float, 4 );
DEFINE_FIELDVALUE( Float64, double, 8 );
DEFINE_FIELDVALUE( Timestamp, Timestamp, 8 );

DEFINE_FIELDVALUE( StrVec, std::vector<Str>, VAR_LENGTH );
DEFINE_FIELDVALUE( BoolVec, std::vector<bool>, VAR_LENGTH );
DEFINE_FIELDVALUE( CharVec, std::vector<char>, VAR_LENGTH );
DEFINE_FIELDVALUE( Int32Vec, std::vector<int32_t>, VAR_LENGTH );
DEFINE_FIELDVALUE( Int64Vec, std::vector<int64_t>, VAR_LENGTH );
DEFINE_FIELDVALUE( Float32Vec, std::vector<float>, VAR_LENGTH );
DEFINE_FIELDVALUE( Float64Vec, std::vector<double>, VAR_LENGTH );
DEFINE_FIELDVALUE( TimestampVec, std::vector<Timestamp>, VAR_LENGTH );



/// \note the order of each field type must be consistent with the order of values in FieldTypeTag.
using VarField = std::variant<NullField,
                              StrField,
                              BoolField,
                              CharField,
                              Int32Field,
                              Int64Field,
                              Float32Field,
                              Float64Field,
                              TimestampField,

                              StrVecField,
                              BoolVecField,
                              CharVecField,
                              Int32VecField,
                              Int64VecField,
                              Float32VecField,
                              Float64VecField,
                              TimestampVecField>;


template<class PrimitiveType>
struct CompatibleFieldType
{
    static constexpr bool is_defined = FieldValue<PrimitiveType>::type != FieldTypeTag::End;
    static constexpr bool is_other_string = std::is_same_v<std::string_view, std::decay_t<PrimitiveType>> ||
                                            std::is_same_v<const char *, std::decay_t<PrimitiveType>> ||
                                            std::is_same_v<char *, std::decay_t<PrimitiveType>>;
    static constexpr bool value = is_defined || is_other_string;
    using type = std::conditional_t<is_other_string, std::string, std::decay_t<PrimitiveType>>;
};

template<class PrimitiveType>
using CompatibleFieldType_t = typename CompatibleFieldType<PrimitiveType>::type;
template<class PrimitiveType>
static constexpr bool CompatibleFieldType_v = CompatibleFieldType<PrimitiveType>::value;


template<class... Args>
constexpr bool CompatibleFieldTypes()
{
    return ( CompatibleFieldType_v<Args> && ... );
}

// using FieldRef = std::reference_wrapper<const VarField>;

using Record = std::vector<VarField>;

using Rowindex = size_t;

//-- bellow compare the NullField with other Field types.
inline bool operator==( const VarField &a, const VarField &b )
{
    int anynull = ( a.index() == 0 ? 1 : 0 ) | ( b.index() == 0 ? 2 : 0 );
    if ( anynull )
        return anynull == 3;
    assert( a.index() == b.index() ); // may throw
    return std::operator==( a, b );
}
inline bool operator!=( const VarField &a, const VarField &b )
{
    return !operator==( a, b );
}
inline bool operator<( const VarField &a, const VarField &b )
{
    int anynull = ( a.index() == 0 ? 1 : 0 ) | ( b.index() == 0 ? 2 : 0 );
    if ( anynull )
        return ( anynull == 1 ); // null is always less than non-null.
    assert( a.index() == b.index() );
    return std::operator<( a, b );
}
inline bool operator>( const VarField &a, const VarField &b )
{
    int anynull = ( a.index() == 0 ? 1 : 0 ) | ( b.index() == 0 ? 2 : 0 );
    if ( anynull )
        return ( anynull == 2 ); // null is always less than non-null.
    assert( a.index() == b.index() );
    return std::operator>( a, b );
}
inline bool operator<=( const VarField &a, const VarField &b )
{
    return !operator>( a, b );
}
inline bool operator>=( const VarField &a, const VarField &b )
{
    return !operator<( a, b );
}



///////////////////////////////////////////////////////////////
/// create_default_field
///////////////////////////////////////////////////////////////

// dynamic to static invoke
template<typename Func, class... Args>
auto static_invoke_for_type( FieldTypeTag typeTag, Func &&func, Args &&... args )
{
    switch ( typeTag )
    {
    case FieldTypeTag::Null:
        return func.template invoke<Null>( std::forward<Args>( args )... );
    case FieldTypeTag::Str:
        return func.template invoke<Str>( std::forward<Args>( args )... );
    case FieldTypeTag::Bool:
        return func.template invoke<bool>( std::forward<Args>( args )... );
    case FieldTypeTag::Char:
        return func.template invoke<char>( std::forward<Args>( args )... );
    case FieldTypeTag::Int32:
        return func.template invoke<int32_t>( std::forward<Args>( args )... );
    case FieldTypeTag::Int64:
        return func.template invoke<int64_t>( std::forward<Args>( args )... );
    case FieldTypeTag::Float32:
        return func.template invoke<float>( std::forward<Args>( args )... );
    case FieldTypeTag::Float64:
        return func.template invoke<double>( std::forward<Args>( args )... );
    case FieldTypeTag::Timestamp:
        return func.template invoke<Timestamp>( std::forward<Args>( args )... );
        //- vec
    case FieldTypeTag::StrVec:
        return func.template invoke<std::vector<Str>>( std::forward<Args>( args )... );
    case FieldTypeTag::BoolVec:
        return func.template invoke<std::vector<bool>>( std::forward<Args>( args )... );
    case FieldTypeTag::CharVec:
        return func.template invoke<std::vector<char>>( std::forward<Args>( args )... );
    case FieldTypeTag::Int32Vec:
        return func.template invoke<std::vector<int32_t>>( std::forward<Args>( args )... );
    case FieldTypeTag::Int64Vec:
        return func.template invoke<std::vector<int64_t>>( std::forward<Args>( args )... );
    case FieldTypeTag::Float32Vec:
        return func.template invoke<std::vector<float>>( std::forward<Args>( args )... );
    case FieldTypeTag::Float64Vec:
        return func.template invoke<std::vector<double>>( std::forward<Args>( args )... );
    case FieldTypeTag::TimestampVec:
        return func.template invoke<std::vector<Timestamp>>( std::forward<Args>( args )... );
    default:
        throw std::runtime_error( "Invalid typeTag:" + std::to_string( int( typeTag ) ) );
    }
}

struct GetFieldTypeName
{
    template<class T>
    constexpr const char *invoke() const
    {
        return FieldValue<T>::typeName;
    }
};

inline const char *typeName( FieldTypeTag typeTag )
{
    if ( typeTag >= FieldTypeTag::End )
        return "Invalid FieldTypeTag";
    return static_invoke_for_type( typeTag, GetFieldTypeName() );
}


template<class T, class... Args>
VarField fieldt( Args &&... args )
{
    return VarField{std::in_place_type_t<FieldValue<T>>(), std::forward<Args>( args )...};
}


template<class T>
VarField field( T v )
{
    return VarField{std::in_place_type_t<FieldValue<T>>(), FieldValue<T>{std::move( v )}};
}
template<>
inline VarField field( const char *v )
{
    return VarField{std::in_place_type_t<FieldValue<std::string>>(), FieldValue<std::string>{v}};
}
template<>
inline VarField field( std::string_view v )
{
    return VarField{std::in_place_type_t<FieldValue<std::string>>(), FieldValue<std::string>{std::string( v )}};
}
// wrap function field.
struct CreateField
{
    template<class T, class... Args>
    inline constexpr VarField invoke( Args &&... args ) const
    {
        return fieldt<T>( std::forward<Args>( args )... );
    }
};

inline VarField create_default_field( FieldTypeTag typeTag )
{
    return static_invoke_for_type( typeTag, CreateField() );
}



/// \brief Create record from tuple.
template<size_t nth = 0, class T, class... Args>
bool create_record( Record &rec, const std::tuple<T, Args...> &tup, std::ostream *err = nullptr, bool allowNullField = true )
{
    static_assert( CompatibleFieldTypes<T, Args...>() );
    VarField var = field( std::get<nth>( tup ) );
    if ( var.index() == 0 && !allowNullField )
    {
        if ( err )
            *err << "Null field is not allowed!\n";
        return false;
    }
    rec.push_back( std::move( var ) );
    if constexpr ( ( std::tuple_size_v<std::tuple<T, Args...>> ) > nth + 1 )
        return create_record<nth + 1>( rec, tup );
    else
        return true;
}
template<class... Args>
Record record( Args &&... args )
{
    return Record{field( std::forward<Args>( args ) )...};
}
template<class... Args>
Record recordtup( const std::tuple<Args...> &tup )
{
    static_assert( CompatibleFieldTypes<Args...>() );
    Record rec;
    create_record( rec, tup, nullptr );
    return rec;
}

template<class... Args>
std::tuple<Args...> mktuple( Args &&... args )
{
    static_assert( CompatibleFieldTypes<Args...>() );
    return std::make_tuple( std::forward<Args>( args )... );
}

template<size_t nth = 0, class T, class... Args>
void create_columns( ColumnDefs &rec, const std::vector<std::string> &names )
{
    assert( names.size() == 1 + nth + sizeof...( Args ) );
    rec.push_back( ColumnDef{FieldValue<T>::type, names[nth]} );
    if constexpr ( sizeof...( Args ) > 0 )
    {
        create_columns<nth + 1, Args...>( rec, names );
    }
}

inline bool is_field_compatible( const VarField &field, const ColumnDef &col, bool allowNullField = true )
{
    int fieldType = int( field.index() ), colType = int( col.colTypeTag );
    if ( fieldType == 0 )
        return allowNullField;
    return fieldType == colType;
}
inline bool is_record_compatible( const Record &rec, const ColumnDefs &cols, std::ostream *err = nullptr, bool allowNullField = true )
{
    if ( rec.size() != cols.size() )
    {
        if ( err )
            *err << "Record size is not the same as columns: " << rec.size() << "!=" << cols.size() << ".\n";
        return false;
    }
    for ( size_t i = 0, N = rec.size(); i < N; ++i )
    {
        if ( !is_field_compatible( rec[i], cols[i], allowNullField ) )
        {
            if ( err )
                *err << "Incompatible field type:" << typeName( FieldTypeTag( rec[i].index() ) )
                     << ", colType:" << typeName( FieldTypeTag( cols[i].colTypeTag ) ) << ".\n";
            return false;
        }
    }
    return true;
}


///////////////////////////////////////////////////////////////
/// to_str
///////////////////////////////////////////////////////////////

template<class FieldValueT>
std::string to_string( const FieldValueT &val )
{
    return std::to_string( val );
}

template<class T>
std::string to_string( const std::reference_wrapper<T> &val )
{
    return to_string( val.get() );
}

template<>
inline std::string to_string( const char &s )
{
    std::string res;
    res += '\'';
    res += s;
    res += '\'';
    return res;
}
template<>
inline std::string to_string( const std::string &s )
{
    return "\"" + s + "\"";
}

template<>
inline std::string to_string( const Timestamp &tsSinceEpoch )
{
    return tsSinceEpoch.to_string();
}

// Vec require size(), operator[]
template<class Vec>
std::string to_string_vec( const Vec &vec, const std::string &sep = ", ", const char *quotes = "[]" )
{
    std::string s;
    if ( quotes && quotes[0] )
        s += quotes[0];

    for ( size_t i = 0, N = vec.size(); i < N; ++i )
    {
        if ( !sep.empty() )
        {
            if ( i != 0 )
                s += sep;
        }
        s += to_string( vec[i] );
    }
    if ( quotes && quotes[1] )
        s += quotes[1];
    return s;
}

template<class... T>
std::string to_string( const std::vector<T...> &vec, const std::string &sep = ", ", const char *quotes = "[]" )
{
    return to_string_vec( vec, sep, quotes );
}

template<class Set>
std::string to_string_set( const Set &v, const std::string &sep = ", ", const char *quotes = "{}" )
{
    std::string s;
    if ( quotes && quotes[0] )
        s += quotes[0];
    int i = 0;
    for ( const auto &e : v )
    {
        if ( i++ )
            s += sep;
        s += to_string( e );
    }
    if ( quotes && quotes[1] )
        s += quotes[1];
    return s;
}
template<class... T>
std::string to_string( const std::unordered_set<T...> &v, const std::string &sep = ", ", const char *quotes = "{}" )
{
    return to_string_set( v, sep, quotes );
}
template<class Map>
std::string to_string_map( const Map &v, const std::string &kvSep = ": ", const std::string &sep = ", ", const char *quotes = "{}" )
{
    std::string s;
    if ( quotes && quotes[0] )
        s += quotes[0];
    int i = 0;
    for ( const auto &e : v )
    {
        if ( i++ )
            s += sep;
        s += to_string( e.first ) + kvSep + to_string( e.second );
    }
    if ( quotes && quotes[1] )
        s += quotes[1];
    return s;
}
template<class... T>
std::string to_string( const std::unordered_map<T...> &v, const std::string &kvSep = ": ", const std::string &sep = ", ", const char *quotes = "{}" )
{
    return to_string_map( v, kvSep, sep, quotes );
}
template<class... T, size_t nth = 0>
std::string to_string( const std::tuple<T...> &tup, std::string_view sep = ", ", const char *quotes = "()" )
{
    std::string s;
    if constexpr ( nth == 0 )
    {
        if ( quotes && quotes[0] )
            s += quotes[0];
    }
    else
    {
        if ( !sep.empty() )
            s += sep;
    }

    s += to_string( std::get<nth>( tup ) );

    if constexpr ( nth == std::tuple_size_v<std::tuple<T...>> )
    {
        if ( quotes && quotes[1] )
            s += quotes[1];
    }
    return s;
}


template<class T>
std::string to_string( const FieldValue<T> &val )
{
    if constexpr ( std::is_same_v<FieldValue<T>, NullField> )
        return global().nullstr;
    else
        return to_string( val.value );
}

template<class... T>
inline std::string to_string( const std::variant<T...> &var )
{
    std::string res;
    std::visit( [&]( const auto &fieldval ) { res = to_string( fieldval ); }, var );
    return res;
}

template<class... T>
std::ostream &operator<<( std::ostream &os, const std::unordered_set<T...> &s )
{
    os << to_string( s );
    return os;
}
template<class... T>
std::ostream &operator<<( std::ostream &os, const std::vector<T...> &s )
{
    os << to_string( s );
    return os;
}

template<class A, class... T>
std::ostream &operator<<( std::ostream &os, const std::variant<A, T...> &var )
{
    os << to_string( var );
    return os;
}

template<class A, class... T>
inline std::ostream &operator<<( std::ostream &os, const std::tuple<A, T...> &var )
{
    os << to_string( var );
    return os;
}

///////////////////////////////////////////////////////////////
/// from_str
///////////////////////////////////////////////////////////////

template<typename FieldValueT>
bool from_string( FieldValueT &val, std::string_view s );

template<>
inline bool from_string( Timestamp &val, std::string_view s )
{
    auto opDateTime = ParseDateTime( s );
    if ( opDateTime )
    {
        val = *opDateTime;
        return true;
    }
    return false;
}

template<class T>
bool from_string( FieldValue<T> &val, std::string_view s )
{
    if constexpr ( std::is_same_v<FieldValue<T>, NullField> )
    {
        return is_null( s );
    }
    if constexpr ( std::is_same_v<FieldValue<T>, StrField> )
    {
        val.value.assign( s.data(), s.length() );
        return true;
    }
    else if constexpr ( std::is_same_v<FieldValue<T>, CharField> )
    {
        val.value = s[0];
        return true;
    }
    else if constexpr ( std::is_same_v<FieldValue<T>, BoolField> )
    {
        auto x = s[0];
        if ( x == '0' || x == 'f' || x == 'F' || x == 'N' || x == 'n' )
            val.value = false;
        else if ( x == '1' || x == 't' || x == 'T' || x == 'Y' || x == 'y' )
            val.value = true;
        else
            return false;
        return true;
    }
    else if constexpr ( std::is_integral_v<typename FieldValue<T>::value_type> )
    {
        auto pEnd = s.data() + s.length();
        auto res = std::from_chars( s.data(), pEnd, val.value );
        return res.ptr == pEnd;
    }
    else if constexpr ( std::is_floating_point_v<typename FieldValue<T>::value_type> )
    {
        // todo: use std::from_chars
        char *pLast;
        auto pEnd = s.data() + s.length();
        auto x = strtold( s.data(), &pLast );
        if ( pLast == pEnd )
        {
            val.value = x;
            return true;
        }
        return false;
    }
    else if constexpr ( std::is_same_v<FieldValue<T>, TimestampField> ) // Timestamp
    {
        return from_string( val.value, s );
    }
    else
        return false;
}

/// \pre var must be preset with a value, type of which is used to parse string.
inline bool from_string( VarField &var, std::string_view s )
{
    if ( is_null( s ) && global().bParseNull )
    {
        var = NullField{};
        return true;
    }
    return std::visit( [&]( auto &fieldval ) { return from_string( fieldval, s ); }, var );
}

///////////////////////////////////////////////////////////////
/// hash and compare
///////////////////////////////////////////////////////////////

struct GreaterThan
{
    template<class T, class U>
    bool operator()( const T &a, const U &b )
    {
        return b < a;
    }
};

struct LessThan
{
    bool bReserse = false;

    template<class T, class U>
    bool operator()( const T &a, const U &b )
    {
        if ( bReserse )
            return b < a;
        return a < b;
    }
};

inline size_t hash_combine( size_t seed, size_t val )
{
    seed ^= val + 0x9e3779b9 + ( seed << 6 ) + ( seed >> 2 );
    return seed;
}
inline size_t hash_bytes( const void *p, size_t len )
{
    return std::hash<std::string_view>()( std::string_view( (const char *)p, len ) );
}

template<class T>
struct hash_code
{
    size_t operator()( const T &v ) const
    {
        return std::hash<T>()( v );
    }
};
struct HashCode
{
    template<class T>
    size_t operator()( const T &v ) const
    {
        return hash_code<T>()( v );
    }
};
static constexpr HashCode hashcode;

struct VecHash
{
    template<class T>
    size_t operator()( const T &lhs ) const
    {
        size_t r = hashcode( 0 );
        for ( size_t i = 0, N = lhs.size(); i < N; ++i )
        {
            r = i == 0 ? hashcode( lhs[i] ) : hash_combine( r, hashcode( lhs[i] ) );
        }
        return r;
    }
};

/// \note Empty vector has hash_code<size_t>(0).
template<class T>
struct hash_code<std::vector<T>>
{
    size_t operator()( const std::vector<T> &v ) const
    {
        return VecHash()( v );
    }
};

template<>
struct hash_code<Timestamp>
{
    size_t operator()( const Timestamp &v ) const
    {
        return hashcode( v.count() );
    }
};
template<>
struct hash_code<Null>
{
    size_t operator()( const Null &v ) const
    {
        return hashcode( 0u ); // NullField is hashed as 0u.
    }
};

struct TupleForeach
{
    // for each tuple element, call Func(index, const element&>
    template<class Func, class... Args, size_t idx = 0>
    void operator()( const Func &func, const std::tuple<Args...> &tup ) const
    {
        func( idx, std::get<idx>( tup ) );
        if constexpr ( idx + 1 < ( sizeof...( Args ) ) )
            operator()<Func, Args..., idx + 1>( func, tup );
    }
};

template<class... Args>
struct hash_code<std::tuple<Args...>>
{
    size_t operator()( const std::tuple<Args...> &v ) const
    {
        size_t res = hashcode( 0 );
        TupleForeach()( [&]( size_t idx, const auto &elem ) { res = hash_combine( res, hashcode( elem ) ); }, v );
        return res;
    }
};

template<class... Args>
struct hash_code<std::variant<Args...>>
{
    size_t operator()( const std::variant<Args...> &v ) const
    {
        return std::visit( []( const auto &elem ) { return hashcode( elem ); }, v );
    }
};

template<>
struct hash_code<VarField>
{
    size_t operator()( const VarField &val ) const
    {
        return std::visit( []( const auto &fieldval ) { return hashcode( fieldval.value ); }, val );
    }
};

struct VecEqual
{
    template<class T, class U>
    bool operator()( const T &lhs, const U &rhs ) const
    {
        if ( lhs.size() != rhs.size() )
            return false;
        for ( size_t i = 0, N = lhs.size(); i < N; ++i )
        {
            if ( !( lhs[i] == rhs[i] ) )
                return false;
        }
        return true;
    }
};
struct VecLess
{
    template<class T, class U>
    bool operator()( const T &lhs, const U &rhs ) const
    {
        const size_t N = lhs.size();
        const size_t M = rhs.size();
        for ( size_t i = 0, L = std::min( N, M ); i < L; ++i )
        {
            if ( lhs[i] < rhs[i] )
                return true;
            if ( rhs[i] < lhs[i] )
                return false;
        }
        return N < M;
    }
};

enum class IndexType : char
{
    OrderedIndex = 'O',
    ReverseOrderedIndex = 'R',
    HashIndex = 'H', // key: SingleValue
    HashMultiIndex = 'M', // key:MultiValues
};

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
    //    std::vector<FieldRef> getRowRef( size_t irow ) const
    //    {
    //        std::vector<FieldRef> res;
    //        for ( size_t i = 0, N = countCols(); i < N; ++i )
    //            res.push_back( std::cref( at( irow, i ) ) );
    //        return res;
    //    }
    //    std::vector<FieldRef> getRowRef( size_t irow, const std::vector<size_t> &icols ) const
    //    {
    //        std::vector<FieldRef> res;
    //        for ( auto icol : icols )
    //            res.push_back( std::cref( at( irow, icol ) ) );
    //        return res;
    //    }
    //    FieldRef getRowRef( size_t irow, size_t icol ) const
    //    {
    //        return std::cref( at( irow, icol ) );
    //    }

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
/// RecordOrFieldRef
///////////////////////////////////////////////////////////////

template<bool isSingleT>
struct RecordOrFieldRef
{
    static constexpr auto isSingle = isSingleT;
    using RecordType = std::conditional_t<isSingle, VarField, Record>;
    using ColsType = std::conditional_t<isSingle, size_t, std::vector<size_t>>;

    const IDataFrame *df = nullptr;
    Rowindex irow = 0;
    const ColsType *icols = nullptr; // vector<size_t>* or size_t*

    size_t size() const
    {
        if constexpr ( isSingle )
            return 1;
        else
            return icols->size();
    }
    const VarField &at( size_t nthField ) const
    {
        if constexpr ( isSingle )
        {
            assert( nthField == 0 );
            return df->at( irow, *icols );
        }
        else
        {
            if ( nthField >= icols->size() )
                throw std::out_of_range( "RecordRef nthField:" + std::to_string( nthField ) + " >= " + std::to_string( icols->size() ) );
            return df->at( irow, icols->at( nthField ) );
        }
    }
    const VarField &operator[]( size_t nthField ) const
    {
        return at( nthField );
    }
};

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

} // namespace zj
