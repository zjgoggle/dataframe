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
#include <sstream>

namespace zj
{

/**
 * @brief The DataFrame class with Hash/Ordered Index
 */

class RowDataFrame : public IDataFrame
{
protected:
    std::vector<ColumnDef> m_columnDefs;
    std::vector<Record> m_records;

    std::unordered_map<std::string, size_t> m_columnNames; // <name: index>

    bool m_allowNullField = true;

public:
    RowDataFrame() = default;

    RowDataFrame( const std::vector<std::vector<std::string>> &rows, const ColumnDefs &columnDefs )
    {
        std::stringstream err;
        if ( !from_records( rows, columnDefs, &err ) )
            throw std::runtime_error( "Failed to create RowDataFrame from strings vector: " + err.str() );
    }
    template<class... T>
    RowDataFrame( const std::vector<std::tuple<T...>> &tups, const std::vector<std::string> &colNames = {} )
    {
        std::stringstream err;
        if ( !from_tuples( tups, colNames, err ) )
            throw std::runtime_error( "Failed to create RowDataFrame from tuple vector: " + err.str() );
    }
    ~RowDataFrame() override
    {
        clear();
    }
    bool from_records( const std::vector<std::vector<std::string>> &rows, const ColumnDefs &columnDefs = {}, std::ostream *err = nullptr )
    {
        clear();
        m_columnDefs = columnDefs;
        // convert strings to FieldValue types.
        for ( const auto &row : rows )
        {
            if ( !appendRecordStr( row, err ) )
                return false;
        }
        createColumnIndex();
        return true;
    }

    /// TODO: N/A value policy for each column: remove the record, save as null, or report error.
    bool appendRecordStr( const std::vector<std::string> &row, std::ostream *err = nullptr )
    {
        if ( m_columnDefs.empty() )
        {
            if ( err )
                *err << "Failed appendRecordStr: RowDataFrame is not created yet!\n";
            return false;
        }

        if ( row.size() != m_columnDefs.size() )
        {
            if ( err )
                *err << "from_records: Failed construct row=" << to_string( row ) << ". NumFields=" << row.size()
                     << " is not equal to columns=" << m_columnDefs.size() << ".\n";
            return false;
        }
        Record rec;
        for ( size_t i = 0; i < m_columnDefs.size(); ++i )
        {
            VarField afield = create_default_field( m_columnDefs[i].colTypeTag );
            if ( from_string( afield, row[i] ) )
            {
                rec.emplace_back( std::move( afield ) );
            }
            else
            {
                if ( err )
                    *err << "from_records: Failed to parse (row element=" << to_string( row[i] ) << ", col=" << i << ").\n";
                return false;
            }
        }
        if ( !is_record_compatible( rec, m_columnDefs, err, m_allowNullField ) )
            return false;
        m_records.push_back( std::move( rec ) );
        return true;
    }

    template<class... T>
    bool from_tuples( const std::vector<std::tuple<T...>> &tups, const std::vector<std::string> &colNames = {}, std::ostream *err = nullptr )
    {
        static_assert( CompatibleFieldTypes<T...>(), "" );
        clear();

        const std::vector<std::string> *pNames = &colNames;
        std::vector<std::string> generatedNames;
        size_t nCol = std::tuple_size_v<std::tuple<T...>>;
        if ( colNames.empty() )
        {
            for ( auto i = 0u; i < nCol; ++i )
            {
                generatedNames.push_back( "Col" + std::to_string( i ) );
            }
            pNames = &generatedNames;
        }
        else if ( colNames.size() != nCol )
        {
            if ( err )
                *err << "Expecting " << nCol << " names, privided " << colNames.size() << ".\n";
            return false;
        }
        create_columns<0, T...>( m_columnDefs, *pNames );

        for ( const auto &tup : tups )
        {
            Record rec;
            if ( !create_record( rec, tup, err, m_allowNullField ) )
                return false;
            m_records.push_back( std::move( rec ) );
        }
        createColumnIndex();
        return true;
    }

    template<class... T>
    bool appendTupple( const std::tuple<T...> &tup, std::ostream *err = nullptr )
    {
        static_assert( CompatibleFieldTypes<T...>(), "" );
        if ( m_columnDefs.empty() )
        {
            if ( err )
                *err << "Failed appendRecordStr: RowDataFrame is not created yet!\n";
            return false;
        }
        auto tupSize = std::tuple_size_v<std::tuple<T...>>;
        if ( tupSize != m_columnDefs.size() )
        {
            if ( err )
                *err << "appendTupple: Failed construct row=" << to_string( tup ) << ". NumFields=" << tupSize
                     << " is not equal to columns=" << m_columnDefs.size() << ".\n";
            return false;
        }
        Record rec;
        if ( !create_record( rec, tup, err, m_allowNullField ) )
            return false;
        m_records.push_back( std::move( rec ) );
        return true;
    }

    bool canAppend( const IDataFrame &rhs, std::ostream *err = nullptr ) const
    {
        // check if another has all consistent columns that <columnName, columnType> is the same.
        for ( const auto &col : m_columnDefs )
        {
            auto &colDef = rhs.columnDef( rhs.colIndex( col.colName ) );
            if ( col.colTypeTag != colDef.colTypeTag ) // check colType
            {
                if ( err )
                    *err << "Failed to append: column " << col.colName << " type doesn't match " << typeName( col.colTypeTag )
                         << " != " << typeName( colDef.colTypeTag ) << ".\n";
                return false;
            }
        }
        return true;
    }

    // if this is empty, copy from rhs.
    bool append( const IDataFrame &rhs, std::ostream *err = nullptr )
    {
        if ( !canAppend( rhs, err ) )
            return false;
        if ( m_columnDefs.empty() )
        {
            assert( m_records.empty() );
            // copy columns
            for ( size_t i = 0, ncols = countCols(); i < ncols; ++i )
            {
                m_columnDefs.push_back( rhs.columnDef( i ) );
            }
            createColumnIndex();
        }
        for ( size_t i = 0, nrows = countRows(); i < nrows; ++i )
        {
            Record rec;
            for ( const auto &col : m_columnDefs )
            {
                const auto &val = rhs.at( i, col.colName );
                VarField var = val;
                rec.push_back( std::move( var ) );
            }
            m_records.push_back( std::move( rec ) );
        }

        return true;
    }
    IDataFrame *deepCopy() const override
    {
        RowDataFrame *a = new RowDataFrame();
        a->m_columnDefs = m_columnDefs;
        a->m_columnNames = m_columnNames;
        a->m_records = m_records;
        return a;
    }

    size_t countRows() const override
    {
        return m_records.size();
    }
    size_t countCols() const override
    {
        return m_columnDefs.size();
    }
    const VarField &at( size_t irow, size_t icol ) const override
    {
        if ( icol > countCols() )
            throw std::out_of_range( "icol our of range: " + to_string( icol ) + " >= " + to_string( m_columnDefs.size() ) );
        if ( irow > countRows() )
            throw std::out_of_range( "irow our of range: " + to_string( irow ) + " >= " + to_string( countRows() ) );
        return m_records[irow][icol];
    }
    const VarField &at( size_t irow, const std::string &col ) const override
    {
        return m_records.at( irow ).at( colIndex( col ) );
    }
    const VarField &operator()( size_t irow, size_t icol ) const
    {
        return at( irow, icol );
    }
    const VarField &operator()( size_t irow, const std::string &col ) const
    {
        return at( irow, col );
    }

    const ColumnDef &columnDef( size_t icol ) const override
    {
        if ( icol >= m_columnDefs.size() )
        {
            throw std::out_of_range( "icol our of range: " + to_string( icol ) + " >= " + to_string( m_columnDefs.size() ) );
        }
        return m_columnDefs[icol];
    }
    const ColumnDef &columnDef( const std::string &colName ) const override
    {
        return m_columnDefs.at( colIndex( colName ) );
    }

    std::string &colName( size_t icol )
    {
        return m_columnDefs.at( icol ).colName;
    }
    const std::string &colName( size_t icol ) const override
    {
        return m_columnDefs[icol].colName;
    }

    // return
    size_t colIndex( const std::string &colName ) const override
    {
        if ( auto it = m_columnNames.find( colName ); it != m_columnNames.end() )
            return it->second;
        throw std::out_of_range( "Failed to find DataFrame column name:" + colName );
    }

    void clearRecords()
    {
        m_records.clear();
    }
    /// \brief clear records and columns.
    void clear()
    {
        m_columnDefs.clear();
        clearRecords();
    }

protected:
    void createColumnIndex()
    {
        m_columnNames.clear();
        for ( size_t i = 0, N = m_columnDefs.size(); i < N; ++i )
            m_columnNames[m_columnDefs[i].colName] = i;
    }
};

} // namespace zj
