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

/**
 * @brief The DataFrame class with Hash/Ordered Index
 */

class DataFrameBasic : public IDataFrame
{
protected:
    std::vector<ColumnDef> m_columnDefs;
    std::vector<Record> m_records;

    std::unordered_map<std::string, size_t> m_columnIndex; // <name: index>

public:
    DataFrameBasic() = default;

    DataFrameBasic( const std::vector<std::vector<std::string>> &rows, const ColumnDefs &columnDefs = {}, std::ostream *err = nullptr )
    {
        if ( !from_records( rows, columnDefs, err ) )
            clear();
    }
    template<class... T>
    DataFrameBasic( const std::vector<std::tuple<T...>> &tups, const std::vector<std::string> &colNames = {}, std::ostream *err = nullptr )
    {
        if ( !from_tuples( tups, colNames, err ) )
            clear();
    }
    ~DataFrameBasic() override
    {
        clear();
    }
    bool from_records( const std::vector<std::vector<std::string>> &rows, const ColumnDefs &columnDefs = {}, std::ostream *err = nullptr )
    {
        clear();
        m_columnDefs = columnDefs;
        // convert strings to FieldValue types.
        int irow = 0;
        for ( const auto &row : rows )
        {
            if ( row.size() != columnDefs.size() )
            {
                if ( err )
                    *err << "from_records: Failed construct row=" << irow << ". NumFields=" << row.size()
                         << " is not equal to columns=" << columnDefs.size() << ".\n";
                return false;
            }
            Record rec;
            for ( size_t i = 0; i < columnDefs.size(); ++i )
            {
                VarField afield = create_default_field( columnDefs[i].colTypeTag );
                if ( from_string( afield, row[i] ) )
                {
                    rec.emplace_back( std::move( afield ) );
                }
                else
                {
                    if ( err )
                        *err << "from_records: Failed to parse (row=" << irow << ", col=" << i << "): " << row[i] << ".\n";
                    return false;
                }
            }
            m_records.push_back( std::move( rec ) );
            ++irow;
        }
        createColumnIndex();
        return true;
    }

    template<class... T>
    bool from_tuples( const std::vector<std::tuple<T...>> &tups, const std::vector<std::string> &colNames = {}, std::ostream *err = nullptr )
    {
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
            create_record( rec, tup );
            m_records.push_back( std::move( rec ) );
        }
        createColumnIndex();
        return true;
    }

    bool canAppend( const IDataFrame &rhs, std::ostream *err = nullptr ) const
    {
        // check if another has all consistent columns that <columnName, columnType> is the same.
        for ( const auto &col : m_columnDefs )
        {
            if ( auto icol = rhs.colIndex( col.colName ) ) // check colName
            {
                auto &colDef = rhs.columnDef( *icol );
                if ( col.colTypeTag != colDef.colTypeTag ) // check colType
                {
                    if ( err )
                        *err << "Failed to append: column " << col.colName << " type doesn't match " << typeName( col.colTypeTag )
                             << " != " << typeName( colDef.colTypeTag ) << ".\n";
                    return false;
                }
            }
            else
            {
                if ( err )
                    *err << "Failed to append: rhs DataFrame doesn't have col=" << col.colName << ".\n";
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
            for ( size_t i = 0, ncols = rhs.shape()[1]; i < ncols; ++i )
            {
                m_columnDefs.push_back( rhs.columnDef( i ) );
            }
            createColumnIndex();
        }
        for ( size_t i = 0, nrows = rhs.shape()[0]; i < nrows; ++i )
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

    std::array<size_t, 2> shape() const override
    {
        return {m_records.size(), m_columnDefs.size()};
    }
    size_t size() const override
    {
        return m_records.size();
    }
    const VarField &at( size_t irow, size_t icol ) const override
    {
        return m_records[irow][icol];
    }
    const VarField &at( size_t irow, const std::string &col ) const override
    {
        if ( auto icol = colIndex( col ) )
        {
            return m_records[irow][*icol];
        }
        else
        {
            throw std::out_of_range( col );
        }
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
        return m_columnDefs[icol];
    }
    const ColumnDef &columnDef( const std::string &colName ) const override
    {
        if ( auto icol = colIndex( colName ) )
        {
            return m_columnDefs[*icol];
        }
        else
            throw std::out_of_range( colName );
    }

    std::string &colName( size_t icol )
    {
        return m_columnDefs[icol].colName;
    }
    const std::string &colName( size_t icol ) const override
    {
        return m_columnDefs[icol].colName;
    }
    const std::vector<std::string> colNames( const std::vector<size_t> &icols ) const override
    {
        std::vector<std::string> res;
        for ( auto idx : icols )
        {
            if ( idx < m_columnDefs.size() )
                res.push_back( m_columnDefs[idx].colName );
            else
                return {};
        }
        return res;
    }

    // return
    std::optional<size_t> colIndex( const std::string &colName ) const override
    {
        if ( auto it = m_columnIndex.find( colName ); it != m_columnIndex.end() )
            return it->second;
        return {};
    }
    std::vector<size_t> colIndice( const std::vector<std::string> &colNames ) const override
    {
        std::vector<size_t> res;
        for ( const auto &n : colNames )
        {
            if ( auto r = colIndex( n ) )
                res.push_back( *r );
            else
                return {};
        }
        return res;
    }
    std::ostream &print( std::ostream &os, bool bHeader = true, char sepField = '|', char sepRow = '\n' ) const
    {
        if ( bHeader )
        {
            int i = 0;
            for ( auto &col : m_columnDefs )
            {
                if ( i++ )
                    os << sepField;
                os << col.colName;
            }
            os << sepRow;
        }
        for ( auto &row : m_records )
        {
            int j = 0;
            for ( auto &f : row )
            {
                if ( j++ )
                    os << sepField;
                os << to_string( f );
            }
            os << sepRow;
        }
        return os;
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
        m_columnIndex.clear();
        for ( size_t i = 0, N = m_columnDefs.size(); i < N; ++i )
            m_columnIndex[m_columnDefs[i].colName] = i;
    }
};

} // namespace df
