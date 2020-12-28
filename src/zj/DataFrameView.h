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

namespace zj
{

class IDataFrameView : public IDataFrame
{
protected:
    const IDataFrame *m_pDataFrame = nullptr;

public:
    virtual size_t underlyingRow( size_t irow ) const = 0;
    virtual size_t underlyingCol( size_t icol ) const = 0;

    virtual std::array<size_t, 2> underlyingPos( size_t irow, size_t icol ) const
    {
        return {underlyingRow( irow ), underlyingRow( icol )};
    }
    virtual const IDataFrame *underlying() const
    {
        return m_pDataFrame;
    }

    //////////////////////////////////////////////////////////
    /// Implement IDataFrame
    //////////////////////////////////////////////////////////

    /// \return the current view shape.
    //    virtual std::array<size_t, 2> shape() const override = 0;

    const VarField &at( size_t irow, size_t icol ) const override
    {
        assert( m_pDataFrame );
        return m_pDataFrame->at( underlyingRow( irow ), underlyingCol( icol ) );
    }
    const VarField &at( size_t irow, const std::string &col ) const override
    {
        return m_pDataFrame->at( underlyingRow( irow ), col );
    }
    const std::string &colName( size_t icol ) const override
    {
        return m_pDataFrame->colName( underlyingCol( icol ) );
    }

    const ColumnDef &columnDef( size_t icol ) const override
    {
        return m_pDataFrame->columnDef( underlyingCol( icol ) );
    }
    const ColumnDef &columnDef( const std::string &colName ) const override
    {
        return m_pDataFrame->columnDef( colName );
    }
    bool isView() const override
    {
        return true;
    }
    IDataFrame *deepCopy() const override
    {
        return m_pDataFrame->deepCopy();
    }

public:
    static bool check_underlying_rows( const IDataFrame &underlying, const std::vector<std::size_t> &irows, std::ostream *err = nullptr )
    {
        size_t nrow = underlying.countRows();
        for ( auto r : irows )
            if ( r >= nrow )
            {
                if ( err )
                    *err << "Rowindex:" << r << " is not in range:" << nrow << ".\n";
                return false;
            }
        return true;
    }
    static bool check_underlying_cols( const IDataFrame &underlying, const std::vector<std::size_t> &icols, std::ostream *err = nullptr )
    {
        size_t ncol = underlying.countCols();
        for ( auto c : icols )
            if ( c >= ncol )
            {
                if ( err )
                    *err << "Colindex:" << c << " is not in range:" << ncol << ".\n";
                return false;
            }
        return true;
    }
};


/// \brief DataFrameView is a view of selected [rows, columns] of ]underlying DataFrame.
class DataFrameView : public IDataFrameView
{
    std::vector<std::size_t> m_colIndices; // row index of underlying DataFrame.
    std::unordered_map<std::string, size_t> m_columnNames; // <name: currentIndex>
    std::vector<std::size_t> m_rowIndices; // row index of underlying DataFrame.

public:
    bool create( const IDataFrame &df, std::vector<std::size_t> irows, std::vector<std::size_t> icols, std::ostream *err = nullptr )
    {
        if ( !create_column_view_impl( df, std::move( icols ), err ) )
            return false;
        return create_row_view_impl( df, std::move( irows ), err );
    }
    bool create( const IDataFrame &df, std::vector<std::size_t> irows, std::vector<std::string> colNames, std::ostream *err = nullptr )
    {
        return create( df, std::move( irows ), df.colIndex( colNames ), err );
    }
    bool create_column_view( const IDataFrame &df, std::vector<std::size_t> icols, std::ostream *err = nullptr )
    {
        if ( !create_column_view_impl( df, std::move( icols ), err ) )
            return false;
        std::vector<size_t> irows( df.countRows(), 0 );
        std::iota( irows.begin(), irows.end(), 0 );
        return create_row_view_impl( df, std::move( irows ), err );
    }
    bool create_column_view( const IDataFrame &df, std::vector<std::string> colNames, std::ostream *err = nullptr )
    {
        return create_column_view( df, df.colIndex( colNames ), err );
    }
    bool create_row_view( const IDataFrame &df, std::vector<std::size_t> irows, std::ostream *err = nullptr )
    {
        if ( !create_row_view_impl( df, std::move( irows ), err ) )
            return false;
        std::vector<size_t> icols( df.countCols(), 0 );
        std::iota( icols.begin(), icols.end(), 0 );
        return create_column_view_impl( df, std::move( icols ), err );
    }

    void sort_by( const std::vector<std::string> &colNames, bool bReverseOrder = false )
    {
        MultiColOrderedIndex ordered;
        ordered.create( *this, colNames, bReverseOrder );
        const auto &orderedRows = ordered.getRowIndices();
        std::vector<Rowindex> newIndices;
        newIndices.reserve( size() );
        for ( auto i : orderedRows )
            newIndices.push_back( m_rowIndices[i] );
        m_rowIndices = std::move( newIndices );
    }

    //////////////////////////////////////////////////////////
    /// Implement IDatatFrameView
    //////////////////////////////////////////////////////////

    size_t underlyingRow( size_t irow ) const override
    {
        return m_rowIndices[irow];
    }
    size_t underlyingCol( size_t icol ) const override
    {
        return m_colIndices[icol];
    }

    //////////////////////////////////////////////////////////
    /// Implement IDataFrame
    //////////////////////////////////////////////////////////

    size_t countCols() const override
    {
        return m_colIndices.size();
    }
    size_t countRows() const override
    {
        return m_rowIndices.size();
    }
    size_t colIndex( const std::string &colName ) const override
    {
        if ( auto it = m_columnNames.find( colName ); it != m_columnNames.end() )
            return it->second;
        throw std::out_of_range( "Failed to find DataFrameView column name: " + colName );
    }

protected:
    bool create_column_view_impl( const IDataFrame &df, std::vector<std::size_t> &&icols, std::ostream *err = nullptr )
    {
        if ( !check_underlying_cols( df, icols, err ) )
            return false;
        m_pDataFrame = &df;
        if ( df.isView() )
        {
            m_colIndices.resize( icols.size() );
            const IDataFrameView *pView = dynamic_cast<const IDataFrameView *>( &df );
            m_pDataFrame = pView->underlying();
            for ( size_t i = 0, N = icols.size(); i < N; ++i )
                m_colIndices[i] = pView->underlyingCol( icols[i] );
        }
        else
            m_colIndices = std::move( icols );
        // construct m_columnNames
        m_columnNames.clear();
        for ( size_t i = 0, N = countCols(); i < N; ++i )
            m_columnNames[m_pDataFrame->colName( underlyingCol( i ) )] = i;
        return true;
    }
    bool create_row_view_impl( const IDataFrame &df, std::vector<std::size_t> &&irows, std::ostream *err = nullptr )
    {
        if ( !check_underlying_rows( df, irows, err ) )
            return false;
        m_pDataFrame = &df;
        if ( df.isView() )
        {
            m_rowIndices.resize( irows.size() );
            const IDataFrameView *pView = dynamic_cast<const IDataFrameView *>( &df );
            m_pDataFrame = pView->underlying();
            for ( size_t i = 0, N = irows.size(); i < N; ++i )
                m_rowIndices[i] = pView->underlyingRow( irows[i] );
        }
        else
            m_rowIndices = std::move( irows );
        return true;
    }
};

using DataFrameViewPtr = std::shared_ptr<DataFrameView>;

// todo: SparseDataFrameView

} // namespace zj
