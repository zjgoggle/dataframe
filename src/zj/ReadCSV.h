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

#include <vector>
#include <string>
#include <iostream>
#include <functional>

namespace zj
{

/// \brief Read csv rows.
/// Throw runtime_error if number of column varies on rows.
/// param RecordFilter: bool rowFilter(std::vector<std::string>& rec). return false to skip current row, true to add it to result.
inline std::vector<std::vector<std::string>> read_csv_strings( std::istream &is,
                                                               char sep = ',',
                                                               size_t skipLines = 0,
                                                               std::function<bool( std::vector<std::string> & )> rowFilter = nullptr,
                                                               char commentChar = 0,
                                                               const char *quotes = "\"\"" )
{
    std::vector<std::vector<std::string>> rows;
    int iLine = 0;

    auto skipLine = [&] { // return false if EOF
        for ( int c; is.good() && ( c = is.get() ) != EOF; )
            if ( c == '\n' )
            {
                ++iLine;
                return true;
            }
        return false;
    };
    auto trimRight = []( const std::string &s ) {
        int i = int( s.length() ) - 1;
        for ( ; i >= 0 && isspace( s[i] ); --i )
            continue;
        return s.substr( 0, i + 1 );
    };
    for ( auto i = 0u; i < skipLines; ++i )
    {
        if ( !skipLine() )
            return {};
    }
    while ( is.good() )
    {
        std::vector<std::string> row;
        bool endOfLine = false, endOfFile = false;
        while ( !endOfLine && !endOfFile ) // loop until endOfLine or endOfFile
        {
            int c;
            //- skip the spaces
            while ( is.good() ) // skip spaces.
            {
                c = is.get();
                if ( c == EOF )
                {
                    endOfFile = true;
                    endOfLine = true;
                }
                if ( c == '\n' )
                {
                    endOfLine = true;
                    ++iLine;
                    break;
                }
                if ( isspace( c ) )
                    continue;
                if ( commentChar && c == commentChar )
                {
                    if ( !skipLine() )
                        endOfFile = true;
                    endOfLine = true;
                }
                break;
            }
            if ( endOfLine )
                break;
            if ( quotes && c == quotes[0] ) // start quotes, is able to handle  escaped char "\n", "\"".
            {
                std::string s;
                while ( true )
                {
                    if ( !is.good() )
                        throw std::runtime_error( "EOF while Reading quoted string:" + s + " at line: " + std::to_string( iLine ) );
                    c = is.get();
                    if ( c == '\\' && is.good() ) // escaped char
                    {
                        auto x = is.get();
                        if ( x == 'n' )
                            s += '\n';
                        else if ( x == '\"' )
                            s += '\"';
                        else
                        {
                            s += char( c );
                            s += char( x );
                        }
                    }
                    else if ( c == quotes[1] ) // end of quote
                        break;
                    else if ( c == '\n' )
                        throw std::runtime_error( "EndOfLine while Reading quoted string:" + s + " at line: " + std::to_string( iLine ) );
                    else
                        s += char( c );
                }
                // continue read spaces till EOF or EOL, or SEP
                while ( !endOfLine )
                {
                    if ( !is.good() )
                    {
                        endOfFile = true;
                        endOfLine = true;
                    }
                    c = is.get();
                    if ( c == EOF )
                    {
                        endOfFile = true;
                        endOfLine = true;
                    }
                    else if ( c == '\n' )
                    {
                        endOfLine = true;
                        ++iLine;
                        break;
                    }
                    else if ( c == sep )
                        break;
                    else if ( !isspace( c ) )
                        throw std::runtime_error( "ERROR! Non-space char after quoted string: " + s + " at line: " + std::to_string( iLine ) );
                }
                row.push_back( std::move( s ) );
            }
            else // read non-quoted string.
            {
                std::string s;
                s += char( c );
                while ( true )
                {
                    if ( !is.good() )
                    {
                        endOfFile = true;
                        endOfLine = true;
                        break;
                    }
                    c = is.get();
                    if ( c == sep ) // end of quote
                        break;
                    else if ( c == '\n' )
                    {
                        endOfLine = true;
                        ++iLine;
                        break;
                    }
                    else if ( c == EOF )
                    {
                        endOfFile = true;
                        endOfLine = true;
                    }
                    else
                        s += char( c );
                }
                if ( s.empty() && ( endOfFile || endOfLine ) ) // empty string
                {
                }
                else
                {
                    s = trimRight( s ); // note: maybe empty, but ends with sep.
                    row.push_back( std::move( s ) );
                }
            }
        } // while ( !endOfLine && !endOfFile )
        if ( !row.empty() && ( !rowFilter || rowFilter( row ) ) )
        {
            rows.push_back( std::move( row ) );
        }
    }
    return rows;
}

} // namespace zj
