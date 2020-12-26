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
#include <algorithm>
#include <stdexcept>

/// SparseVector

namespace zj
{


enum class SegType
{
    Duplicate, // all elements have the same value.
    Incremental // each element incremnets by 1
};
template<class T>
struct Segment
{
    size_t m_size;
    size_t m_endIdx;
    SegType m_segType;
    T m_value;

    const T &atGlobal( size_t idx ) const
    {
        assert( idx < m_endIdx && idx >= m_endIdx - m_size );
        return m_segType == SegType::Duplicate ? m_value : ( m_value + idx - ( m_endIdx - m_size ) );
    }
    const T &atLocal( size_t idx ) const
    {
        assert( idx < m_size );
        return m_segType == SegType::Duplicate ? m_value : ( m_value + idx );
    }
    size_t localToGlobal( size_t idx ) const
    {
        return m_endIdx - m_size + idx;
    }
    size_t globalToLocal( size_t idx ) const
    {
        return idx - ( m_endIdx - m_size );
    }

    size_t size() const
    {
        return m_size;
    }
};

template<class T>
struct SparseVector
{
    static_assert( std::is_integral_v<T>, "T must be integral!" );

    using this_type = SparseVector;
    static constexpr size_t MAXULONG = std::numeric_limits<size_t>::max();
    struct Iter
    {

        const this_type *vec = nullptr;
        size_t iSeg = MAXULONG;
        size_t idxInSeg = MAXULONG;

        bool isEnd() const
        {
            return iSeg = MAXULONG;
        }
        void setEnd()
        {
            iSeg = MAXULONG;
        }
        Iter &operator++()
        {
            if ( isEnd() )
                return *this;
            if ( vec->m_segs[iSeg].size() == idxInSeg + 1 ) // move to next seg
            {
                if ( iSeg + 1 >= vec->m_segs.size() ) // if this is the last seg, set to end
                    setEnd();
                else
                {
                    ++iSeg;
                    idxInSeg = 0;
                }
            }
            else
                ++idxInSeg;
            return *this;
        }
        Iter operator++( int ) const
        {
            Iter res = *this;
            this->operator++();
            return res;
        }
        size_t index() const
        {
            if ( isEnd() )
                return vec->size();
            return vec->m_segs[iSeg].localToGlobal( idxInSeg );
        }
        ssize_t distanceTo( const Iter &a ) const
        {
            return ssize_t( a.index() ) - ssize_t( index() );
        }

        const T *operator->() const
        {
            return &vec->m_segs[iSeg].atLocal( idxInSeg );
        }
        const T &operator*() const
        {
            return vec->m_segs[iSeg].atLocal( idxInSeg );
        }
    };

    using Seg = Segment<T>;
    using iterator = Iter;
    std::vector<Segment<T>> m_segs;

    void push_back_duplicates( const T &val, size_t n )
    {
        size_t endIndex = m_segs.empty() ? n : ( m_segs.back().m_endIdx + n );
        m_segs.push_back( Seg{n, endIndex, SegType::Duplicate, val} );
    }
    void push_back_incremental( const T &val, size_t n )
    {
        size_t endIndex = m_segs.empty() ? n : ( m_segs.back().m_endIdx + n );
        m_segs.push_back( Seg{n, endIndex, SegType::Incremental, val} );
    }
    size_t size() const
    {
        return m_segs.empty() ? 0 : m_segs.back().m_endIdx;
    }
    const T &at( size_t idx ) const
    {
        if ( m_segs.empty() || idx >= m_segs.back().m_endIdx )
            throw std::out_of_range( "SparseVector idx:" + std::to_string( idx ) );
        auto it = std::upper_bound( m_segs.begin(), m_segs.end(), idx, [this]( int idx, const Seg &s ) { return idx < s.m_endIdx; } );
        assert( it != m_segs.end() );
        return m_segs.atGlobal( idx );
    }
    const T &operator[]( size_t idx ) const
    {
        return at( idx );
    }
    bool empty() const
    {
        return m_segs.empty();
    }
    iterator end() const
    {
        return {this, MAXULONG, MAXULONG};
    }
    iterator begin() const
    {
        return empty() ? end() : iterator{this, 0, 0};
    }
};

} // namespace zj
