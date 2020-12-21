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

#include <stdint.h>
#include <time.h>
#include <string.h>

#include <chrono>
#include <optional>
#include <charconv>
#include <iostream>

namespace df
{

// get current local timzone offset.
inline int getLocalGMTOffsetSec()
{
    static int offset = INT32_MAX;
    if ( offset == INT32_MAX )
    {
        tm t;
        time_t timet = time( nullptr );
        localtime_r( &timet, &t );
        offset = t.tm_gmtoff;
    }
    return offset;
}
// get local timezone offset at given time
inline int getLocalGMTOffsetSec( tm atm )
{
    timelocal( &atm );
    return atm.tm_gmtoff;
}
// get local timezone offset at given time
inline int getLocalGMTOffsetSec( time_t timet )
{
    tm t;
    localtime_r( &timet, &t );
    return t.tm_gmtoff;
}

// 2009-06-15T13:45:30.000-07:00
// subsecondDigits: 0 - 9.
// asLocalOffsetMinutes: print as local time specified by timeZoneSeconds.
// bPrintTimeZoneOffset: 1, always print timezone  "+xxxx" or "-xxxx"; 0, never print time zone; -1, print timezone only when tzOffsetMinutes is
// specified. bUseGMTOffsetIfNotSpecified: if true, use offset 0 if asLocalTimeOffsetMinutes is empty; use local offset at given time.
char *PrintTimestamp( char *buf,
                      size_t bufsize,
                      const std::chrono::nanoseconds &tp,
                      const char *fmt = "%Y-%m-%dT%T",
                      unsigned subsecondDigits = 0,
                      int bPrintTimeZoneOffset = -1,
                      std::optional<int> asLocalTimeOffsetMinutes = {},
                      bool bUseGMTOffsetIfNotSpecified = false );

// DateTime. To be improved:
// - Date only struct. Time only struct.
// - Negative value.
// - Date/Time subtract/addition.
struct DateTime
{
    unsigned year = 0;
    unsigned month = 0, mday = 0;

    unsigned hour = 0, min = 0, sec = 0; // month: 1-12, mday:1-31, sec: 0-60
    size_t nanosec = 0; // 0-1000000000
    std::optional<int> tzOffsetMinutes; //

    enum
    {
        USE_DATETIME = 0,
        DATEONLY = 1,
        TIMEONLY = 2
    };

    // 1 for Date only; 2 for time only; 0 , use both date and time.
    // if Date is not in use, use the current date to generte tm and duration.
    // if Time is not in use, use 00:00:00 by default.
    int dateOrTimeOnly = USE_DATETIME;

    DateTime &dateonly( unsigned y, unsigned m, unsigned d )
    {
        dateOrTimeOnly = DATEONLY;
        year = y;
        month = m;
        mday = d;
        return *this;
    }
    DateTime &timeonly( unsigned h, unsigned m, unsigned s, unsigned nano = 0, std::optional<int> tzMinutes = {} )
    {
        dateOrTimeOnly = TIMEONLY;
        hour = h;
        min = m;
        sec = s;
        nanosec = nano;
        tzOffsetMinutes = tzMinutes;
        return *this;
    }
    DateTime &setDateOnly()
    {
        tzOffsetMinutes.reset();
        dateOrTimeOnly = DATEONLY;
        return *this;
    }
    DateTime &setTimeOnly()
    {
        dateOrTimeOnly = TIMEONLY;
        return *this;
    }
    bool hasTime() const
    {
        return dateOrTimeOnly != DATEONLY;
    }
    bool hasDate() const
    {
        return dateOrTimeOnly != TIMEONLY;
    }
    // tm.tm_gmtoff is ignored.
    void from_tm( const tm &t, size_t nanosec = 0, std::optional<int> tzMinutes = {} )
    {
        dateOrTimeOnly = USE_DATETIME;
        year = t.tm_year + 1900;
        month = t.tm_mon + 1;
        mday = t.tm_mday;
        hour = t.tm_hour;
        min = t.tm_min;
        sec = t.tm_sec;
        this->nanosec = nanosec;
        tzOffsetMinutes = tzMinutes;
    }
    // asUTCIfNoTimeZone: if tzOffsetMinutes doesn't have value, treat it as UTC time; otherwise, use local time offset.
    std::chrono::nanoseconds time_since_epoch( bool asUTCIfNoTimeZone = false ) const
    {
        tm t = to_tm();
        time_t timet;
        timet = timegm( &t );
        if ( tzOffsetMinutes )
            timet -= ( *tzOffsetMinutes ) * 60;
        else if ( !asUTCIfNoTimeZone )
            timet -= getLocalGMTOffsetSec( t );
        return std::chrono::nanoseconds{timet * 1000000000 + nanosec};
    }
    // return nanoseconds since epoch.
    int64_t count() const
    {
        return time_since_epoch().count();
    }
    // tm.tm_gmtoff is ignored.
    tm to_tm() const
    {
        tm t;
        memset( &t, 0, sizeof( t ) );
        if ( hasDate() )
        {
            t.tm_year = year - 1900;
            t.tm_mon = month - 1;
            t.tm_mday = mday;
        }
        else // use current date
        {
            tm t1;
            time_t timet = time( nullptr );
            localtime_r( &timet, &t1 );
            t.tm_year = t1.tm_year;
            t.tm_mon = t1.tm_mon;
            t.tm_mday = t1.tm_mday;
        }
        if ( hasTime() )
        {
            t.tm_hour = hour;
            t.tm_min = min;
            t.tm_sec = sec;
        }
        return t;
    }

    // bPrintTimeZone: 1, always print timezone, 0, never print time zone; -1, print timezone only when tzOffsetMinutes is specified.
    std::string to_string( const char *adateFmt = "%Y-%m-%d",
                           unsigned nSubsecondDigits = 0,
                           int bPrintTimeZone = -1,
                           bool asUTCIfNoTimeZone = false ) const
    {
        char buf[40];
        char fmtBuf[16] = "%Y-%m-%dT%T"; // default;
        const char *datefmt = adateFmt ? adateFmt : "%Y-%m-%d";
        if ( !hasDate() ) // time only
            strcpy( fmtBuf, "%T" );
        else if ( !hasTime() ) // date only
            strcpy( fmtBuf, datefmt );
        else // date and time
        {
            sprintf( fmtBuf, "%sT%s", datefmt, "%T" );
        }
        return PrintTimestamp(
                buf, 50, time_since_epoch( asUTCIfNoTimeZone ), fmtBuf, nSubsecondDigits, bPrintTimeZone, tzOffsetMinutes, asUTCIfNoTimeZone );
    }

    bool operator==( const DateTime &a ) const
    {
        return year == a.year && month == a.month && mday == a.mday && hour == a.hour && min == a.min && sec == a.sec && nanosec == a.nanosec &&
               tzOffsetMinutes == a.tzOffsetMinutes;
    }
    bool operator!=( const DateTime &a ) const
    {
        return !operator==( a );
    }
    bool operator<( const DateTime &a ) const
    {
        return count() < a.count();
    }
};
inline DateTime mkDate( unsigned y, unsigned m, unsigned d )
{
    return DateTime().dateonly( y, m, d );
}
inline DateTime mkTime( unsigned h, unsigned m, unsigned s, unsigned nanosec = 0, std::optional<int> tzMinutes = {} )
{
    return DateTime().timeonly( h, m, s, nanosec, tzMinutes );
}

inline std::ostream &operator<<( std::ostream &os, const DateTime &dt )
{
    os << dt.to_string();
    return os;
}

std::optional<DateTime> ParseDateTime( std::string_view s, std::ostream *err = nullptr );

} // namespace df
