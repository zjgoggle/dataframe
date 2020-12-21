#include "DateTime.h"

namespace df
{

char *PrintTimestamp( char *buf,
                      size_t bufsize,
                      const std::chrono::nanoseconds &tp,
                      const char *fmt,
                      unsigned subsecondDigits,
                      int bPrintTimeZoneOffset,
                      std::optional<int> asLocalTimeOffsetMinutes,
                      bool bUseGMTOffsetIfNotSpecified )
{
    static auto defaultFmt = "%Y-%m-%dT%T";
    static constexpr size_t Nano2SecondMultiple = 1000000000;
    size_t timeNanoSeconds = tp.count();
    size_t secondsPart = timeNanoSeconds / Nano2SecondMultiple;
    size_t nanoSecondsPart = timeNanoSeconds % Nano2SecondMultiple;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> timeInSec{std::chrono::seconds( secondsPart )};
    auto timet = std::chrono::system_clock::to_time_t( timeInSec );
    tm atime;
    int timezoneOffsetSec = 0;
    if ( asLocalTimeOffsetMinutes )
        timezoneOffsetSec = *asLocalTimeOffsetMinutes * 60;
    else if ( !bUseGMTOffsetIfNotSpecified )
        timezoneOffsetSec = getLocalGMTOffsetSec( timet );
    timet += timezoneOffsetSec;
    gmtime_r( &timet, &atime );

    auto nBytes = strftime( buf, bufsize, fmt ? fmt : defaultFmt, &atime ); // YYYYMMDD-HH:MM:SS
    if ( nBytes <= 0 )
        return nullptr;
    if ( subsecondDigits && subsecondDigits < 10 ) // .sss
    {
        char fmtbuf[] = ".%02d";
        fmtbuf[3] = '0' + subsecondDigits;
        for ( int i = 0; i < 9 - subsecondDigits; ++i )
            nanoSecondsPart /= 10;
        sprintf( buf + nBytes, fmtbuf, nanoSecondsPart );
        nBytes += subsecondDigits + 1;
    }
    if ( bPrintTimeZoneOffset == 1 || ( bPrintTimeZoneOffset == -1 && asLocalTimeOffsetMinutes ) ) // +xxxx
    {
        int tz = timezoneOffsetSec;
        if ( tz < 0 )
        {
            buf[nBytes++] = '-';
            tz = -tz;
        }
        else
            buf[nBytes++] = '+';
        sprintf( buf + nBytes, "%02d%02d", tz / 3600, ( tz % 3600 ) / 60 );
    }
    return buf;
}

std::optional<DateTime> ParseDateTime( std::string_view s, std::ostream *err )
{
    DateTime t;
    int x;

    const char *ps = s.data(), *pEnd = s.data() + s.length();

    auto skipSpace = [&]( const char *p ) -> const char * {
        while ( p < pEnd && isspace( *p ) )
            ++p;
        return p;
    };

    unsigned val[2];
    char expectedSep;
    unsigned nDigits[2]; // end of month, date
    auto continueParse2Ints = [&]( auto &res ) -> bool {
        for ( int i = 0; i < 2; ++i )
        {
            ps = res.ptr + 1;
            res = std::from_chars( ps, pEnd, val[i] );
            if ( res.ptr == ps )
            {
                if ( err )
                    *err << "Expected " << i + 2 << "-th part of Year-Mon-Date at: " << ps << ".\n";
                return false;
            }
            if ( i == 0 && expectedSep != *res.ptr )
            {
                if ( err )
                    *err << "Expected sep:" << expectedSep << " at:" << ps << ".\n";
                return false;
            }
            nDigits[i] = res.ptr - ps;
        }
        return true;
    };

    ps = skipSpace( ps );
    auto res = std::from_chars( ps, pEnd, x );
    if ( res.ptr == ps )
    {
        if ( err )
            *err << "Failed to first integer at:" << ps << ".\n";
        return {};
    }
    else if ( *res.ptr != ':' )
    {
        expectedSep = *res.ptr;
        if ( res.ptr - ps > 8 )
        {
            if ( err )
                *err << "Too large year at:" << ps << ".\n";
            return {};
        }
        if ( res.ptr - ps != 8 ) // year
        {
            const char *p = ps; // start of year
            auto yearDidigts = res.ptr - ps;
            if ( !continueParse2Ints( res ) )
                return {};
            if ( yearDidigts == 4 ) // Ymd
            {
                t.year = x;
                t.month = val[0];
                t.mday = val[1];
            }
            else if ( nDigits[1] == 4 ) // mdY
            {
                t.year = val[1];
                t.month = x;
                t.mday = val[0];
            }
            else
            {
                if ( err )
                    *err << "Malformed date format at:" << p << ".\n";
                return {};
            }
        }
        else //  YYYYmmdd.
        {
            t.year = x / 10000;
            x %= 10000;
            t.month = x / 100; // 1:12
            t.mday = x % 100; // 1:31
        }
        //----------- check mon date range ------
        if ( t.month > 12 ) // 1..12
        {
            if ( err )
                *err << "Month is too large:" << t.month << ".\n";
            return {};
        }
        if ( t.mday > 31 || t.mday == 0 )
        {
            if ( err )
                *err << "Invalid date at:" << t.mday << ".\n";
            return {};
        }

        if ( *res.ptr == ':' )
        {
            if ( err )
                *err << "\":\" is not allow at end of date.\n";
            return {};
        }
        if ( res.ptr != pEnd && !isspace( *res.ptr ) ) // skip the Date:Time seperator.
            ++res.ptr;
        ps = skipSpace( res.ptr );
        res = std::from_chars( ps, pEnd, x ); // parse first part of time.
        if ( res.ptr == ps ) // only date was parsed.
        {
            if ( ps != pEnd )
            {
                if ( err )
                    *err << "Invalid string after date:" << ps << std::endl;
                return {};
            }
            t.setDateOnly();
            return t;
        }
    }
    else // else no date.
        t.setTimeOnly();

    //------------------------ parse time HH:MM:SS
    if ( *res.ptr == ':' ) // it's possibly time format.
    {
        t.hour = x;
        expectedSep = ':';
        if ( !continueParse2Ints( res ) ) // read H:S
            return {};

        if ( val[0] >= 60 )
        {
            if ( err )
                *err << "Too large minute:" << val[0] << ".\n";
            return {};
        }
        if ( val[1] >= 60 )
        {
            if ( err )
                *err << "Too large second:" << val[0] << ".\n";
            return {};
        }
        t.min = val[0];
        t.sec = val[1];

        if ( *res.ptr == '.' ) //---- parse subsecond;
        {
            ps = res.ptr + 1;
            res = std::from_chars( ps, pEnd, t.nanosec );
            if ( ps == res.ptr )
            {
                if ( err )
                    *err << "Expected subsecond digits at:" << ps;
                return {};
            }
            for ( size_t i = 0, ndigits = res.ptr - ps; i < 9 - ndigits; ++i )
                t.nanosec *= 10;
        }
    }

    //------------------ time zone, must start with '+' or '-', +HHMM, -HH:MM, +H, -HH
    ps = skipSpace( res.ptr );
    if ( ps == pEnd || *ps == 'Z' ) // no time zone.
        return t;

    if ( *ps == '+' || *ps == '-' )
    {
        int sign = *ps == '+' ? 1 : ( -1 ), h = 0, m = 0;
        res = std::from_chars( ++ps, pEnd, x );
        if ( res.ptr - ps == 4 ) // +xxxx
        {
            h = x / 100;
            m = x % 100;
        }
        else if ( res.ptr - ps == 2 || res.ptr - ps == 1 ) // H or HH or HH:MM
        {
            h = x;
            if ( *res.ptr == ':' ) // HH:MM
            {
                ps = res.ptr + 1;
                res = std::from_chars( ps, pEnd, x );
                if ( res.ptr - ps != 2 && res.ptr - ps != 1 )
                {
                    if ( err )
                        *err << "Invalid timezone minute format:" << ps << ".\n";
                    return {};
                }
                m = x;
            }
        }
        else
        {
            if ( err )
                *err << "Too large to empty timezone format:" << ps << ". Valid formats: +HHMM or -HH:MM.\n";
            return {};
        }
        // check range
        if ( h > 12 || h < 0 )
        {
            if ( err )
                *err << "Invalid timezone hour offset: " << h << ".\n";
            return {};
        }
        if ( m > 60 || m < 0 )
        {
            if ( err )
                *err << "Invalid timezone minute offset: " << m << ".\n";
            return {};
        }
        ps = skipSpace( res.ptr );
        if ( ps != pEnd && *ps != 'Z' )
        {
            if ( err )
                *err << "Non-space after timezone:" << res.ptr << ".\n";
            return {};
        }
        t.tzOffsetMinutes = sign * ( h * 60 + m );
    }

    return t;
}

} // namespace df
