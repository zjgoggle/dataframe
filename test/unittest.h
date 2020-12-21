/*
 * This file is part of the ftl (Fast Template Library) distribution (https://github.com/zjgoggle/ftl).
 * Copyright (c) 2018 Jack Zhang.
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

#include <iostream>
#include <chrono>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <cassert>

#include <execinfo.h>
#include <cxxabi.h>
#include <string.h>

/************************************************************************
 *
Usage:
In main cpp file, use macro UNITTEST_MAIN to add main function.

In test case file,

  - use ADD_TEST_CASE(caseName, description) to add dynamic test cases.
  - or use STATIC_TEST_CASE(caseName, description) to add a static test case that will run before main function runs.
    static test case will still be called even UNITTEST_MAIN is not defined.

  - In each test case, users can use: REQUIRE(expr, descr), REQUIRE_THROW, REQUIRE_EQ, REQUIRE_NE, or REQUIRE_OP.
  - And SECTION( namestr, descr) can be used to add multiple sections when each section end, the test case will rerun from start.
  E.g.
  ADD_TEST_CASE( vector_tests, "test vector comparison")
  {
      std::vector<int> A = {1,3, 2}, B = {1,2,4};
      SECTION( "sort and compare" )
      {
         std::sort(A.begin(), A.end());
         std::sort(B.begin(), B.end());
         REQUIRE_OP( <, A, B); // or REQUIRE(A < B)
      }
      SECTION( "compare") // A is still not sorted.
      {
        REQUIRE( A > B );
      }
  }

When run the test main function, users can specify test cases to run or not. like:
    mytestprogram vector_tests string_tests ~float_tests -onerror abort

        ~testcase means not to run this testcase.
*
************************************************************************/


#define T_STRINGIFY( x ) #x
#define T_TOSTRING( x ) T_STRINGIFY( x )

#define ASSERT( expr, ... ) assert( ( expr ) && ( "error:" __VA_ARGS__ ) )

#define ASSERT_OP( a, b, OP, ... )                                                                                                                  \
    do                                                                                                                                              \
    {                                                                                                                                               \
        auto _a = ( a );                                                                                                                            \
        auto _b = ( b );                                                                                                                            \
        if ( !( _a OP _b ) )                                                                                                                        \
        {                                                                                                                                           \
            std::cerr << __FILE__ << ":" << __LINE__ << " ASSERT_OP<" #a #OP #b << ">. But got <" << _a << #OP << _b << "> " __VA_ARGS__            \
                      << std::endl;                                                                                                                 \
            abort();                                                                                                                                \
        }                                                                                                                                           \
    } while ( 0 )

#define ASSERT_EQ( a, b, ... ) ASSERT_OP( a, b, ==, __VA_ARGS__ )
#define ASSERT_NE( a, b, ... ) ASSERT_OP( a, b, !=, __VA_ARGS__ )

///////////////////////////// PASSERT_EQ : PrintStack when assertion fails //////////////////////////////
///
namespace ftl
{

template<class Obj>
bool is_likely_stack_addr( Obj *addr )
{
    int x = 0;
    std::ptrdiff_t stackaddr = reinterpret_cast<std::ptrdiff_t>( &x ), target = reinterpret_cast<std::ptrdiff_t>( addr );
    return target > stackaddr;
}

template<class OStream = std::ostream, size_t max_frames = 63>
void PrintStack( OStream &os = std::cout, int skipFrames = 3 )
{
    void *addrlist[max_frames + 1];
    int addrlen = backtrace( addrlist, sizeof( addrlist ) / sizeof( void * ) );
    if ( addrlen == 0 )
        return;
    char **symbollist = backtrace_symbols( addrlist, addrlen ); // resolve addresses into strings containing "filename(function+address)",
    char funcname[128];
    for ( int i = skipFrames; i < addrlen; i++ )
    {
        char *begin_name = strchr( symbollist[i], '(' ), *begin_offset = begin_name ? strchr( begin_name, '+' ) : nullptr,
             *end_offset = begin_offset ? strchr( begin_offset, ')' ) : nullptr;
        if ( begin_name && begin_offset && end_offset && begin_name < begin_offset )
        {
            *begin_name++ = '\0';
            *begin_offset++ = '\0';
            *end_offset = '\0';
            int status;
            size_t funcnamelen = 128;
            abi::__cxa_demangle( begin_name, funcname, &funcnamelen, &status );
            os << "  " << symbollist[i] << " : " << ( status == 0 ? funcname : begin_name ) << "+" << begin_offset << std::endl;
        }
        else
        {
            os << "  " << symbollist[i] << std::endl; // couldn't parse the line? print the whole line.
        }
    }
    free( symbollist );
}
} // namespace ftl
#define PASSERT_OP( a, b, OP, ... )                                                                                                                 \
    do                                                                                                                                              \
    {                                                                                                                                               \
        auto _a = ( a );                                                                                                                            \
        auto _b = ( b );                                                                                                                            \
        if ( !( _a OP _b ) )                                                                                                                        \
        {                                                                                                                                           \
            std::cerr << __FILE__ << ":" << __LINE__ << " ASSERT_OP<" #a #OP #b << ">. But got <" << _a << #OP << _b << "> " __VA_ARGS__            \
                      << std::endl;                                                                                                                 \
            jz::PrintStack( std::cerr );                                                                                                            \
            abort();                                                                                                                                \
        }                                                                                                                                           \
    } while ( 0 )
#define PASSERT_EQ( a, b, ... ) PASSERT_OP( a, b, ==, __VA_ARGS__ )
#define PASSERT_NE( a, b, ... ) PASSERT_OP( a, b, !=, __VA_ARGS__ )

//////////////////////////////////////////////////////////////////

#define T_EXIT_OR_THROW( ERRORSTR, EXCEPTIONTYPE )                                                                                                  \
    if ( DoesAbortOnError() )                                                                                                                       \
    {                                                                                                                                               \
        GetOStream() << ERRORSTR << std::endl << std::flush;                                                                                        \
        abort();                                                                                                                                    \
    }                                                                                                                                               \
    else                                                                                                                                            \
        throw EXCEPTIONTYPE( ERRORSTR.c_str() );

////////////////// unittest macros /////////////////////////////

#define REQUIRE_OP( OP, X, Y, ... )                                                                                                                 \
    do                                                                                                                                              \
    {                                                                                                                                               \
        ++m_requires;                                                                                                                               \
        auto rx = ( X );                                                                                                                            \
        auto ry = ( Y );                                                                                                                            \
        if ( !( rx OP ry ) )                                                                                                                        \
        {                                                                                                                                           \
            std::stringstream ss;                                                                                                                   \
            ss << "EQEvaluationError in " __FILE__ ":" T_TOSTRING( __LINE__ ) ". Expected:\"" #X " " #OP " " #Y << "\", got " << rx << " " #OP " "  \
               << ry << ". Desc:\"" __VA_ARGS__ << "\"";                                                                                            \
            T_EXIT_OR_THROW( ss.str(), UnitTestRequireException );                                                                                  \
        }                                                                                                                                           \
    } while ( false )

#define REQUIRE_EQ( X, Y, ... ) REQUIRE_OP( ==, X, Y, __VA_ARGS__ )
#define REQUIRE_NE( X, Y, ... ) REQUIRE_OP( !=, X, Y, __VA_ARGS__ )

#define REQUIRE( expr, ... )                                                                                                                        \
    do                                                                                                                                              \
    {                                                                                                                                               \
        ++m_requires;                                                                                                                               \
        if ( !( expr ) )                                                                                                                            \
        {                                                                                                                                           \
            std::stringstream ss;                                                                                                                   \
            ss << "EvaluationError in " __FILE__ ":" T_TOSTRING( __LINE__ ) ". Expr:\"" #expr << "\", Desc:\"" __VA_ARGS__ << "\"";                 \
            T_EXIT_OR_THROW( ss.str(), UnitTestRequireException );                                                                                  \
        }                                                                                                                                           \
    } while ( false )

#define REQUIRE_THROW( expr, exception, ... )                                                                                                       \
    do                                                                                                                                              \
    {                                                                                                                                               \
        ++m_requires;                                                                                                                               \
        try                                                                                                                                         \
        {                                                                                                                                           \
            expr;                                                                                                                                   \
        }                                                                                                                                           \
        catch ( const exception &ex )                                                                                                               \
        {                                                                                                                                           \
            break;                                                                                                                                  \
        }                                                                                                                                           \
        std::stringstream ss;                                                                                                                       \
        ss << "NoThrowError in " __FILE__ ":" T_TOSTRING( __LINE__ ) ". Expr:\"" #expr << "\", Desc:\"" __VA_ARGS__ << "\"";                        \
        T_EXIT_OR_THROW( ss.str(), UnitTestRequireException );                                                                                      \
    } while ( false )

#define SECTION( name, ... ) for ( const auto _ok_ = StartSection( name ); _ok_; throw UnitTestRerunException( name ) )

#define ADD_TEST_CASE( funcName, ... )                                                                                                              \
    static struct funcName##_TestCase : public UnitTestFixtureBase                                                                                  \
    {                                                                                                                                               \
        void RunTestImpl() override;                                                                                                                \
        funcName##_TestCase() : UnitTestFixtureBase( #funcName )                                                                                    \
        {                                                                                                                                           \
        }                                                                                                                                           \
    } s_##funcName##_TestCase__;                                                                                                                    \
    void funcName##_TestCase::RunTestImpl()

#define ADD_TEST_CASE_F( funcName, Fixture, ... )                                                                                                   \
    static struct funcName##_TestCase : public Fixture                                                                                              \
    {                                                                                                                                               \
        void RunTestImpl() override;                                                                                                                \
        funcName##_TestCase() : Fixture()                                                                                                           \
        {                                                                                                                                           \
            InitTestFixture( #funcName );                                                                                                           \
        }                                                                                                                                           \
    } s_##funcName##_TestCase__;                                                                                                                    \
    void funcName##_TestCase::RunTestImpl()


#define STATIC_TEST_CASE( funcName, ... )                                                                                                           \
    static struct funcName##_TestCase : public UnitTestFixtureBase                                                                                  \
    {                                                                                                                                               \
        void RunTestImpl() override;                                                                                                                \
        funcName##_TestCase()                                                                                                                       \
        {                                                                                                                                           \
            m_name = #funcName;                                                                                                                     \
            RunTest();                                                                                                                              \
        }                                                                                                                                           \
        bool DoesAbortOnError() const                                                                                                               \
        {                                                                                                                                           \
            return true;                                                                                                                            \
        }                                                                                                                                           \
    } s_##funcName##_TestCase__;                                                                                                                    \
    void funcName##_TestCase::RunTestImpl()

////////////////// unittest classes /////////////////////////////

struct UnitTestRequireException : public std::runtime_error
{
    UnitTestRequireException( const char *err ) : std::runtime_error( err )
    {
    }
};

struct UnitTestRerunException : public std::runtime_error
{
    UnitTestRerunException( const char *err = "" ) : std::runtime_error( err )
    {
    }
};

struct UnitTestFixtureBase;
struct UnitTestRegistration
{
    std::unordered_map<std::string, UnitTestFixtureBase *> tests;
    std::vector<std::string> testNames;
    std::string currentTest;
    int errCount = 0;
    std::ostream *os = &std::cout;
    bool abortOnError = false;
};

template<class UnitTestRegistrationT = UnitTestRegistration>
UnitTestRegistrationT &t_get_unittest_registration()
{
    static UnitTestRegistrationT s_reg;
    return s_reg;
}
struct UnitTestFixtureBase
{
    virtual void RunTestImpl()
    {
    }
    virtual ~UnitTestFixtureBase() = default;

    UnitTestFixtureBase() = default;
    UnitTestFixtureBase( const std::string &name )
    {
        InitTestFixture( name );
    }
    void InitTestFixture( const std::string &name )
    {
        if ( t_get_unittest_registration().tests.count( name ) )
        {
            throw UnitTestRequireException( ( name + " Duplicate name" ).c_str() );
        }
        t_get_unittest_registration().tests[name] = this;
        t_get_unittest_registration().testNames.push_back( name );
    }
    bool StartSection( const std::string &section )
    {
        if ( m_executedSections.insert( section ).second )
        {
            m_currSection = section;
            return true;
        }
        return false;
    }
    std::string GetCurrentSection() const
    {
        return m_currSection;
    }
    void RunTest()
    {
        auto &os = GetOStream();
        os << "++ Start test " << m_name << std::endl;
        t_get_unittest_registration().currentTest = m_name;
        m_requires = 0;
        auto timeStart = std::chrono::steady_clock::now();
        for ( bool rerun = true; rerun; )
        {
            try
            {
                m_currSection.clear();
                this->RunTestImpl();
                rerun = false;
            }
            catch ( const UnitTestRerunException &e )
            {
                os << "  Section completed: " << m_name << "/" << e.what() << "\n";
            }
            catch ( const UnitTestRequireException &e )
            {
                auto timeDiff = std::chrono::steady_clock::now() - timeStart;
                os << "Found test error:" << e.what() << std::endl;
                os << "** Test error " << m_name << ", time elapsed (ns): " << timeDiff.count() << std::endl << std::endl;
                std::string s( m_name + ": " );
                s += e.what();
                throw UnitTestRequireException( s.c_str() );
            }
        }
        auto timeDiff = std::chrono::steady_clock::now() - timeStart;
        os << "-- Test ended " << m_name << ", number of requires: " << m_requires << ", time elapsed (ns): " << timeDiff.count() << std::endl
           << std::endl;
    }

    const std::string &GetName() const
    {
        return m_name;
    }
    std::ostream &GetOStream()
    {
        return *t_get_unittest_registration().os;
    }

    bool DoesAbortOnError() const
    {
        return t_get_unittest_registration().abortOnError;
    }
    std::unordered_set<std::string> m_executedSections;
    std::string m_name;
    std::string m_currSection;
    int m_requires = 0;
};

////////////////// unittests_main /////////////////////////////

#define UNITTEST_MAIN                                                                                                                               \
    int main( int argc, const char *argv[] )                                                                                                        \
    {                                                                                                                                               \
        return unittest_main( argc, argv );                                                                                                         \
    }

// unit tests main function
template<class TestCase = UnitTestFixtureBase>
int unittest_main( int argn, const char *argv[] )
{
    auto usage = []( const std::string err = "" ) -> int {
        if ( !err.empty() )
        {
            std::cout << err << std::endl;
        }
        std::cout << "\n Usage: program [-l||--list] [-h|--help] [-onerror abort|return|continue] [testnames...] [~excludedtests...] \n"
                     "    ~excludedtests start with ~.\n"
                     "    -onerror abort: abort program; return, skip rest test cases; default continue to run other testcases.\n\n";
        return !err.empty();
    };
    auto printTestNames = [] {
        const auto &names = t_get_unittest_registration().testNames;
        for ( const auto &name : names )
        {
            std::cout << name << std::endl;
        }
        std::cout << " Totally " << names.size() << " tests." << std::endl;
    };
    bool a_exitOnError = false;
    auto &regs = t_get_unittest_registration();

    std::vector<std::string> whitelist;
    std::unordered_set<std::string> blacklist; // excluded test names start with '~'
    for ( int i = 1; i < argn; ++i )
    {
        std::string a = argv[i];
        if ( a == "-l" || a == "--list" )
        {
            printTestNames();
            return 0;
        }
        if ( a == "-h" || a == "--help" )
            return usage();

        if ( a == "-onerror" )
        {
            if ( i == argn - 1 )
                return usage( "No argument for -onerror" );
            a = argv[++i];
            if ( a == "abort" )
                t_get_unittest_registration().abortOnError = true;
            else if ( a == "return" )
                a_exitOnError = true;
            else if ( a == "continue" )
                a_exitOnError = false;
            else
                return usage( "Wrong -onerror argument: " + a );
        }
        else if ( argv[i][0] == '~' )
            blacklist.insert( &argv[i][1] );
        else
        {
            if ( !regs.tests.count( a ) )
                return usage( "No test case found: " + a );
            whitelist.push_back( argv[i] );
        }
    }

    const auto &testNames = whitelist.empty() ? regs.testNames : whitelist;
    std::string errors;
    const auto start = std::chrono::steady_clock::now();
    for ( const auto &name : testNames )
    {
        try
        {
            if ( !blacklist.count( name ) )
                regs.tests[name]->RunTest();
        }
        catch ( const UnitTestRequireException &e )
        {
            errors += e.what();
            errors += "\n";
            ++regs.errCount;
            if ( a_exitOnError )
                break;
        }
    }
    const auto stop = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>( stop - start );
    if ( regs.errCount == 0 )
    {
        *regs.os << "Succedded running all " << testNames.size() << " test cases. Tests took " << duration.count() << " us." << std::endl;
    }
    else
    {
        *regs.os << "All tests errors:\n" << errors << std::endl;
        *regs.os << regs.errCount << " errors in " << testNames.size() << " test cases. Tests took " << duration.count() << " us." << std::endl;
    }
    return regs.errCount;
}
