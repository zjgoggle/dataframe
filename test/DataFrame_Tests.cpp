#include <unittest.h>
#include <zj/DataFrame.h>
#include <zj/DataFrameIndex.h>
#include <zj/RowDataFrame.h>
#include <zj/DataFrameView.h>


UNITTEST_MAIN

using namespace zj;
ADD_TEST_CASE( DataFrame_Basic )
{
    RowDataFrame df, df1;

    { // ParseTimestamp
        //        auto opDateTime = ParseDateTime( "20201225 12:05:02-4", &std::cerr );
        //        REQUIRE_EQ( *opDateTime, *ParseDateTime( opDateTime->to_string(), &std::cerr ) );

        REQUIRE_EQ( ParseDateTime( "2000/10/22", &std::cerr )->to_string(), "2000-10-22" );
        REQUIRE_EQ( ParseDateTime( "20:08:10", &std::cerr )->to_string(), "20:08:10" );
        REQUIRE_EQ( ParseDateTime( "20:08:10.12", &std::cerr )->to_string( nullptr, 2 ), "20:08:10.12" );
        REQUIRE_EQ( ParseDateTime( "20:08:10.12 -3:30", &std::cerr )->to_string( nullptr, 2 ), "20:08:10.12-0330" );
        REQUIRE_EQ( ParseDateTime( "20201225 12:05:02-4", &std::cerr )->to_string(), "2020-12-25T12:05:02-0400" );
        REQUIRE_EQ( ParseDateTime( "12/25/2020T12:05:02.123 +4:30", &std::cerr )->to_string( nullptr, 3 ), "2020-12-25T12:05:02.123+0430" );
        REQUIRE_EQ( ParseDateTime( "12/25/2020T12:05:02.123", &std::cerr )->to_string(), "2020-12-25T12:05:02" );
        REQUIRE_EQ( ParseDateTime( "12/25/2020T13:05:02.123", &std::cerr )->to_string( nullptr, 0, -1, true ), "2020-12-25T13:05:02" ); // asUTC
    }
    { // PrintTimestamp
        char buf[50];
        auto now = std::chrono::system_clock::now();
        std::cout << "Local Time: " << PrintTimestamp( buf, 40, now.time_since_epoch(), "%Y-%m-%d %T", 0, true ) << std::endl;
        std::cout << "UTC Time:   " << PrintTimestamp( buf, 40, now.time_since_epoch(), "%Y-%m-%d %T", 6, true ) << std::endl;
        std::cout << "GMT+8 Time: " << PrintTimestamp( buf, 40, now.time_since_epoch(), "%Y-%m-%dT%T", 3, true, 8 * 60 ) << std::endl;
    }
    //    SECTION( "from_records" )
    {
        std::vector<ColumnDef> colDefs = {
                StrCol( "Name" ), Int32Col( "Age" ), {FieldTypeTag::Char, "Level"}, {FieldTypeTag::Float32, "Score"}, TimestampCol( "BirthDate" )};
        std::vector<StrVec> records{{"John", "23", "A", "29.3", "2000/10/22"}, {"Tom", "18", "B", "45.2", "N/A"}};

        REQUIRE( df.from_records( records, colDefs, &std::cerr ) );

        auto shape = df.shape();
        REQUIRE_EQ( shape[0], 2u ); // rows
        REQUIRE_EQ( shape[1], 5u ); // cols

        REQUIRE_EQ( df( 0, 1 ), fieldval( 23 ) );

        std::cout << "---- DataFrame 0 ----\n";
        df.print( std::cout );
    }

    //    SECTION( "from_tuple" )
    {
        using Tup = std::tuple<std::string, int, float, char, Timestamp>;
        using Tups = std::vector<Tup>;
        REQUIRE( df1.from_tuples( Tups{Tup{"Jonathon", 24, 23.3, 'A', mkDate( 2010, 10, 22 )}, Tup{"Jeff", 12, 43.5, 'C', mkDate( 2008, 10, 22 )}},
                                  {"Name", "Age", "Score", "Level", "BirthDate"},
                                  &std::cerr ) );

        REQUIRE_EQ( df1.size(), 2u );
        std::cout << "---- DataFrame 1 ----\n";
        df1.print( std::cout );

        //-- append
        REQUIRE( df.append( df1, &std::cerr ) );
        REQUIRE_EQ( df.size(), 4u );

        std::cout << "---- DataFrame 0 and 1 ----\n";
        df.print( std::cout );
    }

    // HashIndex
    {
        HashIndex hidxName;
        REQUIRE( hidxName.create( df, 0 ) ); // name
        REQUIRE_EQ( hidxName[fieldval( "Tom" )], 1u );
        REQUIRE_EQ( hidxName[fieldval( "Jeff" )], 3u );

        HashIndex hidxAge;
        REQUIRE( hidxAge.create( df, "Age" ) );

        REQUIRE_EQ( hidxAge[fieldval( 12 )], 3u );
        REQUIRE_EQ( hidxAge[fieldval( 12 )], 3u );

        HashIndex hidxLevel;
        REQUIRE( !hidxLevel.create( df, "Level" ) ); // has duplicte values
    }
    // OrderedIndex
    {
        OrderedIndex oidxName;
        oidxName.create( df, 0 );
        REQUIRE_EQ( *oidxName.findFirst( fieldval( "Jeff" ) ), 0u ); // the first one.
    }
    // MultiColOrderedIndex
    {
        MultiColOrderedIndex idxLevelScore;
        idxLevelScore.create( df, SCols{"Level", "Score"} );

        REQUIRE_EQ( idxLevelScore[0], 2u ); // Jonathon

        MultiColOrderedIndex sortedBirth;
        REQUIRE( sortedBirth.create( df, SCols{"BirthDate"} ) );
        REQUIRE_EQ( sortedBirth[0], 1u );
    }
    // MultiColHashIndex
    {
        MultiColHashIndex hidxLevelAge;
        REQUIRE( hidxLevelAge.create( df, SCols{"Level", "Age"}, &std::cerr ) );

        Record key{fieldval( 'A' ), fieldval( 24 )};
        REQUIRE_EQ( hidxLevelAge[key], 2u ); // Jonathon
    }
    // MultiColHashMultiIndex
    {
        MultiColHashMultiIndex hidxLevel;
        hidxLevel.create( df, StrVec{"Level"} );

        Record key{fieldval( 'A' )};
        REQUIRE_EQ( Set( hidxLevel[key] ), Set( ULongVec{0, 2} ) ); // John, Jonathon
    }
    // DataFrameView
    {
        DataFrameView cv;
        REQUIRE( cv.create_column_view( df, SCols{"Name", "Level"}, &std::cerr ) );
        REQUIRE_EQ( cv.countRows(), df.countRows() );
        REQUIRE_EQ( cv.countCols(), 2u );
        std::cout << "---- Column View: Name, Level ----\n";
        cv.print( std::cout );

        DataFrameView rv;
        rv.create_row_view( cv, IRows{1, 2, 3}, &std::cerr );
        REQUIRE_EQ( rv.countRows(), 3u );
        REQUIRE_EQ( rv.countCols(), cv.countCols() );
        std::cout << "---- Row View: Name, Level [1..3] ----\n";
        rv.print( std::cout );

        DataFrameView gv;
        REQUIRE( gv.create( df, IRows{1, 2, 3}, SCols{"Name", "Level"}, &std::cerr ) );
        std::cout << "---- DataFrameView: Name, Level [1..3] ----\n";
        gv.print( std::cout );
    }
}
