#include <unittest.h>
#include <zj/DataFrame.h>
#include <zj/DataFrameIndex.h>
#include <zj/RowDataFrame.h>
#include <zj/DataFrameView.h>
#include <zj/Condition.h>


UNITTEST_MAIN

using namespace zj;
ADD_TEST_CASE( DataFrame_Basic )
{
    using Tup = std::tuple<std::string, int, float, char, Timestamp>;
    using Tups = std::vector<Tup>;
    RowDataFrame df, df1;
    //    ConditionIsIn<false> isIn2;
    { // ParseTimestamp
        auto opDateTime = ParseDateTime( "20201225 12:05:02-4", &std::cerr );
        REQUIRE_EQ( *opDateTime, *ParseDateTime( opDateTime->to_string(), &std::cerr ) );

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

        REQUIRE_EQ( df( 0, 1 ), field( 23 ) );

        std::cout << "---- DataFrame 0 ----\n";
        df.print( std::cout );
    }

    //    SECTION( "from_tuple" )
    {
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
        REQUIRE( hidxName.create( df, 0, &std::cerr ) ); // name
        REQUIRE_EQ( hidxName[field( "Tom" )], 1u );
        REQUIRE_EQ( hidxName[field( "Jeff" )], 3u );

        HashIndex hidxAge;
        REQUIRE( hidxAge.create( df, "Age" ) );

        REQUIRE_EQ( hidxAge[field( 12 )], 3u );

        HashIndex hidxLevel;
        REQUIRE( !hidxLevel.create( df, "Level" ) ); // has duplicte values
    }
    // OrderedIndex
    {
        OrderedIndex oidxName;
        oidxName.create( df, 0 );
        REQUIRE_EQ( *oidxName.findFirst( field( "Jeff" ) ), 0u ); // the first one.
    }
    // MultiColOrderedIndex
    {
        MultiColOrderedIndex idxLevelScore;
        idxLevelScore.create( df, SCols{"Level", "Score"} );

        REQUIRE_EQ( idxLevelScore[0], 2u ); // Jonathon

        MultiColOrderedIndex sortedBirth;
        sortedBirth.create( df, SCols{"BirthDate"} );
        REQUIRE_EQ( sortedBirth[0], 1u );
    }
    // MultiColHashIndex
    {
        MultiColHashIndex hidxLevelAge;
        REQUIRE( hidxLevelAge.create( df, SCols{"Level", "Age"}, &std::cerr ) );
        std::cout << "--- hidxLevelAge: " << to_string( hidxLevelAge ) << std::endl;

        Record key{field( 'A' ), field( 24 )};
        REQUIRE_EQ( hidxLevelAge[key], 2u ); // Jonathon
    }
    // MultiColHashMultiIndex
    {
        MultiColHashMultiIndex hidxLevel;
        hidxLevel.create( df, StrVec{"Level"} );

        REQUIRE_EQ( Set( hidxLevel[record( 'A' )] ), Set( ULongVec{0, 2} ) ); // John, Jonathon

        MultiColHashMultiIndex hidxName;
        hidxName.create( df, StrVec{"Name"} );
        std::cout << "--- hidxName: " << to_string( hidxName ) << std::endl;

        REQUIRE_EQ( Set( hidxName[record( "John" )] ), Set( ULongVec{0} ) ); // John, Jonathon
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
    // View & Index
    {
        OrderedIndex orderedAge;
        orderedAge.create( df, "Age" );
        DataFrameView gv;
        gv.create( df, orderedAge.getRowIndices(), SCols{"Name", "Level", "Age"} );
        REQUIRE_EQ( gv.at( 0, "Name" ), field( "Jeff" ) );
        std::cout << "---- DataFrameView: sorted by age ----\n";
        gv.print( std::cout );
    }
    // Condition
    {
        ConditionCompare nameEQ;
        REQUIRE( nameEQ.init( &df, {"Name"}, OperatorTag::EQ, record( "Jeff" ), &std::cerr ) );
        REQUIRE( !nameEQ.evalAtRow( 0 ) );
        REQUIRE( nameEQ.evalAtRow( 3 ) );

        ConditionCompare ageLevelGE;
        REQUIRE( ageLevelGE.init( &df, {"Level", "Age"}, OperatorTag::GE, record( 'B', 18 ), &std::cerr ) );
        REQUIRE( !ageLevelGE.evalAtRow( 0 ) );
        REQUIRE( ageLevelGE.evalAtRow( 1 ) );
        REQUIRE( !ageLevelGE.evalAtRow( 2 ) );
        REQUIRE( ageLevelGE.evalAtRow( 3 ) );

        ConditionIsIn isInNames;
        REQUIRE( isInNames.init( &df, {"Name"}, {record( "John" ), record( "Jeff" )}, true, &std::cerr ) );
        REQUIRE( isInNames.evalAtRow( 0 ) );
        REQUIRE( !isInNames.evalAtRow( 1 ) );
        REQUIRE( !isInNames.evalAtRow( 2 ) );
        REQUIRE( isInNames.evalAtRow( 3 ) );
    }
    // Logic Expression
    {
        std::cout << "---- Logic Expressions ----\n";

        Expr eqExp = !( Col( "Name" ) == "John" );
        std::cout << "EQ:   " << eqExp << std::endl;

        Expr ltExp = !( Col( "Age", "Level" ) < std::make_tuple( 15, 'B' ) );
        std::cout << "LT:   " << ltExp << std::endl;

        Expr isinExpr = !Col( "Age" ).isin( {field( 23 ), field( 24 )} );
        std::cout << "ISIN:   " << isinExpr << std::endl;

        AndExpr andExpr = Col( "Name" ) == "John" && Col( "Age", "Level" ) < mktuple( 15, 'B' );
        REQUIRE_EQ( andExpr.ops.size(), 2u );
        std::cout << "AND     " << andExpr << std::endl;

        OrExpr orExpr = !( Col( "Name" ) == "John" && Col( "Age", "Level" ) < mktuple( 15, 'B' ) ) || Col( "Score" ) < 10;
        REQUIRE_EQ( orExpr.ops.size(), 3u );
        std::cout << "OR     " << orExpr << std::endl;
    }
    // HashIndex + isin/eq/notin/ne
    {
        DataFrameWithIndex dfidx( IDataFramePtr( df.deepCopy() ) ); // datafram with index
        dfidx.addIndex( IndexType::HashIndex, {"Name"}, "NameHash" );
        std::cout << "--- DataFrameWithIndex ---\n";
        std::cout << dfidx << std::endl;

        auto viewIsin = dfidx.select( Col( "Name" ).isin( record( "John", "Jeff" ) ) ); // ISIN
        REQUIRE_EQ( viewIsin.size(), 2u );
        std::cout << "------- view of  name isin [John, Jeff] -----\n" << viewIsin << std::endl;

        auto viewEq = dfidx.select( {"Name", "Age", "Level"}, Col( "Name" ) == "Tom" ); // EQ
        REQUIRE_EQ( viewEq.size(), 1u );
        std::cout << "------- view of  name == Tom -----\n" << viewEq << std::endl;

        auto viewNe = dfidx.select( {"Name", "Age", "Level"}, Col( "Name" ) != "Tom" ); // NE
        REQUIRE_EQ( viewNe.size(), dfidx.size() - 1u );
        std::cout << "------- view of  name != Tom -----\n" << viewNe << std::endl;
    }
    // OrderedIdex +
    {
    }
}
