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
    }

    //    SECTION( "from_tuple" )
    {
        REQUIRE( df1.from_tuples( Tups{Tup{"Jonathon", 24, 23.3, 'A', mkDate( 2010, 10, 22 )}, Tup{"Jeff", 12, 43.5, 'C', mkDate( 2008, 10, 22 )}},
                                  {"Name", "Age", "Score", "Level", "BirthDate"},
                                  &std::cerr ) );

        REQUIRE_EQ( df1.size(), 2u );

        //-- append
        REQUIRE( df.append( df1, &std::cerr ) );
        REQUIRE_EQ( df.size(), 4u );
    }
    SECTION( "PrintDataFrame" )
    {
        std::cout << "---- DataFrame 1 ----\n";
        df1.print( std::cout );
        std::cout << "---- DataFrame 0 and 1 ----\n";
        df.print( std::cout );
    }
    SECTION( "ParseTimestamp" )
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
    SECTION( "PrintTimestamp" )
    { // PrintTimestamp
        char buf[50];
        auto now = std::chrono::system_clock::now();
        std::cout << "Local Time: " << PrintTimestamp( buf, 40, now.time_since_epoch(), "%Y-%m-%d %T", 0, true ) << std::endl;
        std::cout << "UTC Time:   " << PrintTimestamp( buf, 40, now.time_since_epoch(), "%Y-%m-%d %T", 6, true ) << std::endl;
        std::cout << "GMT+8 Time: " << PrintTimestamp( buf, 40, now.time_since_epoch(), "%Y-%m-%dT%T", 3, true, 8 * 60 ) << std::endl;
    }

    SECTION( "HashIndex" )
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
    SECTION( "OrderedIndex" )
    // OrderedIndex
    {
        OrderedIndex oidxName;
        oidxName.create( df, 0 );
        REQUIRE_EQ( *oidxName.findFirst( field( "Jeff" ) ), 0u ); // the first one.
    }
    SECTION( "MultiColOrderedIndex" )
    // MultiColOrderedIndex
    {
        MultiColOrderedIndex idxLevelScore;
        idxLevelScore.create( df, SCols{"Level", "Score"} );

        REQUIRE_EQ( idxLevelScore[0], 2u ); // Jonathon

        MultiColOrderedIndex sortedBirth;
        sortedBirth.create( df, SCols{"BirthDate"} );
        REQUIRE_EQ( sortedBirth[0], 1u );
    }
    SECTION( "MultiColHashIndex" )
    // MultiColHashIndex
    {
        MultiColHashIndex hidxLevelAge;
        REQUIRE( hidxLevelAge.create( df, SCols{"Level", "Age"}, &std::cerr ) );
        std::cout << "--- hidxLevelAge: " << to_string( hidxLevelAge ) << std::endl;

        Record key{field( 'A' ), field( 24 )};
        REQUIRE_EQ( hidxLevelAge[key], 2u ); // Jonathon
    }
    SECTION( "MultiColHashMultiIndex" )
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
    SECTION( "DataFrameView" )
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
    SECTION( "View & Index" )
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
    SECTION( "Condition" )
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
    SECTION( "Logic Expression" )
    // Logic Expression
    {
        std::cout << "---- Logic Expressions ----\n";

        Expr eqExp = !( Col( "Name" ) == "John" );
        std::cout << "EQ:   " << eqExp << std::endl;

        Expr ltExp = !( Col( "Age", "Level" ) < std::make_tuple( 15, 'B' ) );
        std::cout << "LT:   " << ltExp << std::endl;

        Expr isinExpr = !Col( "Age" ).isin( {field( 23 ), field( 24 )} );
        std::cout << "Age ISIN: " << isinExpr << std::endl;

        Expr multiIsinExpr = Col( "Age,", "Level" ).isin( {record( 23, 'A' ), record( 24, 'C' )} );
        std::cout << "Age+Level ISIN: " << multiIsinExpr << std::endl;

        AndExpr andExpr = Col( "Name" ) == "John" && Col( "Age", "Level" ) < mktuple( 15, 'B' );
        REQUIRE_EQ( andExpr.ops.size(), 2u );
        std::cout << "AND     " << andExpr << std::endl;

        OrExpr orExpr = !( Col( "Name" ) == "John" && Col( "Age", "Level" ) < mktuple( 15, 'B' ) ) || Col( "Score" ) < 10;
        REQUIRE_EQ( orExpr.ops.size(), 3u );
        std::cout << "OR     " << orExpr << std::endl;
    }
    SECTION( "HashIndex + isin/eq/notin/ne" )
    // HashIndex + isin/eq/notin/ne
    {
        DataFrameWithIndex dfidx( IDataFramePtr( df.deepCopy() ) ); // dataframe with index
        dfidx.addHashIndex( {"Name"}, "NameHash" );
        std::cout << "--- DataFrameWithIndex ---\n";
        std::cout << dfidx << std::endl;

        auto viewISIN = dfidx.select( Col( "Name" ).isin( record( "John", "Jeff" ) ) ); // ISIN
        REQUIRE_EQ( viewISIN.size(), 2u );
        std::cout << "------- view of  name isin [John, Jeff] -----\n" << viewISIN << std::endl;

        auto viewNOTIN = dfidx.select( Col( "Name" ).notin( record( "John", "Jeff" ) ) ); // NOTIN
        REQUIRE_EQ( viewNOTIN.size(), 2u );
        std::cout << "------- view of  name notin [John, Jeff] -----\n" << viewNOTIN << std::endl;

        auto viewEQ = dfidx.select( {"Name", "Age", "Level"}, Col( "Name" ) == "Tom" ); // EQ
        REQUIRE_EQ( viewEQ.size(), 1u );
        std::cout << "------- view of  name == Tom -----\n" << viewEQ << std::endl;

        auto viewNE = dfidx.select( {"Name", "Age", "Level"}, Col( "Name" ) != "Tom" ); // NE
        REQUIRE_EQ( viewNE.size(), dfidx.size() - 1u );
        viewNE.sort_by( {"Age"} );
        std::cout << "------- view of  name != Tom sorted by Age -----\n" << viewNE << std::endl;
    }
    SECTION( "OrderedIdex + OP" )
    {
        DataFrameWithIndex dfidx( IDataFramePtr( df.deepCopy() ) ); // dataframe with index
        dfidx.addOrderedIndex( {"Level"} );

        auto viewISIN = dfidx.select( Col( "Level" ).isin( record( 'A', 'B' ) ) ); // ISIN
        REQUIRE_EQ( viewISIN.size(), 3u );
        std::cout << "------- view of  Level isin [A, B] -----\n" << viewISIN << std::endl;

        auto viewNOTIN = dfidx.select( Col( "Level" ).notin( record( 'A', 'B' ) ) ); // NOTIN
        REQUIRE_EQ( viewNOTIN.size(), 1u );
        std::cout << "------- view of  Level not [A, B] -----\n" << viewNOTIN << std::endl;

        auto viewEQ = dfidx.select( Col( "Level" ) == 'A' ); // EQ
        REQUIRE_EQ( viewEQ.size(), 2u );
        std::cout << "------- view of  Level == A -----\n" << viewEQ << std::endl;

        auto viewNE = dfidx.select( Col( "Level" ) != 'A' ); // NE
        REQUIRE_EQ( viewNE.size(), 2u );
        std::cout << "------- view of  Level != A -----\n" << viewNE << std::endl;

        auto viewGT = dfidx.select( Col( "Level" ) > 'B' ); // GT
        REQUIRE_EQ( viewGT.size(), 1u );
        std::cout << "------- view of  Level > B -----\n" << viewGT << std::endl;

        auto viewGE = dfidx.select( Col( "Level" ) >= 'B' ); // GE
        REQUIRE_EQ( viewGE.size(), 2u );
        std::cout << "------- view of  Level >= B -----\n" << viewGE << std::endl;

        auto viewLT = dfidx.select( Col( "Level" ) < 'B' ); // LT
        REQUIRE_EQ( viewLT.size(), 2u );
        std::cout << "------- view of  Level < B -----\n" << viewLT << std::endl;

        auto viewLE = dfidx.select( Col( "Level" ) <= 'B' ); // LE
        REQUIRE_EQ( viewLE.size(), 3u );
        std::cout << "------- view of  Level <= B -----\n" << viewLE << std::endl;
    }
    SECTION( "AndExpr+OrExpr" )
    {
        auto a = field( 65 );
        auto b = field( 'A' );
        auto c = a == b;
        ASSERT( c );
        DataFrameWithIndex dfidx( IDataFramePtr( df.deepCopy() ) ); // dataframe with index
        dfidx.addOrderedIndex( {"Level"} );

        auto viewAnd = dfidx.select( Col( "Level" ) >= 'B' && Col( "Age" ) > 12 );
        REQUIRE_EQ( viewAnd.size(), 1u );
        std::cout << "------- view of  Level >= B && Age > 12 -----\n" << viewAnd << std::endl;

        auto viewOr = dfidx.select( Col( "Level" ) >= 'B' || Col( "Score" ) < 45.5 );
        REQUIRE_EQ( viewOr.size(), 4u );
        std::cout << "------- view of  Level >= B || Score < 45.5 -----\n" << viewOr << std::endl;

        //        std::cout << "val:" << std::get<int>( a ) << std::endl;
        std::cout << "--- Score: " << to_string( viewOr.getColumnRefAsType( std::in_place_type<float>, "Score" ) ) << std::endl;
        std::cout << "--- Age: " << to_string( viewOr.getColumnRefAsType( std::in_place_type<int>, "Age" ) ) << std::endl;
        std::cout << "--- Level: " << to_string( viewOr.getColumnRefAsType( std::in_place_type<char>, "Level" ) ) << std::endl;
        //        std::cout << "--- BirthDate: " << to_string( viewOr.getColumnRefAsType( std::in_place_type<Timestamp>, "BirthDate" ) ) <<
        //        std::endl;
    }
}
