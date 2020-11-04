import ballerina/io;
import ballerina/java.jdbc;
import ballerina/sql;
// import ballerina/time;

public type Queries record {
    string createQuery;
    string insertQuery;
    string tableName;
};
public type CreateQueries record {
    string createQuery;
    // string tableName;
};
public type InsertQueries record {
    string insertQuery;
    // string tableName;
};

function setUp(jdbc:Client jdbcClient) returns sql:Error|sql:ExecutionResult{

    sql:ExecutionResult|sql:Error result; 

    result = jdbcClient->execute("DROP TYPE IF EXISTS enumValues CASCADE;");

    if(result is sql:Error){
        io:println("Error occurred while creating the enum type..");
        io:println(result);
    }
    
    result = jdbcClient->execute("CREATE TYPE enumValues AS ENUM ('value1','value2','value3')");

    if(result is sql:Error){
        io:println("Error occurred while creating the enum type..");
        io:println(result);
    }

    
    result = jdbcClient->execute("DROP TYPE IF EXISTS complex CASCADE;");

    if(result is sql:Error){
        io:println("Error occurred while creating the complex type..");
        io:println(result);
    }
    
    result = jdbcClient->execute("CREATE TYPE complex AS (r double precision,i double precision)");

    if(result is sql:Error){
        io:println("Error occurred while creating the complex type..");
        io:println(result);
    }

    result = jdbcClient->execute("DROP TYPE IF EXISTS inventory_item CASCADE;");

    if(result is sql:Error){
        io:println("Error occurred while creating the inventory_item type..");
        io:println(result);
    }
    
    result = jdbcClient->execute("CREATE TYPE inventory_item AS (name text,supplier_id integer,price numeric)");

    if(result is sql:Error){
        io:println("Error occurred while creating the inventory_item type..");
        io:println(result);
    }


    result = jdbcClient->execute("DROP TYPE IF EXISTS floatrange CASCADE;");

    if(result is sql:Error){
        io:println("Error occurred while creating the floatrange type..");
        io:println(result);
    }
    
    result = jdbcClient->execute("CREATE TYPE floatrange AS RANGE (subtype = float8,subtype_diff = float8mi )");

    if(result is sql:Error){
        io:println("Error occurred while creating the floatrange type..");
        io:println(result);
    }


    result = jdbcClient->execute("DROP Domain IF EXISTS posint CASCADE;");

    if(result is sql:Error){
        io:println("Error occurred while creating the posint domain..");
        io:println(result);
    }
    
    result = jdbcClient->execute("CREATE DOMAIN posint AS integer CHECK (VALUE > 0);");

    if(result is sql:Error){
        io:println("Error occurred while creating the posint domain..");
        io:println(result);
    }


    return result;

}

function insertQueryMaker(map<string> args) returns InsertQueries{

    string insertQuery = "";
    string columns = "";
    string values = "";

    insertQuery += "(";

    foreach var [column,value] in args.entries() {
       columns += column +",";
       values += "'"+value+"'" + ",";
    }
    int columnLength = columns.length();
    int valueLength = values.length();
    columns = columns.substring(0,columnLength - 1);
    values = values.substring(0,valueLength - 1);

    insertQuery += columns;
    insertQuery += ")";
    insertQuery += " Values ";
    insertQuery += "(";
    insertQuery += values;
    insertQuery += ")";

    InsertQueries result = {insertQuery};

    return result;


} 


function tableCreations(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    int|string|sql:Error? result;

    // result = createNumericTable(jdbcClient);
    // result = createMoneyTable(jdbcClient);
    // result = createCharacterTable(jdbcClient);
    // result = createBinaryTable(jdbcClient);
    // result = createDateTimeTable(jdbcClient);
    // result = createBooleanTable(jdbcClient);
    // result = createEnumTable(jdbcClient);
    // result = createGeometricTable(jdbcClient);
    // result = createNetworkTable(jdbcClient);
    // result = createBitTable(jdbcClient);

    // result = createTextSearchTable(jdbcClient);
    // result = createUuidTable(jdbcClient);
    // result = createXmlTable(jdbcClient);
    // result = createJsonTable(jdbcClient);
    result = createArrayTable(jdbcClient);
    // result = createCompositeTable(jdbcClient);
    // result = createRangeTable(jdbcClient);
    // result = createDomainTable(jdbcClient);
    // result = createObjectIdentifierTable(jdbcClient);
    // result = createPglsnTable(jdbcClient);
    // result = createPseudoTypeTable(jdbcClient);
    
     return result;   


}

function tableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;


    // result = numericTableInsertions(jdbcClient);
    // result = moneyTableInsertions(jdbcClient);
    // result = characterTableInsertions(jdbcClient);
    // result = binaryTableInsertions(jdbcClient);
    // result = DateTimeTableInsertions(jdbcClient);
    // result = booleanTimeTableInsertions(jdbcClient);
    // result = enumTableInsertions(jdbcClient);
    // result = geometricTableInsertions(jdbcClient);
    // result = networkTableInsertions(jdbcClient);
    // result = BitTableInsertions(jdbcClient);

    // result = textSearchTableInsertions(jdbcClient);
    // result = UUIDTableInsertions(jdbcClient);
    // result = xmlTableInsertions(jdbcClient);
    // result = JsonTableInsertions(jdbcClient);
    result = arrayTableInsertions(jdbcClient);
    // result = ComplexTableInsertions(jdbcClient);
    // result = RangeTableInsertions(jdbcClient);
    // result = domainTableInsertions(jdbcClient);
    // result = objectIdentifierTableInsertions(jdbcClient);
    // result = pslgnTableInsertions(jdbcClient);


    return result;
}



public function main() {
    jdbc:Client|sql:Error jdbcClient =  new ("jdbc:postgresql://localhost:5432/datatypes","postgres","postgres");
           
    if (jdbcClient is jdbc:Client) {

        sql:Error|sql:ExecutionResult result = setUp(jdbcClient);

        int|string|sql:Error? err = tableCreations(jdbcClient);

        sql:ExecutionResult| sql:Error? insertResult = tableInsertions(jdbcClient);

        sql:Error? selectionResult = tableSelections(jdbcClient);
        
        sql:Error? e = jdbcClient.close();  

        if(e is sql:Error){
            io:println("Conection close failed!!");
        } 
    } 
    else {
        io:println("Initialization failed!!");
        io:println(jdbcClient);
    }
}

//===============================================================================================================================================================
//===============================================================================================================================================================


function tableSelections(jdbc:Client jdbcClient) returns sql:Error?{

        sql:Error? result;

        // result = numericTableSelection(jdbcClient);
        // result = moneyTableSelection(jdbcClient);
        // result = characterTableSelection(jdbcClient);
        // result = binaryTableSelection(jdbcClient);
        // result = dateTimeTableSelection(jdbcClient);
        // result = BooleanTableSelection(jdbcClient);
        // result = enumTableSelection(jdbcClient);
        // result = geometricTableSelection(jdbcClient);
        // result = networkTableSelection(jdbcClient);
        // result = bitTableSelection(jdbcClient);

        // result = textsearchTableSelection(jdbcClient);
        // result = uuidTableSelection(jdbcClient);
        // result = xmlTableSelection(jdbcClient);
        // result = jsonTableSelection(jdbcClient);
        result = arrayTableSelection(jdbcClient);
        // result = moneyTableSelection(jdbcClient);
        // result = moneyTableSelection(jdbcClient);
        // result = moneyTableSelection(jdbcClient);
        // result = moneyTableSelection(jdbcClient);

        return result;
        
}


function selecionQueryMaker(string tableName , string columns = "*",string condition = "True") returns string{

    return "Select " + columns + " from "+ tableName + " where " + condition;

}


//.........................................................................................................




//.........................................................................................................




public type UuidRecord record{
    
    int ID;
    byte[] uuidType;
};


function uuidTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
// "uuidType":"uuid"
     io:println("------ Start Query in Uuid table-------");

    string selectionQuery = selecionQueryMaker("uuidTypes",columns,condition);


        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, UuidRecord);


    stream<UuidRecord, sql:Error> customerStream =
        <stream<UuidRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(UuidRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.uuidType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Uuid table-------");


}





//.........................................................................................................

public type ArrayRecord record{
    
    int ID;
    string textArrayType;
    string[] textArray2Type;
    int[] integerArrayType;
    string integerArray2Type;
    int[5] arrayType;
    int[] array2Type;
};


function arrayTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
            // "textArrayType":"text[][]",
            // "textArray2Type":"text[]",
            // "integerArrayType":"int[]",
            // "integerArray2Type":"int[][]",
            // "arrayType":"int array[5]",
            // "array2Type":"int array"
     io:println("------ Start Query in Array table-------");

    string selectionQuery = selecionQueryMaker("arrayTypes",columns,condition);

    selectionQuery = "select textArrayType::text, textArray2Type, integerArrayType, integerArray2Type::text,arrayType,array2Type from arrayTypes";


        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, ArrayRecord);


    stream<ArrayRecord, sql:Error> customerStream =
        <stream<ArrayRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(ArrayRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.textArrayType);
        io:println(rec.textArray2Type);
        io:println(rec.integerArrayType);
        io:println(rec.integerArray2Type);
        io:println(rec.arrayType);
        io:println(rec.array2Type);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Array table-------");


}




//.........................................................................................................




public type JsonRecord record{
    
    int ID;
    json jsonType;
    json jsonbType;
    string jsonpathType;
};


function jsonTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
//             "jsonType":"json",
//             "jsonbType":"jsonb",
//             "jsonpathType":"jsonpath"
     io:println("------ Start Query in Json table-------");

    string selectionQuery = selecionQueryMaker("jsonTypes",columns,condition);


        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, JsonRecord);


    stream<JsonRecord, sql:Error> customerStream =
        <stream<JsonRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(JsonRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.jsonType);
        io:println(rec.jsonbType);
        io:println(rec.jsonpathType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Json table-------");


}




//.........................................................................................................




public type XmlRecord record{
    
    int ID;
    string xmlType;
};


function xmlTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
// "xmlType":"xml"
     io:println("------ Start Query in Xml table-------");

    string selectionQuery = selecionQueryMaker("xmlTypes",columns,condition);

    selectionQuery = "select ID,xmlType::text from xmlTypes";

        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, XmlRecord);


    stream<XmlRecord, sql:Error> customerStream =
        <stream<XmlRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(XmlRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.xmlType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Xml table-------");


}




//.........................................................................................................




public type UuidRecord record{
    
    int ID;
    byte[] uuidType;
};


function uuidTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
// "uuidType":"uuid"
     io:println("------ Start Query in Uuid table-------");

    string selectionQuery = selecionQueryMaker("uuidTypes",columns,condition);


        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, UuidRecord);


    stream<UuidRecord, sql:Error> customerStream =
        <stream<UuidRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(UuidRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.uuidType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Uuid table-------");


}





//........................................................................................................




public type TextSearchRecord record{
    
    int ID;
    string tsvectorType;
    byte[] tsqueryType;
};


function textsearchTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
//             "tsvectorType":"tsvector",
//             "tsqueryType":"tsquery"
     io:println("------ Start Query in TextSearch table-------");

    string selectionQuery = selecionQueryMaker("textsearchTypes",columns,condition);


        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, TextSearchRecord);


    stream<TextSearchRecord, sql:Error> customerStream =
        <stream<TextSearchRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(TextSearchRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.tsvectorType);
        io:println(rec.tsqueryType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in TextSearch table-------");


}









//........................................................................................................




public type BitRecord record{
    
    int ID;
    string bitType;
    decimal bitVaryType;
    float bitVaryType2;
    int bitOnlyType;
};


function bitTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
//             "bitType":"bit(3)",
//             "bitVaryType":"BIT VARYING(5)",
//             "bitVaryType2":"BIT VARYING(7)",
//             "bitOnlyType":"bit"
     io:println("------ Start Query in Bit table-------");

    // string selectionQuery = selecionQueryMaker("bitTypes",columns,condition);

    string selectionQuery = "select ID,bitType::int,bitVaryType,bitVaryType2,bitOnlyType from bitTypes"; 

        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, BitRecord);


    stream<BitRecord, sql:Error> customerStream =
        <stream<BitRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(BitRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.bitType);
        io:println(rec.bitVaryType);
        io:println(rec.bitVaryType2);
        io:println(rec.bitOnlyType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Bit table-------");


}






//.......................................................................................................


public type NetworkRecord record{
    
    int ID;
    string inetType;
    string cidrType;
    string macaddrType;
    string macaddr8Type;
};


function networkTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
//             "inetType":"inet",
//             "cidrType":"cidr",
//             "macaddrType":"macaddr",
//             "macaddr8Type":"macaddr8"
     io:println("------ Start Query in Network table-------");

    string selectionQuery = selecionQueryMaker("networkTypes",columns,condition);

        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, NetworkRecord);


    stream<NetworkRecord, sql:Error> customerStream =
        <stream<NetworkRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(NetworkRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.inetType);
        io:println(rec.cidrType);
        io:println(rec.macaddrType);
        io:println(rec.macaddr8Type);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Network table-------");


}

//.......................................................................................................




public type GeometricRecord record{
    
    int ID;
    string pointType;
    string lineType;
    string lsegType;
    string boxType;
    string pathType;
    string polygonType;
    string circleType;

};


function geometricTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
// "pointType":"point",
//             "lineType":"line",
//             "lsegType":"lseg",
//             "boxType":"box",
//             "pathType":"path",
//             "polygonType":"polygon",
//             "circleType":"circle"
     io:println("------ Start Query in Geometric table-------");

    string selectionQuery = selecionQueryMaker("geometricTypes",columns,condition);

        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, GeometricRecord);


    stream<GeometricRecord, sql:Error> customerStream =
        <stream<GeometricRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(GeometricRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.pointType);
        io:println(rec.lineType);
        io:println(rec.lsegType);
        io:println(rec.boxType);
        io:println(rec.pathType);
        io:println(rec.polygonType);
        io:println(rec.circleType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Geometric table-------");


}







//........................................................................................................



public type EnumRecord record{
    
    int ID;
    string enumType;

};


function enumTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
// "enumType":"enumValues"
     io:println("------ Start Query in Enum table-------");

    string selectionQuery = selecionQueryMaker("enumTypes",columns,condition);

        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, EnumRecord);


    stream<EnumRecord, sql:Error> customerStream =
        <stream<EnumRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(EnumRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.enumType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Enum table-------");


}



//........................................................................................................



public type BooleanRecord record{
    
    int ID;
    boolean booleanType;

};


function BooleanTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
    //         "booleanType":"boolean"
     io:println("------ Start Query in Boolean table-------");

    string selectionQuery = selecionQueryMaker("booleanTypes",columns,condition);

        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, BooleanRecord);


    stream<BooleanRecord, sql:Error> customerStream =
        <stream<BooleanRecord, sql:Error>>resultStream;
    
    error? e = customerStream.forEach(function(BooleanRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.booleanType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Boolean table-------");


}



//.........................................................................................................


public type DateTimeRecord record{
    
    int ID;
    int timestampType;
    int dateType;
    int timeType;
    string intervalType;

};
function dateTimeTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
    //         "timestampType":"timestamp",
    //         "dateType":"date",
    //         "timeType":"time",
    //         "intervalType":"interval"
     io:println("------ Start Query in DateTime table-------");

    string selectionQuery = selecionQueryMaker("dateTimeTypes",columns,condition);

    // selectionQuery = "Select ID,timestampType,dateType,timeType, intervalType::int from dateTimeTypes where True";
        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, DateTimeRecord);

    // io:println(resultStream);
    // if(resultStream is sql:Error){
    //     io:println("resultStream");
    //     io:println(resultStream);
    // }

    stream<DateTimeRecord, sql:Error> customerStream =
        <stream<DateTimeRecord, sql:Error>>resultStream;

        // io:println(customerStream);
    
    error? e = customerStream.forEach(function(DateTimeRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.timestampType);
        io:println(rec.dateType);
        io:println(rec.timeType);
        io:println(rec.intervalType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in DateTime table-------");


}




//.........................................................................................................



public type BinaryRecord record{

    int ID;
    byte[] byteaType;

};

function binaryTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
        // "byteaType":"bytea" 

     io:println("------ Start Query in Binary table-------");

    string selectionQuery = selecionQueryMaker("binaryTypes",columns,condition);
        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, BinaryRecord);

    // io:println(resultStream);

    stream<BinaryRecord, sql:Error> customerStream =
        <stream<BinaryRecord, sql:Error>>resultStream;

        // io:println(customerStream);
    
    error? e = customerStream.forEach(function(BinaryRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.byteaType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in Binary table-------");


}











//'''''''''''''''
public type characterRecord record{

    int ID;
    string charType;
    string varcharType;
    string textType;
    string nameType;
    string charWithoutLengthType;

};

function characterTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
    //         "charType":"char(10)",
    //         "varcharType":"varchar(10)",
    //         "textType":"text",
    //         "nameType":"name",
    //         "charWithoutLengthType": "char"
     io:println("------ Start Query in character table-------");

    string selectionQuery = selecionQueryMaker("charTypes",columns,condition);
        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, characterRecord);

    // io:println(resultStream);

    stream<characterRecord, sql:Error> customerStream =
        <stream<characterRecord, sql:Error>>resultStream;

        // io:println(customerStream);
    
    error? e = customerStream.forEach(function(characterRecord rec) {
        io:println("\n");
        io:println(rec);
        io:println(rec.charType);
        io:println(rec.varcharType);
        io:println(rec.textType);
        io:println(rec.nameType);
        io:println(rec.charWithoutLengthType);
        io:println("\n");
    });
    
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in character table-------");


}

//''''''''''''''''''''''''''''''




public type moneyRecord record{

    int ID;
    int moneyType;

};

function moneyTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{
    // "MoneyType":"money"
     io:println("------ Start Query in money table-------");

    string selectionQuery = selecionQueryMaker("moneyTypes","ID, moneytype::numeric::float8",condition);
        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, moneyRecord);

    io:println(resultStream);

    stream<moneyRecord, sql:Error> customerStream =
        <stream<moneyRecord, sql:Error>>resultStream;

        io:println(customerStream);

    error? e = customerStream.forEach(function(moneyRecord rec) {
        io:println(rec);
        io:println(rec.moneyType);
        
    });
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in money table-------");


}


public type numericRecord record{

    int ID;
    int smallIntType;
    int intType;
    int bigIntType;
    decimal decimalType;
    decimal numericType;
    float realType;
    float doublePrecisionType;
    int smallSerialType;
    int serialType;
    int bigSerialType;

};

function numericTableSelection(jdbc:Client jdbcClient, string columns = "*",string condition = "True") returns sql:Error?{

     io:println("------ Start Query in numerica table-------");

    string selectionQuery = selecionQueryMaker("numericTypes",columns,condition);
        stream<record{}, error> resultStream =
        jdbcClient->query(selectionQuery, numericRecord);

    stream<numericRecord, sql:Error> customerStream =
        <stream<numericRecord, sql:Error>>resultStream;

    error? e = customerStream.forEach(function(numericRecord rec) {
        io:println(rec);
        io:println(rec.smallIntType);
        io:println(rec.intType);
        io:println(rec.bigIntType);
        io:println(rec.decimalType);
        io:println(rec.numericType);
        io:println(rec.realType);
        io:println(rec.doublePrecisionType);
        io:println(rec.smallSerialType);
        io:println(rec.serialType);
        io:println(rec.bigSerialType);
        
    });
    if (e is error) {
        io:println(e);
    }

    io:println("------ End Query in numerica table-------");


}










// ======================================================================================================================================================================================================================================================================
// ======================================================================================================================================================================================================================================================================



function numericTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    //Check NAN values and Infinity values

    sql:ExecutionResult|sql:Error? result;
    result = insertNumericTable(jdbcClient,
    
        -32768,-2147483648,-9223372036854775808,
        1.2,
        2.33,
        123456.123456,
        123456789.123456,
        1,1,1

    );
    result = insertNumericTable(jdbcClient,
    
        -32768,-2147483648,-9223372036854775808,
        -92233720368547758079223372036854775807.92233720368547758079223372036854775807,
        -19223372036854775807922337203685477580792233720368547758079223372036854775807.92233720368547758079223372036854775807,
        123456.123456,
        123456789.123456,
        1,1,1

    );
    result = insertNumericTable(jdbcClient,
    
        32767,2147483647,9223372036854775807,
        9223372036854775807922337203685477580792233720368547758079223372036854775807.92233720368547758079223372036854775807,
        -9223372036854775807922337203685477580792233720368547758079223372036854775807.92233720368547758079223372036854775807,
        0.123456,
        1.111222333444555,
        32767,2147483647,9223372036854775807
    );
    return result;

}

function insertNumericTable(jdbc:Client jdbcClient , string|int|float|decimal smallIntType, string|int|float|decimal intType, string|int|float|decimal bigInttypeypeType, string|int|float|decimal decimalType, string|int|float|decimal numericType, string|int|float|decimal realType, string|int|float|decimal doublePrecisionType, string|int|float|decimal smallSerialType, string|int|float|decimal serialType, string|int|float|decimal bigSerialType) returns sql:ExecutionResult|sql:Error?{

   sql:ParameterizedQuery insertQuery =
            `INSERT INTO numericTypes (
                smallIntType, intType, bigIntType, decimalType, numericType, realType, doublePrecisionType, smallSerialType, serialType, bigSerialType
                ) 
             VALUES (
            ${smallIntType}, ${intType}, ${bigInttypeypeType}, ${decimalType}, ${numericType}, ${realType}, ${doublePrecisionType}, ${smallSerialType}, ${serialType}, ${bigSerialType}
            
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}


function moneyTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertMoneyTable(jdbcClient,
    
    "-92233720368547758.08"

    );
    result = insertMoneyTable(jdbcClient,
    
        "92233720368547758.07"

    );
    return result;

}

function insertMoneyTable(jdbc:Client jdbcClient , string|int|float|decimal moneyType) returns sql:ExecutionResult|sql:Error?{
    // "MoneyType":"money"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO moneyTypes (
                moneyType              ) 
             VALUES (
                ${moneyType} ::numeric::money 
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}



function characterTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertCharacterTable(jdbcClient,
    
    "1234567890","1234567890","function characterTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?",
    "12345678123456781234567812345678123456781234567812345678123456789","1"

    );
    result = insertCharacterTable(jdbcClient,
    
        "1","2","3","4","5"

    );
    return result;

}

function insertCharacterTable(jdbc:Client jdbcClient , string charType, string varcharType, string textType, string nameType, string charWithoutLengthType) returns sql:ExecutionResult|sql:Error?{
    //         "charType":"char(10)",
    //         "varcharType":"varchar(10)",
    //         "textType":"text",
    //         "nameType":"name",
    //         "charWithoutLengthType": "char"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO charTypes (
                charType, varcharType, textType, nameType, charWithoutLengthType              ) 
             VALUES (
                ${charType}, ${varcharType}, ${textType}, ${nameType}, ${charWithoutLengthType}
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}



function binaryTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    byte[] byteArray1 = [5, 24, 56, 243, 24, 56, 243, 24, 56, 243, 24, 56, 243, 24, 56, 243, 24, 56, 243, 24, 56, 243, 24, 56, 243, 24, 56, 243];

    byte[] byteArray2 = base16 `aeeecdefabcd12345567888822aeeecdefabcd12345567888822aeeecdefabcd12345567888822aeeecdefabcd12345567888822aeeecdefabcd12345567888822`;

    byte[] byteArray3 = base64 `aGVsbG8gYmFsbGVyaW5hICEhIQ==`;

    result = insertBinaryTable(jdbcClient,
    
    "\\xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"

    );
    result = insertBinaryTable(jdbcClient,
    
        "abc \\153\\154\\155 \\052\\251\\124\\153\\154\\155 \\052\\251\\124\\153\\154\\155 \\052\\251\\124\\153\\154\\155 \\052\\251\\124\\153\\154\\155 \\052\\251\\124"

    );
    result = insertBinaryTable(jdbcClient,
    
        byteArray1

    );
    result = insertBinaryTable(jdbcClient,
    
        byteArray2

    );
    result = insertBinaryTable(jdbcClient,
    
        byteArray3

    );
    return result;

}

function insertBinaryTable(jdbc:Client jdbcClient , string|byte[] byteaType) returns sql:ExecutionResult|sql:Error?{
    // "byteaType":"bytea" 
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO binaryTypes (
                byteaType
                            ) 
             VALUES (
                ${byteaType} :: bytea
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function DateTimeTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertDateTimeTable(jdbcClient,
    
    "2004-10-19 10:23:54","January 8, 1999","04:05:06.789","4 Year"

    );
    result = insertDateTimeTable(jdbcClient,
    
        "2004-10-19 10:23:54+02","January 8, 1999","2003-04-12 04:05:06 America/New_York","4 Month"

    );
    return result;

}

function insertDateTimeTable(jdbc:Client jdbcClient , string|int timestampType, string|int dateType, string|int timeType, string|int intervalType) returns sql:ExecutionResult|sql:Error?{
    //         "timestampType":"timestamp",
    //         "dateType":"date",
    //         "timeType":"time",
    //         "intervalType":"interval"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO dateTimeTypes (
                timestampType, dateType, timeType, intervalType
                             ) 
             VALUES (
                ${timestampType}:: timestamp, ${dateType}::Date, ${timeType}::time, ${intervalType} :: interval
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function booleanTimeTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertBooleanTimeTable(jdbcClient,
    
    "true"

    );
    result = insertBooleanTimeTable(jdbcClient,
    
        true

    );
    result = insertBooleanTimeTable(jdbcClient,
    
        "yes"

    );
    result = insertBooleanTimeTable(jdbcClient,
    
        "on"

    );
    result = insertBooleanTimeTable(jdbcClient,
    
        "1"

    );
    return result;

}

function insertBooleanTimeTable(jdbc:Client jdbcClient ,boolean|string|int booleanType) returns sql:ExecutionResult|sql:Error?{
    //         "booleanType":"boolean"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO booleantypes (
                booleanType
                             ) 
             VALUES (
                ${booleanType} :: boolean
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}


function enumTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertEnumTable(jdbcClient,
    
    "value1"

    );
    result = insertEnumTable(jdbcClient,
    
        "value2"

    );
    return result;

}

function insertEnumTable(jdbc:Client jdbcClient ,string|int|boolean|float|decimal|byte[]|xml enumType) returns sql:ExecutionResult|sql:Error?{
// "enumType":"enumValues"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO enumTypes (
                enumType
                             ) 
             VALUES (
                ${enumType}::enumvalues
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function geometricTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertGeometricTable(jdbcClient,
    
    "(1,2)", "{1,2,3}","[(1,2),(3,4)]","((1,2),(3,4))","((1,2),(3,4))","((1,2),(3,4))","((1,2),3)"

    );
    result = insertGeometricTable(jdbcClient,
    
    "(1,2)", "{1,2,3}","[(1,2),(3,4)]","((1,2),(3,4))","((1,2),(3,4))","((1,2),(3,4))","((1,2),3)"

    );
    
    return result;

}

function insertGeometricTable(jdbc:Client jdbcClient ,string pointType, string lineType, string lsegType, string boxType, string pathType, string polygonType, string circleType) returns sql:ExecutionResult|sql:Error?{
// "pointType":"point",
//             "lineType":"line",
//             "lsegType":"lseg",
//             "boxType":"box",
//             "pathType":"path",
//             "polygonType":"polygon",
//             "circleType":"circle"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO geometrictypes (
                pointType, lineType, lsegType, boxType, pathType, polygonType, circleType
                             ) 
             VALUES (
                ${pointType}::point, ${lineType}::line, ${lsegType}::lseg, ${boxType}::box, ${pathType}::path, ${polygonType}::polygon, ${circleType}::circle
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function networkTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertNetworkTable(jdbcClient,
    
    "192.168.0.1/24","::ffff:1.2.3.0/120","08:00:2b:01:02:03","08-00-2b-01-02-03-04-05"

    );
    result = insertNetworkTable(jdbcClient,
    
        "192.168.0.1/24","::ffff:1.2.3.0/120","08:00:2b:01:02:03","08-00-2b-01-02-03-04-05"

    );
    return result;

}

function insertNetworkTable(jdbc:Client jdbcClient ,string inetType, string cidrType, string macaddrType, string macaddr8Type) returns sql:ExecutionResult|sql:Error?{
// "inetType":"inet",
//             "cidrType":"cidr",
//             "macaddrType":"macaddr",
//             "macaddr8Type":"macaddr8"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO networkTypes (
                inetType, cidrType, macaddrType, macaddr8Type
                             ) 
             VALUES (
                ${inetType}::inet, ${cidrType}::cidr, ${macaddrType}::macaddr, ${macaddr8Type}::macaddr8
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}
function BitTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertBitTable(jdbcClient,
    
    "111","B001","B100","B101"

    );
    // result = insertBitTable(jdbcClient,
    
    //     "001","B101","B101","B101"

    // );
    return result;

}

function insertBitTable(jdbc:Client jdbcClient ,string bitType, string bitVaryType, string bitVaryType2, string bitOnlyType) returns sql:ExecutionResult|sql:Error?{

// "bitType":"bit(3)",
//             "bitVaryType":"BIT VARYING(5)",
//             "bitVaryType2":"BIT VARYING(7)",
//             "bitOnlyType":"bit"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO bitTypes (
                bitType, bitVaryType, bitVaryType2, bitOnlyType
                             ) 
             VALUES (
                ${bitType}::bit(3), ${bitVaryType}::BIT VARYING(5), ${bitVaryType2}::BIT VARYING(7), ${bitOnlyType}::bit
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}


function textSearchTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertTextSearchTable(jdbcClient,
    
    "a fat cat sat on a mat and ate a fat rat","fat & rat"

    );
    result = insertTextSearchTable(jdbcClient,
    
        "a fat cat sat on a mat and ate a fat rat","fat & rat"

    );
    return result;

}

function insertTextSearchTable(jdbc:Client jdbcClient ,string tsvectorType, string tsqueryType) returns sql:ExecutionResult|sql:Error?{

// "tsvectorType":"tsvector",
//             "tsqueryType":"tsquery"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO textSearchTypes (
                tsvectorType, tsqueryType
                             ) 
             VALUES (
                ${tsvectorType}::tsvector, ${tsqueryType}::tsquery
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function UUIDTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertUUIDTable(jdbcClient,
    
    "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"

    );
    result = insertUUIDTable(jdbcClient,
    
        "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"

    );
    result = insertUUIDTable(jdbcClient,
    
    "a0eebc999c0b4ef8bb6d6bb9bd380a11"

    );
    result = insertUUIDTable(jdbcClient,
    
        "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11"

    );
    result = insertUUIDTable(jdbcClient,
    
        "{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}"

    );
    result = insertUUIDTable(jdbcClient,
    
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"

    );
    return result;

}

function insertUUIDTable(jdbc:Client jdbcClient ,string uuidType) returns sql:ExecutionResult|sql:Error?{

// "uuidType":"uuid"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO uuidTypes (
                uuidType
                             ) 
             VALUES (
                ${uuidType}::uuid
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function xmlTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    
    result = insertXmlTable(jdbcClient,
    
        "<foo>bar</foo>"

    );
    result = insertXmlTable(jdbcClient,
    
        "bar"

    );
    result = insertXmlTable(jdbcClient,
    
     xml `<foo><tag>bar</tag><tag>tag</tag></foo>`

    );
    return result;

}

function insertXmlTable(jdbc:Client jdbcClient ,string|xml xmlType) returns sql:ExecutionResult|sql:Error?{

// "xmlType":"xml"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO xmlTypes (
                xmlType
                             ) 
             VALUES (
                ${xmlType}::xml
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function JsonTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertJsonTable(jdbcClient,
    
     "{\"bar\": \"baz\", \"balance\": 7.77, \"active\":false}", "{\"bar\": \"baz\", \"balance\": 7.77, \"active\":false}","$.floor[*].apt[*] ? (@.area > 40 && @.area < 90) ? (@.rooms > 2)"

    );
    result = insertJsonTable(jdbcClient,
    
      "{\"bar\": \"baz\", \"balance\": 7.77, \"active\":false}", "{\"bar\": \"baz\", \"balance\": 7.77, \"active\":false}","$.floor[*].apt[*] ? (@.area > 40 && @.area < 90) ? (@.rooms > 2)"  

    );
    return result;

}

function insertJsonTable(jdbc:Client jdbcClient ,string jsonType, string jsonbType, string jsonPathType) returns sql:ExecutionResult|sql:Error?{

// "ID": "SERIAL", 
//             "jsonType":"json",
//             "jsonbType":"jsonb",
//             "jsonpathType":"jsonpath"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO jsonTypes (
                jsonType, jsonbType, jsonPathType
                             ) 
             VALUES (
                ${jsonType}:: json, ${jsonbType}:: jsonb, ${jsonPathType}:: jsonpath 
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}
function arrayTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertArrayTable(jdbcClient,
    

            "{{\"A\"},{\"B\"}}","{\"A\",\"B\",\"\"}", "{1,2,3,4,5}","{{1,2},{3,4}}", "{1,2,3,4}","{1,2,3,4,5,6,7}"
    );
    result = insertArrayTable(jdbcClient,
    

            "{{\"A\"},{\"B\"}}","{\"A\",\"B\",\"\"}", "{1,2,3,4,5}","{{1,2},{3,4}}", "{1,2,3,4}","{1,2,3,4,5,6,7}"
    );
    return result;

}

function insertArrayTable(jdbc:Client jdbcClient ,string textArrayType ,string textArray2Type , string integerArrayType, string integerArray2Type, string arrayType, string array2Type) returns sql:ExecutionResult|sql:Error?{

            // "textArrayType":"text[][]",
            // "textArray2Type":"text[]",
            // "integerArrayType":"int[]",
            // "integerArray2Type":"int[][]",
            // "arrayType":"int array[5]",
            // "array2Type":"int array"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO arrayTypes (
                textArrayType,textArray2Type, integerArrayType,integerArray2Type, arrayType, array2Type
                             ) 
             VALUES (
                ${textArrayType}::text[][],${textArray2Type}::text[], ${integerArrayType}::int[],${integerArray2Type}::int[][], ${arrayType}:: int array[5], ${array2Type}:: int array
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function ComplexTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertComplexTable(jdbcClient,
    

        "(1.1,2.2)","(\"Name\",2,456.32)"
    );
    result = insertComplexTable(jdbcClient,
    

        "(1.1,2.2)","(\"Name\",2,456.32)"
    );
    return result;

}

function insertComplexTable(jdbc:Client jdbcClient ,string complexType, string inventoryType) returns sql:ExecutionResult|sql:Error?{

            // "complexType":"complex",
            // "inventoryType":"inventory_item"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO complexTypes (
                complexType, inventoryType
                             ) 
             VALUES (
                ${complexType}::complex, ${inventoryType}::inventory_item
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}

function RangeTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertRangeTable(jdbcClient,
    

        "(2,50)","(10,100)","(0,24)","(2010-01-01 14:30, 2010-01-01 15:30)","(2010-01-01 14:30, 2010-01-01 15:30)","(2010-01-01 14:30, 2010-01-01 )","(1,4)"
    );
    result = insertRangeTable(jdbcClient,
    

        "(1,3)","(10,100)","(0,24)","(2010-01-01 14:30, 2010-01-01 15:30)","(2010-01-01 14:30, 2010-01-01 15:30)","(2010-01-01 14:30, 2010-01-01 )","(2,12)"
    );
    return result;

}

function insertRangeTable(jdbc:Client jdbcClient ,string int4rangeType, string int8rangeType, string numrangeType,string tsrangeType,string tstzrangeType,string daterangeType, string floatrangeType) returns sql:ExecutionResult|sql:Error?{

            // "int4rangeType":"int4range",
            // "int8rangeType":"int8range",
            // "numrangeType":"numrange",
            // "tsrangeType":"tsrange",
            // "tstzrangeType":"tstzrange",
            // "daterangeType":"daterange",
            // "floatrangeType":"floatrange"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO rangeTypes (
                int4rangeType, int8rangeType, numrangeType, tsrangeType, tstzrangeType, daterangeType, floatrangeType
                             ) 
             VALUES (
                ${int4rangeType}::int4range, ${int8rangeType}::int8range, ${numrangeType}::numrange, ${tsrangeType}::tsrange, ${tstzrangeType}::tstzrange, ${daterangeType}::daterange, ${floatrangeType}::floatrange

            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}


function domainTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertDomainTable(jdbcClient,
    
        1

    );
    result = insertDomainTable(jdbcClient,
    
        "1"

    );
    return result;

}

function insertDomainTable(jdbc:Client jdbcClient ,string|int posintType) returns sql:ExecutionResult|sql:Error?{

            // "posintType":"posint"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO domainTypes (
                posintType
                             ) 
             VALUES (
                ${posintType}::posint
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}



function objectIdentifierTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertObjectIdentifierTable(jdbcClient,
    
        "564182","pg_type","english","simple","pg_catalog","!","*(int,int)","NOW","sum(int4)","postgres","int"

    );
    // result = insertObjectIdentifierTable(jdbcClient,
    
    //     "564182","pg_type","english","simple","pg_catalog","*","*(integer,​integer) or -(NONE,​integer)","sum","sum(int4)","smithee","integer"

    // );
    return result;

}

function insertObjectIdentifierTable(jdbc:Client jdbcClient ,string oidType, string regclassType, string regconfigType, string regdictionaryType, string regnamespaceType, string regoperType, string regoperatorType, string regprocType, string regprocedureType, string regroleType, string regtypeType ) returns sql:ExecutionResult|sql:Error?{

    //         "oidType" : "oid",
    //         "regclassType" : "regclass",
    //         "regconfigType" : "regconfig",
    //         "regdictionaryType" : "regdictionary",
    //         "regnamespaceType" : "regnamespace",
    //         "regoperType" : "regoper",
    //         "regoperatorType" : "regoperator",
    //         "regprocType" : "regproc",
    //         "regprocedureType" : "regprocedure",
    //         "regroleType" : "regrole",
    //         "regtypeType" : "regtype"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO objectIdentifierTypes (
                oidType, regclassType, regconfigType, regdictionaryType, regnamespaceType, regoperType, regoperatorType, regprocType, regprocedureType, regroleType, regtypeType
                             ) 
             VALUES (
                ${oidType} ::oid, ${regclassType} ::regclass, ${regconfigType} ::regconfig, ${regdictionaryType} ::regdictionary, ${regnamespaceType} ::regnamespace, ${regoperType} ::regoper, ${regoperatorType} ::regoperator, ${regprocType} ::regproc, ${regprocedureType} ::regprocedure, ${regroleType} ::regrole, ${regtypeType} ::regtype
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}



function pslgnTableInsertions(jdbc:Client jdbcClient) returns sql:ExecutionResult|sql:Error?{

    sql:ExecutionResult|sql:Error? result;
    result = insertPslgnTable(jdbcClient,
    
        "16/B374D848"

    );
    result = insertPslgnTable(jdbcClient,
    
        "16/B374D848"

    );
    return result;

}

function insertPslgnTable(jdbc:Client jdbcClient ,string pglsnType) returns sql:ExecutionResult|sql:Error?{

            // "pglsnType" : "pg_lsn"
   sql:ParameterizedQuery insertQuery =
            `INSERT INTO pglsnTypes (
                pglsnType
                             ) 
             VALUES (
                ${pglsnType}::pg_lsn
            )`;
    

    sql:ExecutionResult|sql:Error result = jdbcClient->execute(insertQuery);

    if (result is sql:ExecutionResult) {
        io:println("\nInsert success, generated Id: ", result.lastInsertId);
    } 
    else{
        io:println("\nError ocurred while insert to numeric table\n");
        io:println(result);
        io:println("\n");
    }
    
    return result;
        

}



//==================================================================================================================================================================================





function initializeTable(jdbc:Client jdbcClient, string tableName,string createQuery) returns int|string|sql:Error? {

    sql:ExecutionResult result =  check jdbcClient->execute("DROP TABLE IF EXISTS "+tableName);

    io:println("Drop table executed. ", result);

    result = check jdbcClient->execute("CREATE TABLE IF NOT EXISTS " + tableName +" "+ createQuery);
    
    return result.affectedRowCount;


}

public function createQueryMaker(map<string> args, string primarykey) returns CreateQueries{

    io:println("Table create query initializing....");
    

    string createQuery = "";
    createQuery += "(";

    foreach var [name,datatype] in args.entries() {
    createQuery += name + " " + datatype+", ";
    }
    createQuery += "PRIMARY KEY("+primarykey+")";
    createQuery += ")";

    // string createQuery = "(customerId SERIAL, firstName VARCHAR(300),lastName VARCHAR(300), registrationID INTEGER, creditLimit real,country VARCHAR(300), PRIMARY KEY (customerId))";
    // string insertQuery = "(firstName,lastName,registrationID,creditLimit,country) VALUES ('Peter', 'Stuart', 1, 5000.71, 'USA')";

     CreateQueries result =  {
         createQuery
            };

    return result;

}




function createNumericTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    string tableName = "numericTypes";

        CreateQueries createTableQuery = createQueryMaker({

            "ID": "SERIAL", "smallIntType":"smallInt","intType": "integer", "bigIntType": "bigint", "decimalType": "decimal","numericType": "numeric",
            "realType":"real", "doublePrecisionType":"double precision","smallSerialType":"smallserial", "serialType":"serial", "bigSerialType":"bigserial"

        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}


function createMoneyTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    string tableName = "moneyTypes";

        CreateQueries createTableQuery = createQueryMaker({

            "ID": "SERIAL", 
            "MoneyType":"money"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createCharacterTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    string tableName = "charTypes";

        CreateQueries createTableQuery = createQueryMaker({

            "ID": "SERIAL", 
            "charType":"char(10)",
            "varcharType":"varchar(10)",
            "textType":"text",
            "nameType":"name",
            "charWithoutLengthType": "char"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createBinaryTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    string tableName = "binaryTypes";

        CreateQueries createTableQuery = createQueryMaker({

            "ID": "SERIAL", 
            "byteaType":"bytea"    //Two types need to test. bytea hexa type and bytea escape type.
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createDateTimeTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    string tableName = "dateTimeTypes";

        CreateQueries createTableQuery = createQueryMaker({
            //Need to test with timezone and without time zone. And also several ways in interval type need to test.
            "ID": "SERIAL", 
            "timestampType":"timestamp",
            "dateType":"date",
            "timeType":"time",
            // "timeWithTimeZoneType":"timestamp with time zone",
            "intervalType":"interval"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createBooleanTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    string tableName = "booleanTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "booleanType":"boolean"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createEnumTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "enumTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "enumType":"enumValues"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createGeometricTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "geometricTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "pointType":"point",
            "lineType":"line",
            "lsegType":"lseg",
            "boxType":"box",
            "pathType":"path",
            "polygonType":"polygon",
            "circleType":"circle"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}


function createNetworkTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "networkTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "inetType":"inet",
            "cidrType":"cidr",
            "macaddrType":"macaddr",
            "macaddr8Type":"macaddr8"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createBitTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "bitTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "bitType":"bit(3)",
            "bitVaryType":"BIT VARYING(5)",
            "bitVaryType2":"BIT VARYING(7)",
            "bitOnlyType":"bit"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}


function createTextSearchTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "textSearchTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "tsvectorType":"tsvector",
            "tsqueryType":"tsquery"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createUuidTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "uuidTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "uuidType":"uuid"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createXmlTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "xmlTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "xmlType":"xml"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createJsonTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "jsonTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "jsonType":"json",
            "jsonbType":"jsonb",
            "jsonpathType":"jsonpath"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createArrayTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "arrayTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "textArrayType":"text[][]",
            "textArray2Type":"text[]",
            "integerArrayType":"int[]",
            "integerArray2Type":"int[][]",
            "arrayType":"int array[5]",
            "array2Type":"int array"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createCompositeTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "complexTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "complexType":"complex",
            "inventoryType":"inventory_item"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}


function createRangeTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "rangeTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "int4rangeType":"int4range",
            "int8rangeType":"int8range",
            "numrangeType":"numrange",
            "tsrangeType":"tsrange",
            "tstzrangeType":"tstzrange",
            "daterangeType":"daterange",
            "floatrangeType":"floatrange"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createDomainTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "domainTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "posintType":"posint"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createObjectIdentifierTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "objectIdentifierTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "oidType" : "oid",
            "regclassType" : "regclass",
            // "regcollationType" : "regcollation",
            "regconfigType" : "regconfig",
            "regdictionaryType" : "regdictionary",
            "regnamespaceType" : "regnamespace",
            "regoperType" : "regoper",
            "regoperatorType" : "regoperator",
            "regprocType" : "regproc",
            "regprocedureType" : "regprocedure",
            "regroleType" : "regrole",
            "regtypeType" : "regtype"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createPglsnTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "pglsnTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "pglsnType" : "pg_lsn"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}

function createPseudoTypeTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

   string tableName = "pseudoTypes";

        CreateQueries createTableQuery = createQueryMaker({
            "ID": "SERIAL", 
            "anyType" : "any",
            "anyelementType" : "anyelement",
            "anyarrayType" : "anyarray",
            "anynonarrayType" : "anynonarray",
            "anyenumType" : "anyenum",
            "anyrangeType" : "anyrange",
            "anycompatibleType" : "anycompatible",
            "anycompatiblearrayType" : "anycompatiblearray",
            "anycompatiblenonarrayType" : "anycompatiblenonarray",
            "anycompatiblerangeType" : "anycompatiblerange",
            "cstringType" : "cstring",
            "internalType" : "internal",
            "language_handlerType" : "language_handler",
            "fdw_handlerType" : "fdw_handler",
            "table_am_handlerType" : "table_am_handler",
            "index_am_handlerType" : "index_am_handler",
            "tsm_handlerType" : "tsm_handler",
            "recordType" : "record",
            "triggerType" : "trigger",
            "event_triggerType" : "event_trigger",
            "pg_ddl_commandType" : "pg_ddl_command",
            "voidType" : "void",
            "unknownType" : "unknown"
        },"ID");

        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
    }

    return initResult;

}





