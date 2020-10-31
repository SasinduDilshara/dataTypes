import ballerina/io;
import ballerina/java.jdbc;
import ballerina/sql;

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

public function main() {
    jdbc:Client|sql:Error jdbcClient =  new ("jdbc:postgresql://localhost:5432/datatypes","postgres","postgres");
           
    if (jdbcClient is jdbc:Client) {

        sql:Error|sql:ExecutionResult result = setUp(jdbcClient);

        int|string|sql:Error? err = tableCreations(jdbcClient);
        
        sql:Error? e = jdbcClient.close();   
    } 
    else {
        io:println("Initialization failed!!");
        io:println(jdbcClient);
    }
}


function tableCreations(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    int|string|sql:Error? result;

    result = createNumericTable(jdbcClient);
    result = createMoneyTable(jdbcClient);
    result = createCharacterTable(jdbcClient);
    result = createBinaryTable(jdbcClient);
    result = createDateTimeTable(jdbcClient);
    result = createBooleanTable(jdbcClient);
    result = createEnumTable(jdbcClient);
    result = createGeometricTable(jdbcClient);
    result = createNetworkTable(jdbcClient);
    result = createBitTable(jdbcClient);
    result = createTextSearchTable(jdbcClient);
    result = createUuidTable(jdbcClient);
    result = createXmlTable(jdbcClient);
    result = createJsonTable(jdbcClient);
    result = createArrayTable(jdbcClient);
    result = createCompositeTable(jdbcClient);
    result = createRangeTable(jdbcClient);
    result = createDomainTable(jdbcClient);
    result = createObjectIdentifierTable(jdbcClient);
    result = createPglsnTable(jdbcClient);
    // result = createPseudoTypeTable(jdbcClient);
    
     return result;   


}

function createNumericTable(jdbc:Client jdbcClient) returns int|string|sql:Error?{

    string tableName = "numericTypes";

        CreateQueries createTableQuery = createQueryMaker({

            "ID": "SERIAL", "smallIntType":"smallInt","intType": "integer", "bigntType": "bigint", "decimalType": "decimal","numericType": "numeric",
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
            "tsvectotType":"tsvector",
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
            "integerArrayType":"int[]",
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





