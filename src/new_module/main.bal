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

    result = jdbcClient->execute("Drop type enumValues if exists");
    
    result = jdbcClient->execute("CREATE TYPE enumValues AS ENUM ('value1','value2','value3')");

    if(result is sql:Error){
        io:println("Error occurred while creating the enum type..");
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

// function insertTable() int|string|sql:Error?


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

        int|string|sql:Error?? err = tableCreations(jdbcClient);
        
        sql:Error? e = jdbcClient.close();   
    } 
    else {
        io:println("Initialization failed!!");
        io:println(jdbcClient);
    }
}


function tableCreations(jdbc:Client jdbcClient) returns int|string|sql:Error??{

    int|string|sql:Error?? result;

    result = createNumericTable(jdbcClient);
    result = createMoneyTable(jdbcClient);
    result = createCharacterTable(jdbcClient);
    result = createBinaryTable(jdbcClient);
    result = createDateTimeTable(jdbcClient);
    result = createBooleanTable(jdbcClient);
    result = createEnumTable(jdbcClient);
    result = createGeometricTable(jdbcClient);
    result = createNetworkTable(jdbcClient);
    // result = createMoneyTable(jdbcClient);
    // result = createCharacterTable(jdbcClient);
    // result = createMoneyTable(jdbcClient);
    // result = createCharacterTable(jdbcClient);

     return result;   


}

function createNumericTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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


function createMoneyTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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

function createCharacterTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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

function createBinaryTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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

function createDateTimeTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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

function createBooleanTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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

function createEnumTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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

function createGeometricTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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


function createNetworkTable(jdbc:Client jdbcClient) returns int|string|sql:Error??{

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






