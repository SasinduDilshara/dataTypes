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

function insertQueryMaker(map<string> args) returns InsertQueries{

    string insertQuery = "";
    string columns = "";
    string values = "";

    insertQuery += "(";

    foreach var [column,value] in args.entries() {
       columns += column +",";
       values += value + ",";
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

// function insertTable()


function initializeTable(jdbc:Client jdbcClient, string tableName,string createQuery, string insertQuery) returns int|string|sql:Error? {

    sql:ExecutionResult result =  check jdbcClient->execute("DROP TABLE IF EXISTS "+tableName);

    io:println("Drop table executed. ", result);

    result = check jdbcClient->execute("CREATE TABLE IF NOT EXISTS " + tableName +" "+ createQuery);

    result = check jdbcClient->execute("INSERT INTO " + tableName+" " +insertQuery);

    io:println("Rows affected: ", result.affectedRowCount);
    io:println("Generated Customer ID: ", result.lastInsertId);
    
    return result.lastInsertId;


}

public function createQueryMaker(map<string> args, string primarykey) returns CreateQueries{

    string[] a = ["1","2","3","4","5"];
    string columns1 = "";
    foreach var column in a {
       columns1 += column +",";
    }
    int length = columns1.length();
    string c = columns1.substring(0,length-1);
    io:println(c);
    

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

        string tableName = "Test2";

        CreateQueries createTableQuery = createQueryMaker({

            "customerId": "SERIAL", "firstName":"VARCHAR(300)","lastName": "VARCHAR(300)", "registrationID": "INTEGER", "creditLimit": "real","country": "VARCHAR(300)"

        },"customerId");

        InsertQueries insertTableQuery = insertQueryMaker({

            // "firstName":"Peter","lastName": "Stuward", "registrationID": "1", "creditLimit": "5000.01","country": "USA"
            "firstName":"'Peter'","lastName": "'Stuward'", "registrationID": "1", "creditLimit": "5000.01","country": "'USA'"


        });


        
        int|string|sql:Error? initResult = initializeTable(jdbcClient, tableName , createTableQuery.createQuery,insertTableQuery.insertQuery);
        if (initResult is int) {
            io:println("Sample executed successfully!");
        } 
        else if (initResult is sql:Error) {
            io:println("Customer table initialization failed: ", initResult);
        }
        sql:Error? e = jdbcClient.close();   
    } 
    else {
        io:println("Initialization failed!!");
        io:println(jdbcClient);
    }
}





