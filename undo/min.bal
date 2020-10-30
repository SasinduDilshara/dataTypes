import ballerina/io;
import ballerina/java.jdbc;
import ballerina/sql;

public type Queries record {
    string createQuery;
    string insertQuery;
    string tableName;
};

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

public function createQueryMaker(map<string> args, string primarykey) returns Queries{
    
    string createQuery = "";
    createQuery += "(";

     foreach var [name,datatype] in args.entries() {
        createQuery += name + " " + datatype+", ";
    }
    createQuery += "PRIMARY KEY("+primarykey+")";
    createQuery += ")";

    // string createQuery = "(customerId SERIAL, firstName VARCHAR(300),lastName VARCHAR(300), registrationID INTEGER, creditLimit real,country VARCHAR(300), PRIMARY KEY (customerId))";
    string insertQuery = "(firstName,lastName,registrationID,creditLimit,country) VALUES ('Peter', 'Stuart', 1, 5000.71, 'USA')";
    string tableName = "Test2";

     Queries result =  {
         createQuery,
         insertQuery,
         tableName
            };

    return result;

}

public function main() {
    jdbc:Client|sql:Error jdbcClient =  new ("jdbc:postgresql://localhost:5432/datatypes","postgres","postgres");
           
    if (jdbcClient is jdbc:Client) {

        Queries result = createQueryMaker({

            "customerId": "SERIAL", "firstName":"VARCHAR(300)","lastName": "VARCHAR(300)", "registrationID": "INTEGER", "creditLimit": "real","country": "VARCHAR(300)"

        },"customerId");
        
        int|string|sql:Error? initResult = initializeTable(jdbcClient, result.tableName , result.createQuery,result.insertQuery);
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





