# commons-mysql
`commons-mysql` contains a collection of classes and methods that allow interaction with a database. Basic operations on DB such as creating and dropping databases, push/pull data, truncate or delete from a table, etc. are provided through various methods
## Usage
### pom.xml
To use commons-mysql, include the following in the project's pom.xml
```xml
<dependency>
    <groupId>com.increff.commons</groupId>
    <artifactId>commons-mysql</artifactId>
    <version>${increff-commons-mysql.version}</version>
    <scope>test</scope>
</dependency>
```
## Key Classes
### SqlUtil
SqlUtil provides an interface for the creation of fully composed SQL Query Commands which can be directly executed using the CmdUtil.runCmd() method from commons-lang library. To construct an SqlUtil object, basic SQL setup parameters, such as host, username, password and schema are passed into the constructor. After this, getQueryCmd() or getAdminCmd() method may be called on any SQL query to obtain the fully composed command array required for execution. The primary methods present are:

- `String[] getQueryCmd(String query)`: Return fully composed SQL Query array for the input query
- `String[] getDeleteAllCommand(String tableName)`: Return command for deleting all records in the table using DELETE
- `String[] getTruncateCmd(String tableName)`: Return command to delete all records in the table using TRUNCATE

#### Example
As an example, when the SQL command to create a database, `CREATE DATABASE "test_database"` is passed into `CmdUtil.getAdminCmd()`, the fully composed command array returned is as follows:

`[mysql, --host=host, --user=username, --password=password, -e, CREATE DATABASE "test_database"]`

As mentioned, this array can be passed directly to the `CmdUtil.runCmd()` method for execution

### DbCmd
WARNING: This class uses TSV format for files. It automatically applies .tsv to the given file-names

Provides access to certain DB functions that may be run directly such as to create a database, pull data, etc. The primary functions are:

- `void createDb(String schema)` Constructs and runs admin SQL Command: CREATE SCHEMA `schema` DEFAULT CHARACTER SET utf8
- `void dropDb(String schema)` Runs SQL query to drop a database (if it exists) DROP DATABASE IF EXISTS "schema"
- `void pull(String fileName, String fullQuery)` Pull a file from the DB and store in specified location
- `void pull(HashMap<String, String> fileQueryMap)` Pull a collection of files from the database, represented by the fileQueryMap HashMap. fileQueryMap maps the location to where the file has to be pulled to (i.e destination for the pulled file), and the query that has to be run for pulling the corresponding file
- `void pullAppend(String fileName, String fullQuery)` Pull data from database and append to the specified TSV file. The filename should be without the extension as .tsv is automatically appended
- `void processQuery(String fullQuery)` Construct a full (non admin) SQL query, using the inputted fullQuery as the basic query. and then execute it
- `void push(String fileName)` Push contents of a single file on to the database based on format specified in getImportCmd()
- `void truncate(String tableName)` Truncate a table, i.e. remove all records from the table using SQL TRUNCATE (DDL operation)
- `void delete(String tableName)` Delete all records stored in the specified table using SQL DELETE (DML operation)
## DbQuery
Used to setup the JDBC Driver for interacting with the database and creation of Connections. To instantiate DbQuery, the JDBC Driver class name, Driver URL, username and password are required. Once instantiated, the methods that may be used include

- `Connection getConnection()`: Uses the provided JDBC URL, username, password to instantiate and return a Connection object to interact with the DB
- `void closeConnection(): Quietly closes the connection corresponding to the calling DbQuery object
List<ResultRow> executeQuery(String query)`: Execute the specified SQL query and return the result as a list of ResultRow objects
int importData(String filename, String tableName)`: Reads rows from a file, read from the client host, into a table using JDBC connection
- `void exportData(String filename, String query)`: Writes rows, resulting from a query, to a file using a JDBC Connection
## QueryResource
Provides utilities for handling queries. Primary methods are

- `String getQuery(String queryName, Properties props)` Returns a fully composed query by substituting the variables in the query string with values from a Properties object. The queryName argument is an identifier for the query stored at resourceBase location. Note: The query variables should be in the format ${variable_name}
- `String getQuery(String queryName, Map<String, String> data)` Returns a fully composed query by substituting the variables in the query string with values from a HashMap mapping variable names to their values. queryName argument is an identifier for the query variable at resourceBase
- `String getQuery(String resourceName)` Fetch the query string corresponding to the identifier resourceName stored in the location represented by the resourceBase.
- `static String cleanComments(String s)` Removes all SQL comments from the inputted string

## License
Copyright (c) Increff

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License
is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied. See the License for the specific language governing permissions and limitations under
the License.
