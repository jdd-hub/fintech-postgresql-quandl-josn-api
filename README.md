# fintech-postgresql-quandl-josn-api

This repository contains my latest work ingesting stock market data using the Quandl API with the JSON documents stored on a drive, processed and pushed to a PostgreSQL database.

This project is in part connected to https://github.com/jdd-hub/fintech-mongo-db-quandl-josn-api. On a high level, The package consists of a MongoDB class containing all of the functionality required for interaction with the database. The Markets class consists of a series of CRUD functions necessary for interaction with the individual markets and stock data collections. The markets class inherits the connection for communication with the database. The insert_markets and fetch markets are simply processes which do as they are named, one inserts the individual stock market reference details and codes required by the API to be able to fetch and store the latest stock performance data. For more information, please head over to the other project. 

This project processes the data within the JSON documents stored on the drive that was previously extracted through the API and pushed to a PostgresSQL database. 

fetch_stock_history.py - this is the main feature within the project. 

1. The process goes through each directory and file within the root_path - directory. 
2. get_json_payload loads the JSON document file from the current file_path.
3. The JSON document converts to a DataFrame. 
4. The JSON document format has the data and the keys stored in separate elements, used as columns for the DataFrame. 
5. As part of the data requirements, the filename is as a column. Used to log the origin for a row of data, along with the market code and year.
6. I also perform data cleansing on the date format within the DataFrame.
7. The column case is changed to lower case so that the DataFrame structure matches the PostgresSQL database table structure. 
8. Finally, the order of the columns is also adjusted to match the PostgresSQL database table.
9. In a separate custom function .to_sql, I push the DataFrame to the PostgreSQL database. 

The .to_sql function connects to the database, along with the built-in push .to_sql function available within the Pandas DataFrame. Additionally, the function also tracks the proccess_time.  It uses a custom config parser config.py that extracts the database connection details. Stored in a separate instruction file, makes for easier distribution and managing security.

Additionally, there are a few more functions that are part of the overall process. 

markets_csv_export.py - has been created to convert the initial market details list/data dictionary into a CSV file so that maintenance is more manageable over time. All one needs to do is update the list within the CSV file and run the inserts_markets.py. 

insert_markets.py - process loads the data from the CSV into a DataFrame and uses the built-in .to_sql function to push the data to the PostgreSQL database table - markets. Additionally, the function also tracks the proccess_time. It uses a custom config parser config.py that extracts the database connection details. Stored in a separate instruction file, makes for easier distribution and managing security.

create_table.py - using the psycopg2 module. The create_table function contains: create table SQL code that makes up the database associated with the project along with the function that extracts the database connection details, stored in a separate instruction file. 

The process is quite simple: 

1. Get the database connection parameters;
2. Create a connection to the PostgreSQL database;
3. Create a cursor;
4. Execute the create table SQL statement;
5. Close the cursor;
6. Commit transactions;
7. Close the connection;

DONE. 

database.ini - this file used throughout the project where a database interaction is occurring, it consists of the following sections with parameters.

[postgresql]
host= database server address, e.g., localhost or an IP address;
dbname= the name of the database that you want to connect;
user= the username used to authenticate;
password= password used to authenticate;
