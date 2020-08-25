import os
import json
import pandas as pd
from time import process_time
from config import config
import sqlalchemy as db


def to_sql(data_frame):

    # Record the start time for the process.
    start_time = process_time()

    # Get the PostgreSQL configuration parameters.
    db_host = config()['host']
    db_user = config()['user']
    db_pass = config()['password']
    db_name = config()['dbname']

    # Set the connection string.
    con_string = str(f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}")

    # Create the database engine object.
    engine = db.create_engine(con_string)

    # Create the connection to the database and connect.
    con = engine.connect()

    # Write the DataFrame to the table.
    data_frame.to_sql("stock_history", con, if_exists="append", index=False)

    # Record the end time for the process.
    end_time = process_time()

    # Close the connection to the database.
    con.close()

    # Calculate the time for the entire process.
    elapsed_time = end_time - start_time

    # Return the process time to the user.
    return f"Frame written to table. \nElapsed time: {elapsed_time:0.4f} seconds."


def get_json_payload(file_path):

    # Load the JSON document from file path.
    with open(file_path) as json_file:
        return json.load(json_file)


# The file path for the JSON documents storage.
root_path = 'Users/julianmuscatdoublesin/PycharmProjects/PythonLibrary/ds_quandl/json_api_stock_markets'

# Print process message to console.
print("\nStock Market End of Day Data")
print("\nRepository:", root_path)
print("\nStarting to load JSON document from the root...")

# Set the root directory to the JSON documents.
root = os.listdir(root_path.replace("Users", "/Users"))

# Go through each file within the root directory.
for folder in root:

    # Skip system folders.
    if folder.find('.') != 0:

        # Print the current folders to console.
        print("\nSub-Directory:", folder)

        # Set sub-folder root.
        sub_folder_path = root_path.split("/")
        sub_folder_path.append(folder)
        sub_folder_root = '/'.join(map(str, sub_folder_path))

        sub_folder = os.listdir(sub_folder_root.replace("Users", "/Users"))

        # Go through each file within the sub directory.
        for file in sub_folder:

            # Skip system folders.
            if file.find('.') != 0:
                # Print the current folders to console.
                print("\nReading file:", file)

                # Set the file path to the JSON document.
                file_path = sub_folder_root.split("/")
                file_path.append(file)
                file_path = '/'.join(map(str, file_path))

                # Store the current JSON document.
                json_data = get_json_payload(file_path.replace("Users", "/Users"))

                # Convert the data dictionary from the JSON document into DataFrame.
                pd_data = pd.DataFrame(json_data['dataset']['data'])

                # Set the DataFrame column names to dataset keys.
                pd_data.columns = json_data['dataset']['column_names']

                # Add year column.
                pd_data["filename"] = file

                # Add dataset / stock market code column.
                pd_data["code"] = json_data['dataset']['dataset_code']

                # Clean the date format. Convert Date to DateTime.
                pd_data["date"] = pd.to_datetime(pd_data["Date"])

                # Add year column from date.
                pd_data["year"] = pd.DatetimeIndex(pd_data["Date"]).year

                # Rename Columns.
                pd_data.rename(columns={'Date': 'date', 'Open': 'open',
                                       'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume',
                                        'Dividend': 'dividend', 'Split': 'split', 'Adj_Open': 'adj_open',
                                        'Adj_High': 'adj_high', 'Adj_Low': 'adj_low', 'Adj_Close': 'adj_close',
                                        'Adj_Volume': 'adj_volume'}, inplace=True)

                # Change column order.
                pd_data = pd_data[["filename",
                                   "code", "year", "date", "open",
                                    "high", "low", "close", "volume", "dividend", "split",
                                    "adj_open", "adj_high", "adj_low", "adj_close", "adj_volume"]]

                print(pd_data.head())
                print(to_sql(pd_data))
