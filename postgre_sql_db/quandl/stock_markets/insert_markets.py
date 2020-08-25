import pandas as pd
from time import process_time
from config import config
import sqlalchemy as db

# Import the markets code, name into DataFrom for insertion into database.
pd_markets = pd.read_csv(r'/Users/julianmuscatdoublesin/PycharmProjects/PythonLibrary/postgre_sql_db/quandl/stock_markets/markets.csv', index_col=0)

# Remove index.
pd_markets.reset_index(inplace=True)

# Change column order.
pd_markets = pd_markets[["code", "name"]]


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
    data_frame.to_sql("markets", con, if_exists="append", index=False)

    # Record the end time for the process.
    end_time = process_time()

    # Close the connection to the database.
    con.close()

    # Calculate the time for the entire process.
    elapsed_time = end_time - start_time

    # Return the process time to the user.
    return f"Frame written to table. \nElapsed time: {elapsed_time:0.4f} seconds."


# Push markets DataFrame to database.
print(to_sql(pd_markets))
