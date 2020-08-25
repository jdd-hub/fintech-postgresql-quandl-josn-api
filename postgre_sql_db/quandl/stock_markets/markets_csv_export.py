# Import pandas as pd
import pandas as pd

# The list of codes required by the API.
lst_code = ["DIS", "MSFT", "INTC", "IBM", "AAPL", "MMM", "PFE", "JNJ", "PG", "NKE"]

# List of names as a reference to each code.
lst_names = ["The Walt Disney Company (DIS)",
             "Microsoft Corporation (MSFT)",
             "Intel Corporation (INTC)",
             "International Business Machines Corporation (IBM)",
             "Apple Inc. (AAPL)",
             "3M Company (MMM)",
             "Pfizer inc. (PFE)",
             "Johnson & Johnson (JNJ)",
             "Procter & Gamble Company (PG)",
             "Nike Inc. (NKE)"]

# Converts both lists into on data dictionary.
dict_markets = {
    "name": lst_names,
    "code": lst_code
}

# Converts the data dictionary to a DataFrame.
pd_markets = pd.DataFrame(dict_markets)

# Remove index.
pd_markets.reset_index(inplace=True)

# Write DataFrame to individual CVS File. This DataFrame is for an single company.
pd_markets.to_csv(
    r'/Users/julianmuscatdoublesin/PycharmProjects/PythonLibrary/postgre_sql_db/quandl/stock_markets/markets.csv',
    index=False, header=True)