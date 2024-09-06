# Importing the required libraries
import pandas as pd
import sqlite3
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import requests

# Declaring known variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_name = "Largest_banks"
log_file = "code_log.txt"
database_name = "Banks.db"
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = "./exchange_rate.csv"
output_path = "./Largest_banks_data.csv"
sql_connection = sqlite3.connect(database_name)

def log_progress(message):
    '''Logs the specified message along with a timestamp to a log file.'''
    time_format = "%Y-%m-%d-%H:%M:%S"
    timestamp = datetime.now().strftime(time_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp} : {message}\n")

log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    '''Extracts the required information from the website and returns it as a DataFrame.'''
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    data_list = []
    
    tables = soup.find_all("table")
    table = tables[0].find("tbody")
    rows = table.find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) >= 3:
            data_dict = {
                "Name": col[1].get_text(strip=True),
                "MC_USD_Billion": float(col[2].get_text(strip=True).replace(',', ''))
            }
            data_list.append(data_dict)

    df = pd.DataFrame(data_list, columns=table_attribs)
    return df

# Extract data
extracted_df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

def transform(extracted_df, csv_path):
    '''Transforms the extracted data to different currencies using exchange rates from a CSV file.'''
    exchange_df = pd.read_csv(csv_path)
    exchange_dict = exchange_df.set_index('Currency')['Rate'].to_dict()
    
    # Perform the conversion and assign it to new columns in the DataFrame
    extracted_df["MC_GBP_Billion"] = extracted_df["MC_USD_Billion"] * exchange_dict["GBP"]
    extracted_df["MC_EUR_Billion"] = extracted_df["MC_USD_Billion"] * exchange_dict["EUR"]
    extracted_df["MC_INR_Billion"] = extracted_df["MC_USD_Billion"] * exchange_dict["INR"]

    # Rounding to 2 decimal places
    extracted_df["MC_GBP_Billion"] = extracted_df["MC_GBP_Billion"].round(2)
    extracted_df["MC_EUR_Billion"] = extracted_df["MC_EUR_Billion"].round(2)
    extracted_df["MC_INR_Billion"] = extracted_df["MC_INR_Billion"].round(2)

    return extracted_df

# Transform data
transformed_data = transform(extracted_df, csv_path)
log_progress("Data transformation complete. Initiating Loading process")

def load_to_csv(transformed_data, output_path):
    ''' This function saves the final data frame as a CSV file in the provided path.'''
    transformed_data.to_csv(output_path, index=False)

load_to_csv(transformed_data, output_path)
log_progress("Data saved to CSV file.")

def load_to_db(transformed_data, sql_connection, table_name):
    ''' This function saves the final data frame to a database table with the provided name.'''
    transformed_data.to_sql(table_name, sql_connection, if_exists="replace", index=False)

load_to_db(transformed_data, sql_connection, table_name)
log_progress("Data saved to SQL database.")

def run_query(query_statement, sql_connection):
    '''Runs the query on the database table and prints the output.'''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Execute queries
query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = "SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

# Close the SQL connection
sql_connection.close()
log_progress("SQL connection closed.")
