ETL Pipeline for Extracting Largest Banks Data
This project extracts data about the largest banks from Wikipedia, transforms the extracted data by applying currency conversion, and loads it into a database as well as a CSV file. The pipeline also supports querying the data from an SQLite database for further analysis.

Project Overview
The ETL pipeline is structured as follows:

Extract:
Retrieves a table of the largest banks from a Wikipedia page using web scraping techniques with BeautifulSoup.
Extracts relevant columns such as the bank's name and market capitalization (in USD).

Transform:
Reads exchange rates from a CSV file and applies these rates to convert the market capitalization into GBP, EUR, and INR.
Adds new columns for the converted values and rounds them to two decimal places.

Load:
Saves the transformed data as a CSV file for local storage.
Inserts the transformed data into an SQLite database, enabling SQL-based queries for analysis.
Project Files
Banks.db: SQLite database containing the bank data after the ETL process.
Largest_banks_data.csv: CSV file with the transformed data.
exchange_rate.csv: CSV file containing currency exchange rates for conversion.
code_log.txt: Log file recording progress and any issues encountered during the ETL process.
ETL_pipeline.py: Python script that performs the full ETL process.
