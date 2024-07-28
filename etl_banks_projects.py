"""
    Dans ce code nous allons creer un processus (ETL) pour
    1. Extraire des donnees a l'aide de BeautifulSoup sur le Web
    2. Transformer ces donnees en utilusant Pandas
    3. Et enfin les sauvegarder dans une base de donnes
"""
# print("test")

import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3
import numpy as np
from datetime import datetime

# Define the log processs function
''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''


def log_progress(log_point):
    timestamp = datetime.now()
    with open('code_log.txt', 'a') as log_file:
        log_file.write(f"{timestamp} Progress at : {log_point}\n")


log_progress("Declaring known values")

URL = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
COLS_NAME = ["Rank", "Bank_Name", "MC_USD_Billion"]


# Define the extract function
''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''


def extract(url, table_attribs):
    response = requests.get(url)
    df = None

    if response.status_code == 200:
        response_page = response.text
        soup = BeautifulSoup(response_page, "html.parser")

        rows = []

        tables = soup.find_all('table')[1]
        tbodys = tables.find_all('tbody')[0]
        # for
        trs = tbodys.find_all('tr')
        # tds = trs[1].find_all('td')
        # print(tds)
        # print(tr[1:])
        for i in range(1, len(trs[1:]) + 1):
            # print(i)
            tds = trs[i].find_all('td')
            # print(tds, end='\n\n')

            rank = tds[0].contents[0].split('\n')[0]
            bank_name = tds[1].find_all('a')[-1].contents[0]
            total_assets_us_billion = tds[-1].contents[0].split('\n')[0].replace(',', '')

            row = [rank, bank_name, total_assets_us_billion]
            rows.append(row)
            row = []

            # print(rows)
            df = pd.DataFrame(data=rows, columns=table_attribs)
            # print(row, float(row[-1]))
            # break
    else:
        print(f"Status code : {response.status_code}")
    return df


# Define the transform function
""" This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies"""


def transform(df, csv_path):
    exchange_rate_df = pd.read_csv(csv_path)

    dict = exchange_rate_df.set_index('Currency')['Rate'].to_dict()
    print(dict)

    for val, i in zip(df['MC_USD_Billion'], range(len(df['MC_USD_Billion']))):
        # df['MC_USD_Billion'][i] = float(val)
        df.loc[i, "MC_USD_Billion"] = float(val)

    df['MC_GBP_Billion'] = [np.round(x * dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * dict['INR'], 2) for x in df['MC_USD_Billion']]
    # df.to_csv(csv_path)


""" This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""


def load_csv(df, output_csv_path):
    df.to_csv(output_csv_path)


""" This function saves the final data frame to a database
    table with the provided name. Function returns nothing."""


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


log_progress("Call extract() function")
df = extract(URL, COLS_NAME)

log_progress("Call transform()")
transform(df, "exchange_rate.csv")

log_progress("Call load_to_csv()")
load_csv(df, "./Largest_banks_data.csv")

log_progress("Initiate SQLite3 connection	SQL Connection initiated")
sql_conn = sqlite3.connect("Banks.db")

log_progress("Call load_to_db()")
load_to_db(df, sql_conn, TABLE_NAME)

# df.to_csv("Bank_ranking.csv")
print(df)
