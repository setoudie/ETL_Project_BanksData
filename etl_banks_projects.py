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
TABLE_NAME = ""
COLS_NAME = ["Rank", "Bank_Name", "MC_USD_Billion"]
# Log points status
log_points = ["Start", "Initialize variables", "Data loaded", "Processing step 1", "Processing step 2", "End"]

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


df = extract(URL, COLS_NAME)
log_progress("Call extract() function")
print(df)
