#**************************#
import sqlite3
from datetime import datetime
import pandas as pd
from DataOperator import DataOperator


#**********************************#
# class definition ****************#
#**********************************#

class DataUpdater:
    def __init__(self, client, db_path):
        # Attributes
        self.client          = client
        self.db_path         = db_path
        self.dataOperatorObj = None  

        self.pair_db_open_time = None
        self.pair_db_end_time  = None

    def get_db_times(self, pair):
        table_name = f"{pair}_RAW_DATA"
        conn       = sqlite3.connect(self.db_path)
        cursor     = conn.cursor()

        # Ellenőrizzük, hogy létezik-e a tábla
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]

        if row_count > 0:
            # Ha van adat, akkor lekérdezzük az első és utolsó sor időpontját
            cursor.execute(f"SELECT MIN(open_time), MAX(open_time) FROM {table_name}")
            result                 = cursor.fetchone()
            self.pair_db_open_time = result[0]
            self.pair_db_end_time  = result[1]
        else:
            # Ha nincs adat, akkor alapértelmezett értékeket állítunk be
            self.pair_db_open_time = '2024-11-08 14:00'
            self.pair_db_end_time  = '2024-11-08 15:00'

        conn.close()


    def download_coin_data(self, pair, interval):
        # DataOperator objektum létrehozása és tárolása az attribútumban
        self.dataOperatorObj = DataOperator(
            client          = self.client,
            pair            = pair,
            interval        = interval,
            start_open_time = self.pair_db_open_time,
            end_open_time   = self.pair_db_end_time
        )
        # download data
        self.dataOperatorObj.fetch_data()


    #Insert data from raw_data to the SQLite database."""
    def insert_raw_data_to_db(self):
        
        if self.dataOperatorObj.raw_data.empty:
            print("No data to insert.")
            return

        # Connect to the database
        table_name = f"{self.dataOperatorObj.pair}_RAW_DATA"
        conn   = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Prepare the insert statement with placeholders for each column
        insert_query = f'''
        INSERT OR IGNORE INTO {table_name} (open_time, open_date_time, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        '''

        # Iterate over raw_data and insert each row
        for _, row in self.dataOperatorObj.raw_data.iterrows():
            # Get the data from each row
            open_time       = int(row['open_time'])
            open_date_time  = row['open_date_time']
            open_price      = float(row['open'])
            high_price      = float(row['high'])
            low_price       = float(row['low'])
            close_price     = float(row['close'])
            volume          = float(row['volume'])
            
            # Execute the insert statement
            cursor.execute(insert_query, (open_time, open_date_time, open_price, high_price, low_price, close_price, volume))
        
        # Commit and close the connection
        conn.commit()
        conn.close()
        print("Data inserted successfully.")



    def get_pair_summary(self):
        """Retrieve summary for the specified trading pair."""
        # Connect to the database
        table_name = f"{self.dataOperatorObj.pair}_RAW_DATA"
        conn       = sqlite3.connect(self.db_path)
        cursor     = conn.cursor()

        # Define the parameterized query
        query = f'''
        SELECT COUNT(*), MIN(open_date_time), MAX(open_date_time)
        FROM {table_name}
        '''

        # Execute the query
        cursor.execute(query)
        result = cursor.fetchone()

        # Close the connection
        conn.close()

        # Return the result as a dictionary for easy access
        return {
            "count": result[0],
            "min_open_date_time": result[1],
            "max_open_date_time": result[2]
        }




    
    
  