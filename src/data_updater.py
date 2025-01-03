#**************************#
import sqlite3
from datetime import datetime
import pandas as pd
from data_operator import data_operator


#**********************************#
# class definition ****************#
#**********************************#

class data_updater:
    def __init__(self, client, db_path, pair):
        # Attributes
        self.client          = client
        self.db_path         = db_path
        self.pair            = pair

        self.data_operator_obj = None  
        self.pair_db_open_time = None
        self.pair_db_end_time  = None

  

    
    
  