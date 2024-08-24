# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 07:02:30 2024

@author: Diego
"""

import os
import pandas as pd
import datetime as dt
import pandas_datareader as web

class DataPrep:
    
    def __init__(self):
        
        self.parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.data_path = os.path.join(self.parent_path, "data")
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        
        self.tsy_tickers = [
            "DGS1MO", "DGS3MO", "DGS1", "DGS2", "DGS5", "DGS7", "DGS10", 
            "DGS20", "DGS30"]
        
        self.start_date = dt.date(year = 1900, month = 1, day = 1)
        self.end_date = dt.date.today()
        
    def get_tsy_yields(self) -> pd.DataFrame:
        
        path = os.path.join(self.data_path, "TSYFredYields.parquet")
        try:
            
            print("Searching for data")
            df_out = pd.read_parquet(path = path, engine = "pyarrow")
            print("Found Data")
            
        except:
            
            print("Collecting Data")
            df_out = (web.DataReader(
                name = self.tsy_tickers,
                data_source = "fred",
                start = self.start_date,
                end = self.end_date))
            
            df_out.to_parquet(path = path, engine = "pyarrow")
            
        return df_out