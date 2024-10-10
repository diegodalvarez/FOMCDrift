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
        self.bbg_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\data"
        self.fut_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\PXFront"
        self.deliv_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\BondDeliverableRisk"
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        
        self.tsy_tickers = [
            "DGS1MO", "DGS3MO", "DGS1", "DGS2", "DGS5", "DGS7", "DGS10", 
            "DGS20", "DGS30"]
        
        self.start_date = dt.date(year = 1900, month = 1, day = 1)
        self.end_date = dt.date.today()
        
    def get_tsy_yields(self) -> pd.DataFrame:
        
        path = os.path.join(self.data_path, "TSYFredYields.parquet")
        try:
            
            print("Searching for Treasury data")
            df_out = pd.read_parquet(path = path, engine = "pyarrow")
            print("Found Data\n")
            
        except:
            
            print("Collecting Data")
            df_out = (web.DataReader(
                name = self.tsy_tickers,
                data_source = "fred",
                start = self.start_date,
                end = self.end_date))
            
            df_out.to_parquet(path = path, engine = "pyarrow")
            
        return df_out
    
    def get_labor_sentiment(self) -> pd.DataFrame:
        
        in_path = os.path.join(self.bbg_path, "BENLPFED.parquet")
        read_path = os.path.join(self.data_path, "LaborSentiment.paruqet")
        
        try:
            
            print("Searching for Labor Data")
            df_out = pd.read_parquet(path = read_path, engine = "pyarrow")
            print("Found Data\n")
            
        except: 
        
            print("Collecting Data")    
            df_out = (pd.read_parquet(
                path = in_path, engine = "pyarrow").
                assign(
                    date = lambda x: pd.to_datetime(x.date).dt.date,
                    security = lambda x: x.security.str.split(" ").str[0]).
                drop(columns = ["variable"]).
                pivot(index = "date", columns = "security", values = "value"))
            
            df_out.to_parquet(path = read_path, engine = "pyarrow")
        
        return df_out
    
    def get_fut_data(self) -> pd.DataFrame:
        
        read_path = os.path.join(self.data_path, "FuturesData.parquet")
        
        try:
            
            print("Searching for Futures Data")
            df_out = pd.read_parquet(path = read_path, engine = "pyarrow")
            print("Found Data\n")
            
        except: 
        
            print("Collecting Data")
            tickers = ["TU", "TY", "US", "FV", "UXY", "WN", "ES", "UX"]
            paths = [os.path.join(
                self.fut_path, ticker + ".parquet") 
                for ticker in tickers]
            
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                assign(date = lambda x: pd.to_datetime(x.date)))
        
            df_out.to_parquet(path = read_path, engine = "pyarrow")
        
        return df_out
    
    def get_bond_deliverables(self) -> pd.DataFrame: 
        
        read_path = os.path.join(
            self.data_path, "TreasuryDeliverables.parquet")
        
        try:
            
            print("Searching for Bond Future Deliverables")
            df_out = pd.read_parquet(path = read_path, engine = "pyarrow")
            print("Found Data\n")
            
        except: 
        
            print("Collecting Data")
            tickers = ["TU", "TY", "US", "FV", "UXY", "WN"]
            paths = [os.path.join(
                self.deliv_path, ticker + ".parquet")
                for ticker in tickers]
            
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                assign(date = lambda x: pd.to_datetime(x.date)))
            
            df_out.to_parquet(path = read_path, engine = "pyarrow")
        
        return df_out
    
    def get_fedfunds_data(self) -> pd.DataFrame: 
        
        read_path = os.path.join(self.data_path, "FedFundsRate.parquet")
        try:
            
            print("Searching for Fed Funds Future Data")
            df_out = pd.read_parquet(path = read_path, engine = "pyarrow")
            print("Found Data\n")
            
        except:
            
            print("Collecting Data")
            path = os.path.join(self.bbg_path, "FDTR.parquet")
            df_out = (pd.read_parquet(
                path = path, engine = "pyarrow").
                assign(
                    date = lambda x: pd.to_datetime(x.date).dt.date,
                    security = lambda x: x.security.str.split(" ").str[0]).
                drop(columns = ["variable"]))
            
            df_out.to_parquet(path = read_path, engine = "pyarrow")
            
        return df_out
        
        
                
def main():
    
    data_prep = DataPrep()
    
    data_prep.get_tsy_yields()
    data_prep.get_labor_sentiment()
    data_prep.get_fut_data()
    data_prep.get_bond_deliverables()
    data_prep.get_fedfunds_data()
    
#if __name__ == "__main__": main()

