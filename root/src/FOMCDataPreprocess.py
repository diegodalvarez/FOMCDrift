# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 04:01:15 2024

@author: Diego
"""

import os
import pandas as pd
from   FOMCDataPrep import DataPrep

class FOMCPreprocess(DataPrep):
    
    def __init__(self) -> None: 
        
        super().__init__()
        self.processed_data = (
            os.path.join(os.getcwd().split("\\root")[0], "data", "ProcessedData"))
        
        if os.path.exists(self.processed_data) == False: os.makedirs(self.processed_data)
        self.nlp_tickers = ["BENLPFED", "APUSISGF", "APUSSPGF", "APUSTYGF", "APUSXRGF"]
        self.window      = 10
        
    def _get_roll_stats(self, df: pd.DataFrame, window: int) -> pd.DataFrame: 
        
        df_out = (df.sort_values(
            "date").
            assign(
                roll_mean   = lambda x: x.value.ewm(span = window, adjust = False).mean(),
                roll_std    = lambda x: x.value.ewm(span = window, adjust = False).std(),
                z_score     = lambda x: (x.value - x.roll_mean) / x.roll_std,
                lag_zscore  = lambda x: x.z_score.shift(),
                roll_median = lambda x: x.value.rolling(window = window).median(),
                lag_median  = lambda x: x.roll_median.shift(),
                lag_value   = lambda x: x.value.shift()))
        
        return df_out
        
    def prep_nlp(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.processed_data, "ProcessedSentimentData.parquet")
        try:
            
            if verbose == True: print("Trying to find prepped data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it")
            
            read_path = os.path.join(self.data_path, "SentimentData.parquet")
            df_out    = (pd.read_parquet(
                path = read_path, engine = "pyarrow").
                query("security == @self.nlp_tickers").
                groupby("security").
                apply(self._get_roll_stats, self.window).
                reset_index(drop = True).
                dropna().
                assign(plot_name = lambda x: (x.Description.str.split(
                    "cs").
                    str[1].
                    str.split("-").str[0].
                    str.split("of").str[-1].
                    str.split("Nat").str[0].
                    str.replace("Reserve", "Reserve\n").
                    str.replace("Year", "Year\n").
                    str.replace("Exchange", "Exchange\n").
                    str.replace("year", "year\n"))))
            
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def prep_mai_data(self, verbose: bool = False) -> None: 
    
        sheet_names = ["Daily Data", "Monthly Data"]
        for sheet_name in sheet_names: 
            
            file_path = os.path.join(self.processed_data, sheet_name.split(" ")[0] + "MAI.parquet")
            try:
                
                if verbose == True: print("Trying to prep MAI Data")
                df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
                if verbose == True: print("Found data\n")
                
            except: 
                
                if verbose == True: print("Couldn't find data, collecting it now")
                
                mai_path  = os.path.join(self.data_path, "Fisher_Martineau_Sheng_MAI Data.xlsx")
                df_out    = (pd.read_excel(
                    io = mai_path, sheet_name = sheet_name).
                    assign(date = lambda x: pd.to_datetime(x.date).dt.date).
                    melt(id_vars = "date").
                    dropna().
                    assign(
                        sentiment_source = lambda x: x.variable.str.split("_").str[-1],
                        sentiment_type   = lambda x: x.variable.str.replace("_ni", "").str.replace("_wi", "")))
                
                if verbose == True: print("Saving data\n")
                df_out.to_parquet(path = file_path, engine = "pyarrow")

    
    
def main() -> None:
        
    FOMCPreprocess().prep_nlp(verbose = True)
    FOMCPreprocess().prep_mai_data(verbose = True)
    
if __name__ == "__main__": main()