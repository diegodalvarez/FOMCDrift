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
                dropna())
            
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
        
df = FOMCPreprocess().prep_nlp()