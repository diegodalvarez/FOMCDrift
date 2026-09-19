# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 01:32:56 2026

@author: Diego
"""

import os
import zipfile
import numpy as np
import pandas as pd

class Impute:
    
    def __init__(self) -> None: 
        
        self.src_path    = os.getcwd()
        self.repo_path   = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path   = os.path.join(self.repo_path, "data")
        self.raw_path    = os.path.join(self.data_path, "RawFirstRateData")
        self.zone_path   = os.path.join(self.data_path, "ZoneFirstRateData")
        self.impute_path = os.path.join(self.data_path, "ZoneImputedFirstRateData")
        
        self.first_rate_path = r"G:\FirstRateData"
        
        if not os.path.exists(self.impute_path):
            os.makedirs(self.impute_path)
            
        self.columns = ["date", "open", "high", "low", "close", "volume"]
    
    def _read_txt_files(self, files: list, folder: str, columns: list) -> pd.DataFrame:
        
        df_list = []
        
        with zipfile.ZipFile(folder) as z:
            for file in files: 
                with z.open(file) as f: 
                    
                    df_add = (pd
                          .read_csv(
                              filepath_or_buffer = f,
                              header             = None,
                              names              = columns)
                          .melt(id_vars = "date")
                          .assign(file = file))
                    
                    df_list.append(df_add)
                    
        df_out = pd.concat(df_list)
        return df_out
    
    def _impute_prior_close(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_meta = (df
                .drop(columns = ["variable", "value"])
                .drop_duplicates())
        
        df_replaced = (df
                .pivot(index = "time", columns = "variable", values = "value")
                .assign(
                    prior_close = lambda x: x.close.shift(),
                    impute_open = lambda x: np.where(x.open != x.open, x.prior_close, x.open),
                    variable    = lambda x: np.where(x.open != x.open, "prior_close", "open")))
        
        df_out = (df_meta
                .merge(right = df_replaced, how = "outer", on = ["time"]))
        
        return df_out
     
    def impute_open_close(self, verbose: bool = True) -> None: 
        
        files = os.listdir(self.zone_path)
        for file in files: 
            
            if verbose: print("Working on {}".format(file.split(".")[0].replace("_", " ")))
            
            in_path  = os.path.join(self.zone_path, file)
            out_path = os.path.join(self.impute_path, file)
            zone     = file.split("_")[-1].split(".")[0]
            
            if os.path.exists(out_path):
                if verbose: print("Already have data\n")
                continue
            
            df_out = (pd
                      .read_parquet(path = in_path, engine = "pyarrow")
                      .groupby("meeting_id")
                      .apply(self._impute_prior_close)
                      .reset_index()
                      .drop(columns = ["level_1"])
                      .assign(variable = lambda x: np.where(x.impute_open != x.impute_open, "missing", x.variable)))
            
            total_meetings = len(df_out.meeting_id.drop_duplicates())
            
            tmp_path   = os.path.join(self.data_path, "Guides", "AllTradingTimes.xlsx")
            df_counter = (pd
                    .read_excel(
                        io        = tmp_path,
                        index_col = 0)
                    [["zone", "times"]]
                    .groupby("zone")
                    .agg("count")
                    .assign(total = lambda x: x.times * total_meetings))
            
            total_count = (df_counter
                    .total
                    .to_dict()
                    [zone])
            
            if len(df_out) == total_count: 
                if verbose: print("Matched minutes out")
                
            else: 
                if verbose: print("Couldn't match source")
            
            if verbose: print("Saving data\n")
            df_out.to_parquet(path = out_path, engine = "pyarrow")
                
if __name__ == "__main__": 
    Impute().impute_open_close()