# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 00:34:25 2026

@author: Diego
"""

import os
import zipfile
import requests
import pandas as pd

from tqdm import tqdm
from io import StringIO

class FirstRateFutures:
    
    def __init__(self) -> None: 
        
        self.src_path   = os.getcwd()
        self.repo_path  = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path  = os.path.join(self.repo_path, "data")
        self.guide_path = os.path.join(self.data_path, "Guides")
        self.frate_path = os.path.join(self.data_path, "FirstRateData")
        
        self.first_rate_path = r"G:\FirstRateData"
        self.columns         = ["date", "open", "high", "low", "close", "volume"]
        
        if not os.path.exists(self.frate_path):
            os.makedirs(self.frate_path)
            
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

    def get_intraday_futures(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting Intraday Futures around FOMC Dates")
        
        guide_path = os.path.join(self.data_path, "Guides")
        spef_path  = os.path.join(guide_path, "WeeklyVolumeDates.xlsx")
        
        df_files = (pd
                .read_excel(
                    io        = spef_path,
                    index_col = 0)
                .assign(
                    start_date = lambda x: x.fomc_date - pd.Timedelta(days = 1),
                    end_date   = lambda x: x.fomc_date + pd.Timedelta(days = 2),
                    new_file   = lambda x: x.file.str.split("_1day").str[0])
                .loc[lambda x: x.value != 0]
                .loc[lambda x: ~x.new_file.isin(["ZQ_F08", "ZQ_G08"])])
        
        lookbacks = ["1hour", "1min", "5min", "30min"]
        files     = df_files.new_file.drop_duplicates().sort_values().to_list()
        
        for lookback in lookbacks: 
            
            df_slicer = (df_files
                    [["new_file", "start_date", "end_date", "fomc_date"]]
                    .assign(
                        file     = lambda x: x.new_file + "_" + lookback + ".txt",
                        ticker   = lambda x: x.new_file.str.split("_").str[0],
                        contract = lambda x: x.new_file.str.split("_").str[1]))
        
            tickers = df_slicer.ticker.drop_duplicates().sort_values().to_list()
            for ticker in tickers: 
        
                if verbose: print("Working on {} {}".format(lookback, ticker))
                
                out_path = os.path.join(self.frate_path, ticker + "_" + lookback + ".parquet")
                if os.path.exists(out_path):
                    if verbose: print("Already have {} {}\n".format(lookback, ticker))
                    continue
                
                zip_path = os.path.join(self.first_rate_path, "FutData", "fut_contract_" + lookback + "_archive.zip")
                paths    = [file + "_" + lookback + ".txt" for file in files if file.split("_")[0] == ticker]
                df_raw   = self._read_txt_files(paths, zip_path, self.columns)
                
                df_out = (df_raw
                        .merge(right = df_slicer, how = "inner", on = ["file"])
                        .assign(date = lambda x: pd.to_datetime(x.date))
                        .loc[lambda x: (x.start_date <= x.date) & (x.date <= x.end_date)])
                
                if verbose: print("Saving data\n")
                df_out.to_parquet(path = out_path, engine = "pyarrow")
            
FirstRateFutures().get_intraday_futures()
