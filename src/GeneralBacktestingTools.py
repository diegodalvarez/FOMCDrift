# -*- coding: utf-8 -*-
"""
Created on Thu Sep 24 08:55:05 2026

@author: Diego
"""

import os
import numpy as np
import pandas as pd
import datetime as dt

class GeneralTools:
    
    def __init__(self) -> None: 
        
        self.src_path = os.getcwd()
        self.repo_path = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path = os.path.join(self.repo_path, "data")
        
        self.gen_path = os.path.join(self.data_path, "GeneralBacktest")
        if not os.path.exists(self.gen_path): os.makedirs(self.gen_path)
        
    def get_full_period_open_pnl(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting Full Period PnL")
        
        out_path = os.path.join(self.gen_path, "FullPeriodPnl.parquet")
        if os.path.exists(out_path):
            if verbose: print("Getting Full Period Open PnL")
            return None
        
        time_path = os.path.join(self.data_path, "Guides", "MeetingTimes.parquet")
        df_times  = pd.read_parquet(path = time_path, engine = "pyarrow")
        
        px_path = os.path.join(self.data_path, "RawFirstRateData")
        df_px   = (pd
                .read_parquet(path = px_path, engine = "pyarrow")
                .loc[lambda x: x.variable == "open"]
                .loc[lambda x: x.ticker != "ZQ"]
                .drop(columns = [
                    "variable", "file", "new_file", 
                    "start_date", "end_date"])
                .rename(columns = {"date": "datetime"})
                .assign(fomc_date = lambda x: x.fomc_date + pd.Timedelta(hours = 12 + 2)))
        
        df_px_diff = (df_px
                .set_index("datetime")
                .groupby(["ticker", "contract", "fomc_date"])
                .apply(lambda x: x.sort_index().value.diff())
                .to_frame(name = "px_diff")
                .reset_index())
        
        df_adj_diff = (df_px_diff
                .set_index("datetime")
                .groupby(["ticker", "contract", "fomc_date"])
                .apply(lambda x: x / x.ewm(span = 10, adjust = False).std())
                .rename(columns = {"px_diff": "px_adj_diff"})
                .reset_index()
                .assign(px_adj_diff = lambda x: np.where(np.abs(x.px_adj_diff) > 50, np.nan, x.px_adj_diff)))
        
        df_out = (df_px
                .merge(right = df_times,    how = "inner", on = ["datetime", "fomc_date"])
                .merge(right = df_px_diff,  how = "inner", on = ["datetime", "fomc_date", "ticker", "contract"])
                .merge(right = df_adj_diff, how = "inner", on = ["datetime", "fomc_date", "ticker", "contract"]))
        
        if verbose: 
            print("Saving data\n")
            df_out.to_parquet(path = out_path, engine = "pyarrow")
        
GeneralTools().get_full_period_open_pnl()