# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 22:50:32 2026

@author: Diego
"""

import os
import pickle
import numpy as np
import pandas as pd

class InSampleBacktest:
    
    def __init__(self) -> None: 
        
        self.src_path     = os.getcwd()
        self.repo_path    = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path    = os.path.join(self.repo_path, "data")
        self.is_back_path = os.path.join(self.data_path, "InSampleBacktest")
        
        if not os.path.exists(self.is_back_path):
            os.makedirs(self.is_back_path)
            
        self.bad_futures = ["ZQ"]
            
    def vol_adj_backtest(self, verbose: bool = True) -> None: 
        
        out_path = os.path.join(self.is_back_path, "InSampleBacktest.parquet")
        if os.path.exists(out_path): 
            if verbose: print("Already have data\n")
            return None
        
        if verbose: print("Getting Vol Targetted PnL")
        
        path    = os.path.join(self.data_path, "", r"ZoneImputedFirstRateData")
        df_zone = (pd
            .read_parquet(path = path)
            .loc[lambda x: x.ticker != "ZQ"]
            .set_index("date")
            [["meeting_id", "ticker", "contract", "impute_open", "zone", "name"]])
        
        df_pnl = (df_zone
            .groupby(["meeting_id", "ticker", "contract", "zone", "name"])
            .apply(lambda x: x.sort_index().impute_open.diff())
            .reset_index())
        
        model_path = os.path.join(self.data_path, "Volatilities", "CombinedVolEstimators.pkl")
        with open(model_path, "rb") as f: models = pickle.load(f)
        
        df_lists = []
        for model_name in models.keys():
        
            ticker, name, zone, vol_type = model_name.split(" ")
            
            df_add = (models
                [model_name]
                .fittedvalues
                .to_frame(name = "vol_est")
                .reset_index()
                .rename(columns = {"index": "meeting_id"})
                .assign(
                    ticker   = ticker,
                    name     = name,
                    zone     = zone,
                    vol_type = vol_type))
            
            df_lists.append(df_add)
            
        df_vol_weighting = (pd
            .concat(df_lists)
            .assign(
                ann_vol = lambda x: x.vol_est * np.sqrt(390),
                weight  = lambda x: 0.01 / x.ann_vol))
        
        df_unmatched = (df_vol_weighting
                [["meeting_id", "ticker", "zone", "vol_type"]]
                .drop_duplicates()
                .groupby(["ticker", "zone", "vol_type"])
                ["meeting_id"]
                .agg(["min", "max", "count"])
                .add_suffix("_val")
                .assign(
                    exp_val  = lambda x: x.max_val - x.min_val,
                    diff_val = lambda x: x.exp_val - x.count_val)
                .reset_index()
                .sort_values("diff_val", ascending = False)
                .loc[lambda x: x.diff_val != 0])

        if len(df_unmatched) > 0: 
            if verbose: 
            
                print("There are some unmatched data points {} occurances and {} occurances >= 4".format(
                    len(df_unmatched),
                    len(df_unmatched.loc[lambda x: x.diff_val >= 4])))
                
                print(df_unmatched)
                
        else: 
            if verbose: print("Passed Vol Weighting Quality Check")
    
        df_vol_target = (df_pnl
            .assign(meeting_id = lambda x: x.meeting_id.astype(int))
            .merge(right = df_vol_weighting, how = "inner", on = ["ticker", "meeting_id", "zone", "name"])
            .assign(vol_rtn = lambda x: x.weight * x.impute_open))
        
        # since there are holes in the original data when we check for holes
        # we have to account for that
        
        df_vol_event_check = (df_vol_target
                [["ticker", "meeting_id", "zone"]]
                .drop_duplicates()
                .groupby(["ticker", "zone"])
                .agg(["min", "max", "count"])
                ["meeting_id"]
                .add_suffix("_val")
                .apply(lambda x: x.astype(int))
                .assign(exp_count = lambda x: x.max_val - x.min_val)
                [["count_val", "exp_count"]]
                .rename(columns = {
                    "count_val": "from_target_count",
                    "exp_count": "from_target_exp"})
                .reset_index())
        
        path          = os.path.join(self.data_path, "DataQuality", "ImputeDataQuality.parquet")
        df_qual_check = (pd
                .read_parquet(path = path, engine = "pyarrow")
                .reset_index(drop = True)
                [["count_count", "exp_count", "file"]]
                .assign(
                    count_count = lambda x: x.count_count.astype(int),
                    exp_count   = lambda x: x.exp_count.astype(int))
                .rename(columns = {
                    "count_count": "from_qual_count",
                    "exp_count"  : "from_qual_exp"})
                .assign(
                    ticker = lambda x: x.file.str.split("_").str[0],
                    zone   = lambda x: x.file.str.split("_").str[1].str.split(".").str[0])
                .drop(columns = ["file"])
                .loc[lambda x: ~x.ticker.isin(self.bad_futures)])
        
        df_qual_check = (df_vol_event_check
                .merge(right = df_qual_check, how = "outer", on = ["ticker", "zone"])
                .loc[lambda x: x.from_qual_count != x.from_target_count]
                [["ticker", "zone", "from_qual_count", "from_target_count"]]
                .assign(diff_val = lambda x: x.from_qual_count - x.from_target_count)
                .loc[lambda x: x.diff_val != 0])
        
        if len(df_qual_check) > 0:
            if verbose:
                print("There are {} occurances and {} occurances >= 4".format(
                    len(df_qual_check),
                    len(df_qual_check.loc[lambda x: x.diff_val >= 4])))
        
                print(df_qual_check)
                
                
        else: 
            if verbose: print("Passed Output Quality Check")
        
        df_pnl_count = (df_pnl.
                dropna()
                [["ticker", "contract", "meeting_id", "name", "impute_open"]]
                .groupby(["ticker", "contract", "meeting_id", "name"])
                .agg("count")
                .reset_index()
                .rename(columns = {"impute_open": "from_pnl"}))
        
        df_vol_count = (df_vol_target
                .dropna()
                [["ticker", "contract", "meeting_id", "name", "impute_open", "vol_type"]]
                .groupby(["ticker", "contract", "meeting_id", "name", "vol_type"])
                .agg("count")
                .reset_index()
                .rename(columns = {"impute_open": "from_vol"}))
        
        df_check = (df_pnl_count
                .merge(right = df_vol_count, how = "outer", on = ["ticker", "contract", "meeting_id", "name"])
                .loc[lambda x: x.from_pnl != x.from_vol])
        
        if len(df_check) > 0: 
            if verbose: print("There is a problem with {} meetings occuring at\n{}".format(
                    len(df_check),
                    df_check[["ticker", "contract", "meeting_id", "name"]]))
            
            
        else: 
            if verbose: print("Passed Meeting Count Test")
            
        if verbose: print("Saving data\n")
        df_vol_target.to_parquet(path = out_path, engine = "pyarrow")
        
InSampleBacktest().vol_adj_backtest()