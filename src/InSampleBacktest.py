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
            
    def vol_adj_backtest(self, verbose: bool = True) -> None: 
        
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
        
        '''
        display(df_pnl
                [["meeting_id", "ticker"]]
                .drop_duplicates()
                .groupby("ticker")
                ["meeting_id"]
                .agg(["min", "max", "count"])
                .add_suffix("_event")
                .assign(compare_val = lambda x: x.max_event - x.min_event)
                .apply(lambda x: x.astype(int))
                .loc[lambda x: x.count_event != x.compare_val])
        return-1
        '''
        
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
        
        display(df_vol_weighting
                .loc[lambda x: x.ticker == "TN"]
                .loc[lambda x: x.vol_type == x.vol_type.min()]
                .loc[lambda x: x.zone == x.zone.min()]
                .drop(columns = ["vol_type", "zone"])
                .sort_values("meeting_id")
                .loc[lambda x: x.meeting_id > 185])
        return-1
    
        df_vol_target = (df_pnl
            .assign(meeting_id = lambda x: x.meeting_id.astype(int))
            .merge(right = df_vol_weighting, how = "inner", on = ["ticker", "meeting_id", "zone", "name"])
            .assign(vol_rtn = lambda x: x.weight * x.impute_open))
        
        '''
        df_output = (df_vol_target
                [["meeting_id", "ticker", "contract", "zone", "name"]]
                .assign(from_output = 1))
        '''
        
        df_pnl_count = (df_pnl
                .dropna()
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
        return-1
        
InSampleBacktest().vol_adj_backtest()