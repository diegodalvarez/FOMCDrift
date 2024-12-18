# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 12:41:16 2024

@author: Diego
"""

import os
import sys
import pandas as pd
from   FOMCDataPrep import DataPrep

class EventDrivenSignalGenerator(DataPrep):
    
    def __init__(self):
        
        super().__init__()
        self.fdtr_event = r"C:\Users\Diego\Desktop\app_prod\BBGEvent\data\event"
        if os.path.exists(self.fdtr_event) == False: 
            self.fdtr_event = r"/Users/diegoalvarez/Desktop/BBGEvent/data/event"
        
    def _fill_days(self, df: pd.DataFrame, before: int, after: int, verbose: bool = False) -> pd.DataFrame:
        
        day_count = abs(before) + after + 1
        days      = [i + before for i in range(day_count)]
        try: 
            
            df = df.assign(day = days)
            return df
            
        except: 
            if verbose == True: print("failed at {} {}".format(df.name, df.date.min()))
    
    def get_ordered_window(
            self, 
            df     : pd.DataFrame, 
            before : int = -1, 
            after  : int = 1,
            verbose: bool = False) -> pd.DataFrame: 
        
        df_spx = (self.get_equity_futures().query(
            "security == 'ES1'")
            [["date"]].
            assign(date = lambda x: pd.to_datetime(x.date).dt.date).
            sort_values("date"))
        
        df_fdtr_dates = (pd.read_parquet(
            path   = os.path.join(self.fdtr_event, "FDTR.parquet"),
            engine = "pyarrow").
            assign(date = lambda x: pd.to_datetime(x.date).dt.date).
            drop(columns = ["security", "ECO_RELEASE_DT"]).
            assign(indicator = True))
        
        min_date = max([df_spx.date.min(), df_fdtr_dates.date.min()])
        
        df_tmp = (df_fdtr_dates.query(
            "date >= @min_date").
            sort_values("date").
            merge(right = df_spx.query("date >= @min_date"), how = "outer", on = ["date"]).
            assign(date = lambda x: pd.to_datetime(x.date).dt.date).
            sort_values("date").
            assign(
                prev_date = lambda x: x.date.shift(-before),
                post_date = lambda x: x.date.shift(-after)).
            dropna().
            reset_index(drop = True).
            reset_index().
            rename(columns = {"index": "event"}).
            assign(event = lambda x: x.event + 1))

        df_out = pd.DataFrame()
        for i, row in df_tmp.iterrows():
            
            start_date, end_date = row["prev_date"], row["post_date"]
            df_tmp = (df.query(
                "@start_date <= date <= @end_date").
                assign(date = lambda x: pd.to_datetime(x.date).dt.date).
                groupby("variable").
                apply(self._fill_days, before, after, verbose).
                reset_index(drop = True).
                assign(event = row["event"]))
            
            df_out = pd.concat([df_out, df_tmp])
            
        return df_out