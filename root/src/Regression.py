#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 00:37:12 2024

@author: diegoalvarez
"""
import os
import pandas as pd

from FOMCDataPreprocess import FOMCPreprocess
from EventDrivenSignalGenerator import EventDrivenSignalGenerator

class Regression(FOMCPreprocess, EventDrivenSignalGenerator):
    
    def __init__(self):
        
        super().__init__()
        self.regression_path = os.path.join(self.data_path, "Regression")
        if os.path.exists(self.regression_path) == False: os.makedirs(self.regression_path)
        
        self.days_before = -7
        self.days_after  = 1
        
    def _bbg_sentiment(self) -> pd.DataFrame: 
        
        file_path = os.path.join(self.processed_data, "ProcessedSentimentData.parquet")
        df_out    = (pd.read_parquet(
            path = file_path, engine = "pyarrow").
            drop(columns = [
                "plot_name", "lag_value", "lag_median", "roll_median", 
                "roll_std", "roll_mean", "value", "lag_zscore", "Description"]).
            pivot(index = "date", columns = "security", values = "z_score"))
        
        return df_out
    
    def _mai_sentiment(self) -> pd.DataFrame: 
        
        file_path = os.path.join(self.processed_data, "MAI.parquet")
        variables = ["inflation", "monetary", "unemp", "usd"]
        
        df_out = (pd.read_parquet(
            path = file_path, engine = "pyarrow").
            query("sentiment_type == @variables & group == 'daily_data'")
            [["date", "variable", "z_score"]].
            pivot(index = "date", columns = "variable", values = "z_score"))
        
        return df_out
    
    def _get_forecast(self):
        
        df_out = (self.get_fed_survery_estimate().drop(
            columns = ["num_obs", "raw_result", "result_outcome"]).
            query("variable == 'bn_median'").
            drop(columns = ["variable"]).
            sort_values("date").
            assign(imp_change = lambda x: x.predicted - x.actual.shift())
            [["date", "imp_change"]].
            dropna())
        
        return df_out
        
    def prep_regression(self):
        
        df_fut = (self.get_tsy_futures().rename(
            columns = {"security": "variable"}).
            assign(variable = lambda x: x.variable.str.split(" ").str[0]))
        
        df_window = (self.get_ordered_window(
            df     = df_fut,
            before = self.days_before,
            after  = self.days_after).
            merge(right = self._bbg_sentiment(), how = "inner", on = ["date"]).
            merge(right = self._mai_sentiment(), how = "inner", on = ["date"]))
        
        df_forecast = self._get_forecast()
        
        df_out = (df_window.query(
            "day == 0")
            [["date", "event"]].
            drop_duplicates().
            merge(right = df_forecast, how = "inner", on = ["date"]).
            drop(columns = ["date"]).
            merge(right = df_window, how = "inner", on = ["event"]).
            drop(columns = ["PX_LAST", "PX_diff", "PX_pct", "CTD_DUR", "FUT_CNVX"]))
        
        return df_out
        

df = Regression().prep_regression()