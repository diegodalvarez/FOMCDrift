# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 04:01:15 2024

@author: Diego
"""

import os
import pandas as pd
from   FOMCDataPrep import DataPrep

class FOMCPreprocess:
    
    def __init__(self) -> None: 
        
        self.repo_path      = os.path.abspath(os.path.join(os.getcwd(), ".."))
        self.data_path      = os.path.join(self.repo_path, "data")
        self.processed_data = os.path.join(self.data_path, "ProcessedData") 
        self.event_path     = r"A:\2025Backup\app_prod\BBGEvent\data\event"

        self.nlp_tickers = ["BENLPFED", "APUSISGF", "APUSSPGF", "APUSTYGF", "APUSXRGF"]
        
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
        
    def prep_nlp(self, verbose: bool = False) -> None: 
        
        file_path = os.path.join(self.processed_data, "ProcessedSentimentData.parquet")
        if os.path.exists(file_path) == False: 
        
            if verbose == True: print("Couldn't find data, collecting it")
            
            read_path = os.path.join(self.raw_path, "SentimentData.parquet")
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
            
        else:
            if verbose: 
                print("NLP Data already prepped")
    
    def prep_mai(self, verbose: bool = False) -> None: 
    
        file_path = os.path.join(self.processed_data, "MAI.parquet")
        
        if os.path.exists(file_path) == False:     
        
            if verbose == True: print("Couldn't find data, collecting it now")
            read_path = os.path.join(self.raw_path, "MAIData.parquet")
            df_out    = (pd.read_parquet(
                path = read_path, engine = "pyarrow").
                assign(
                    date = lambda x: pd.to_datetime(x.date).dt.date,
                    sentiment_source = lambda x: x.variable.str.split("_").str[-1],
                    sentiment_type   = lambda x: x.variable.str.replace("_ni", "").str.replace("_wi", ""),
                    group_var        = lambda x: x.variable + "_" + x.group).
                groupby("group_var").
                apply(self._get_roll_stats, self.window).
                reset_index(drop = True).
                dropna())
            
            if verbose == True: print("saving data")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        else: 
            if verbose: print("MAI Data already prepped")
            
    def get_days(
            self, 
            verbose: bool = True,
            before : int  = -5,
            after  : int  = 2) -> None: 
        
        eq_path = os.path.join(self.data_path, "RawData", "EquityFutures.parquet")
        ff_path = os.path.join(self.event_path, "FDTR.parquet")
        
        date_map_path   = os.path.join(self.processed_data, "DateMap.parquet")
        date_order_path = os.path.join(self.processed_data, "OrderedDates.parquet")
        
        if os.path.exists(date_map_path) == False and os.path.exists(date_order_path) == False:
        
            df_spx = (pd.read_parquet(
                path = eq_path, engine = "pyarrow")
                [["date"]].
                drop_duplicates().
                assign(date = lambda x: pd.to_datetime(x.date).dt.date).
                sort_values("date"))
    
            df_fdtr_dates = (pd.read_parquet(
                path = ff_path, engine = "pyarrow").
                assign(date = lambda x: pd.to_datetime(x.date).dt.date).
                drop(columns = ["security", "ECO_RELEASE_DT"]).
                assign(indicator = True))
            
            min_date = max([df_spx.date.min(), df_fdtr_dates.date.min()])
            
            df_tmp = (df_fdtr_dates.query(
                "date >= @min_date").
                sort_values("date").
                merge(right = df_spx, how = "outer", on = ["date"]).
                sort_values("date").
                drop_duplicates().
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
                
                row_dict   = row.to_dict()
                start_date = row_dict["prev_date"]
                end_date   = row_dict["post_date"]
                
                df_add = (df_spx.query(
                    "@start_date <= date <= @end_date").
                    reset_index(drop = True).
                    reset_index(drop = True).
                    reset_index().
                    rename(columns = {"index": "day"}).
                    assign(
                        day   = lambda x: x.day + before,
                        event = row_dict["event"]))
                
                df_out = pd.concat([df_add, df_out])
            
            df_tmp.to_parquet(path = date_map_path, engine = "pyarrow")
            df_out.to_parquet(path = date_order_path, engine = "pyarrow")        
            
        else: 
            if verbose: print("Dates Processed Already")

    
def main() -> None:
     
    fomc_preprocess = FOMCPreprocess()
    fomc_preprocess.prep_nlp(verbose = True)
    fomc_preprocess.prep_mai(verbose = True)
    fomc_preprocess.get_days()
    
if __name__ == "__main__": main()
