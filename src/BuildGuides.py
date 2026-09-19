# -*- coding: utf-8 -*-
"""
Created on Sat Sep 12 23:08:20 2026

@author: Diego
"""

import os
import zipfile
import requests
import pandas as pd

from tqdm import tqdm
from io import StringIO

class FirstRateGuides:
    
    def __init__(self) -> None: 
        
        self.src_path   = os.getcwd()
        self.repo_path  = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path  = os.path.join(self.repo_path, "data")
        self.guide_path = os.path.join(self.data_path, "Guides")
        
        self.first_rate_path = r"G:\FirstRateData"
        self.columns         = ["date", "open", "high", "low", "close", "volume"]
            
        if not os.path.exists(self.guide_path):
            os.makedirs(self.guide_path)
        
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
        
    def _get_max_volume(
            self, 
            df           : pd.DataFrame, 
            df_fomc_dates: pd.DataFrame,
            verbose      : bool = True) -> pd.DataFrame: 
        
        if verbose: print("Selecting dates for {}".format(df.name))
        
        min_max_dict = (df
                [["date"]]
                .agg(["min", "max"])
                .date
                .to_dict())
        
        start_date, end_date = min_max_dict["min"], min_max_dict["max"]
        
        df_dates = (df_fomc_dates
                .loc[lambda x: x.match_start >= start_date]
                .loc[lambda x: x.match_end <= end_date]
                .melt(id_vars = "fomc_date", var_name = "date_name", value_name = "date")
                .assign(date = lambda x: pd.to_datetime(x.date).dt.date))
        
        df_combined = (df
                .merge(right = df_dates, how = "right", on = ["date"]))
        
        df_max = (df_combined
                [["date", "value", "date_name", "fomc_date"]]
                .groupby(["date", "fomc_date", "date_name"])
                .agg("max")
                .reset_index()
                .merge(right = df_combined, how = "inner", on = ["fomc_date", "date_name", "value", "date"]))
    
        df_check_same = (df_max
                .pivot(index = "fomc_date", columns = "date_name", values = "file")
                .loc[lambda x: x.match_end != x.match_start])
        
        if len(df_check_same) == 0: 
            if verbose: print("{} First Date and Last Date Contract Match".format(df.name))
            
        else: 
            if verbose: print("{} First Date and Last Date Contract Don't Match".format(df.name))
            
        return df_max
        
    def get_all_volumes(self, verbose: bool = True) -> None:
        
        if verbose: print("Getting All Volume for All Contracts")
        
        guide_path = os.path.join(self.data_path, "Guides")
        out_path   = os.path.join(guide_path, "AllVolume.parquet")
        
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        daily_zip = os.path.join(self.first_rate_path, "FutData", "fut_contract_1day_archive.zip")
        with zipfile.ZipFile(daily_zip) as z: daily_contracts = z.namelist()
        
        ticker_map_path  = os.path.join(guide_path, "TreasuryFuturesTickers.xlsx")
        df_ticker_mapper = (pd
                .read_excel(io = ticker_map_path)
                [["Ticker", "Bloomberg"]])
        
        tickers    = df_ticker_mapper.Ticker.drop_duplicates().sort_values().to_list()
        spef_files = ([
            file for file in daily_contracts
            if file.split("_")[0] in tickers])
        
        columns   = ["date", "open", "high", "low", "close", "volume", "open_int"]
        df_daily  = self._read_txt_files(spef_files, daily_zip, columns)
        df_volume = (df_daily
                .loc[lambda x: x.variable == "open_int"]
                .loc[lambda x: x.value > 1])
        
        if verbose: print("Saving all Volumes")
        df_volume.to_parquet(path = out_path, engine = "pyarrow")
        
    def get_volume_dates(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting Volume Dates")
        
        guide_path = os.path.join(self.data_path, "Guides")
        fomc_path  = os.path.join(guide_path, "FedMeetings.xlsx")
        vol_path   = os.path.join(guide_path, "AllVolume.parquet")
        out_path   = os.path.join(guide_path, "VolumeGuide.xlsx")
        
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        df_volume = pd.read_parquet(path = vol_path, engine = "pyarrow")
        
        df_fomc_dates = (pd
                .read_excel(io = fomc_path)
                .loc[lambda x: x.meeting_type == "scheduled"]
                [["date"]]
                .rename(columns = {"date": "fomc_date"})
                .assign(
                    match_start = lambda x: x.fomc_date - pd.offsets.BDay(2),
                    match_end   = lambda x: x.fomc_date + pd.offsets.BDay(2))
                .apply(lambda x: pd.to_datetime(x).dt.date))
        
        df_volume_guide = (df_volume
                .assign(
                    date   = lambda x: pd.to_datetime(x.date).dt.date,
                    ticker = lambda x: x.file.str.split("_").str[0])
                .groupby("ticker")
                .apply(self._get_max_volume, df_fomc_dates)
                .reset_index()
                .drop(columns = ["level_1"]))
        
        if verbose: print("Saving data\n")
        
        df_volume_guide.to_excel(out_path)
     
    def get_zone_times(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting all of the trading times")
        
        out_path = os.path.join(self.guide_path, "AllTradingTimes.xlsx")
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        path    = os.path.join(self.guide_path, "TradingZones.xlsx")
        df_zone = pd.read_excel(io = path)
        
        times_list = []
        
        for i, row in df_zone.iterrows():
            
            row_dict   = row.to_dict()
            start_time = row_dict["start_time"]
            end_time   = row_dict["end_time"]
            
            df_joiner = (pd.DataFrame
                    .from_dict(row_dict, orient = "index")
                    .T)
            
            df_add = (pd.DataFrame({
                "times": pd.date_range(
                    start = pd.Timestamp.combine(pd.Timestamp.today(), start_time),
                    end   = pd.Timestamp.combine(pd.Timestamp.today(), end_time),
                    freq  = "1min")})
                .assign(
                    start_time = start_time,
                    times      = lambda x: x.times.dt.time)
                .merge(right = df_joiner, how = "inner", on = ["start_time"]))
            
            times_list.append(df_add)
            
        df_all_times = pd.concat(times_list)
        
        if verbose: print("Saving data\n")
        df_all_times.to_excel(out_path)
        
def main() -> None: 
            
    first_rate = FirstRateGuides()
    #first_rate.get_all_volumes()
    #first_rate.get_volume_dates()
    #first_rate.get_zone_times()
    
if __name__ == "__main__": main()