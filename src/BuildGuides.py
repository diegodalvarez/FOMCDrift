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
    
    def _get_roll_in_date(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_out = (df
                .sort_values(["date", "roll_out_date"])
                .assign(roll_in_date = lambda x: x.roll_out_date.shift()))
        
        return df_out
        
    def get_roll_dates(self, verbose: bool = True) -> None: 
        
        guide_path = os.path.join(self.data_path, "Guides")
        
        if verbose: print("Getting Roll Dates")
        out_path = os.path.join(guide_path, "AllRollDates.xlsx")
        
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
        
        columns  = ["date", "open", "high", "low", "close", "volume", "open_int"]
        df_daily = self._read_txt_files(spef_files, daily_zip, columns)
        
        df_rolls = (df_daily
                .loc[lambda x: x.variable == "close"]
                [["date", "file"]]
                .groupby("file")
                .agg("max")
                .reset_index()
                .assign(
                    date          = lambda x: pd.to_datetime(x.date),
                    roll_out_date = lambda x: x.date - pd.Timedelta(days = 5),
                    ticker        = lambda x: x.file.str.split("_").str[0])
                #.loc[lambda x: x.ticker == x.ticker.min()]
                .groupby("ticker")
                .apply(self._get_roll_in_date)
                .reset_index()
                .drop(columns = ["level_1"]))
        
        if verbose: print("Saving rolls\n")
        df_rolls.to_excel(out_path)
        
    def _get_max_vol(self, df: pd.DataFrame) -> pd.DataFrame: 
        df_out = df.loc[lambda x: x.value == x.value.max()]
        return df_out
        
    def get_volume_dates(self, verbose: bool = True) -> None:
        
        if verbose: print("Getting Weekly Volume Dates")
        
        guide_path = os.path.join(self.data_path, "Guides")
        fomc_path  = os.path.join(guide_path, "FedMeetings.xlsx")
        
        out_path = os.path.join(guide_path, "WeeklyVolumeDates.xlsx")
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        df_fomc_dates = (pd
                .read_excel(io = fomc_path)
                .loc[lambda x: x.meeting_type == "scheduled"]
                .assign(week_date = lambda x: pd.to_datetime(x.date).dt.strftime("%Y-%U"))
                [["date", "week_date"]]
                .rename(columns = {"date": "fomc_date"}))
        
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
                .assign(week_date = lambda x: pd.to_datetime(x.date).dt.strftime("%Y-%U"))
                .drop(columns = ["date", "variable"])
                .merge(right = df_fomc_dates, how = "inner", on = ["week_date"])
                .sort_values("fomc_date")
                .groupby(["week_date", "file", "fomc_date"])
                .agg("sum")
                .reset_index()
                .assign(
                    str_split = lambda x: x.file.str.split("_"),
                    ticker    = lambda x: x.str_split.str[0],
                    contract  = lambda x: x.str_split.str[1])
                .drop(columns = ["str_split"])
                .groupby(["week_date", "ticker"])
                .apply(self._get_max_vol)
                .reset_index()
                .drop(columns = ["level_2"]))
    
        if verbose: print("Saving data\n")
        df_volume.to_excel(out_path)
        
    def _get_min_ticker(self, df: pd.DataFrame) -> pd.DataFrame: 
        return df.loc[lambda x: x.days_diff == x.days_diff.min()]
        
    def get_spef_contracts(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting Specific Contracts")
        
        guide_path = os.path.join(self.data_path, "Guides")
        roll_path  = os.path.join(guide_path, "AllRollDates.xlsx")
        fed_path   = os.path.join(guide_path, "FedMeetings.xlsx")
        out_path   = os.path.join(guide_path, "SpefRollContracts.xlsx")
        
        if os.path.exists(out_path):
            if verbose: print("Already have the data\n")
            return None
        
        df_fed_meetings = (pd
                .read_excel(io = fed_path)
                .sort_values("date")
                .reset_index()
                .rename(columns = {"index": "meeting_id"})
                .assign(date = lambda x: pd.to_datetime(x.date)))
        
        df_rolls = (pd
                .read_excel(io = roll_path, index_col = 0)
                .set_index(["ticker", "file"])
                .apply(lambda x: pd.to_datetime(x).dt.date)
                .reset_index()
                .dropna())
        
        start_roll_date = df_rolls.roll_in_date.min()
        end_roll_date   = df_rolls.roll_in_date.max()
        
        df_fed_sliced = (df_fed_meetings
                .assign(date = lambda x: pd.to_datetime(x.date).dt.date)
                .loc[lambda x: (start_roll_date <= x.date) & (x.date <= end_roll_date)])
        
        df_list = []
        for i, row in df_fed_sliced.iterrows():
            
            row_dict = row.to_dict()
            fed_date = pd.to_datetime(row_dict["date"])
            
            df_add = (df_rolls
                    .set_index(["file", "ticker"])
                    .apply(lambda x: pd.to_datetime(x))
                    .loc[lambda x: x.roll_in_date <= fed_date]
                    .reset_index()
                    .assign(
                        fed_date  = fed_date,
                        ticker    = lambda x: x.file.str.split("_").str[0],
                        days_diff = lambda x: (x.fed_date - x.roll_in_date).dt.days)
                    .groupby("ticker")
                    .apply(self._get_min_ticker)
                    .reset_index()
                    .drop(columns = ["level_1"]))
            
            df_list.append(df_add)
            
        if verbose: print("Saving data\n")
            
        df_contracts = (pd
                .concat(df_list)
                .rename(columns = {"date": "last_trade_date"}))
        
        df_contracts.to_excel(out_path)
        
def main() -> None: 
            
    first_rate = FirstRateGuides()
    #first_rate.get_roll_dates()
    first_rate.get_volume_dates()
    #first_rate.get_spef_contracts()
    
if __name__ == "__main__": main()