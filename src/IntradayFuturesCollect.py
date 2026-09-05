# -*- coding: utf-8 -*-
"""
Created on Fri Sep  4 12:51:47 2026

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
        
    def download_fed_meetings(self, verbose: bool = True) -> None:
    
        if verbose: print("Getting Fed Meetings")
        
        out_path = os.path.join(self.guide_path, "FedMeetings.xlsx")
        
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        url = "https://en.wikipedia.org/wiki/History_of_Federal_Open_Market_Committee_actions"
    
        headers = {
            "User-Agent": "Mozilla/5.0"}
    
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    
        tables = pd.read_html(StringIO(response.text))
        df_out = tables[1]
        
        if verbose: print("Saving data\n")
        df_out.to_excel(out_path)
        
    def prep_fed_meetings(self, verbose: bool = True) -> None:
        
        if verbose: print("Getting FOMC Announcements")
        
        in_path  = os.path.join(self.guide_path, "FedMeetings.xlsx")
        out_path = os.path.join(self.guide_path, "PrepFedMeetings.xlsx")
        
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        df_out = (pd
                .read_excel(io = in_path, index_col = 0)
                .assign(tmp = lambda x: x.Notes.str.split(" ").str[0])
                .loc[lambda x: x.tmp == "Official"]
                [["Date", "Notes", "Fed. Funds Rate", "Discount Rate"]]
                .assign(date = lambda x: pd.to_datetime(x.Date, format = "%B %d, %Y"))
                .drop(columns = ["Date"])
                .rename(columns = {
                    "Notes"          : "note", 
                    "Fed. Funds Rate": "fed_funds_rate",
                    "Discount Rate"  : "discount_rate"}))
        
        if verbose: print("Saving data\n")
        df_out.to_excel(out_path)
        
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
        out_path = os.path.join(guide_path, "RollDates.xlsx")
        
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
        
    def _get_min_ticker(self, df: pd.DataFrame) -> pd.DataFrame: 
        return df.loc[lambda x: x.days_diff == x.days_diff.min()]
        
    def get_spef_contracts(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting Specific Contracts")
        
        guide_path = os.path.join(self.data_path, "Guides")
        roll_path  = os.path.join(guide_path, "RollDates.xlsx")
        fed_path   = os.path.join(guide_path, "FedMeetings.xlsx")
        out_path   = os.path.join(guide_path, "SpefContracts.xlsx")
        
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
        
    def get_intraday_futures(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting Intraday Futures around FOMC Dates")
        
        guide_path = os.path.join(self.data_path, "Guides")
        spef_path  = os.path.join(guide_path, "SpefContracts.xlsx")
        
        df_files = (pd
                .read_excel(io = spef_path)
                .assign(
                    start_date = lambda x: x.fed_date - pd.Timedelta(days = 1),
                    end_date   = lambda x: x.fed_date + pd.Timedelta(days = 1),
                    new_file   = lambda x: x.file.str.replace("_1day", "_1min"))
                .loc[lambda x: x.new_file != "ZQ_G08_1min.txt"])
        
        tickers  = df_files.ticker.drop_duplicates().sort_values().to_list()
        min_zip  = os.path.join(self.first_rate_path, "FutData", "fut_contract_1min_archive.zip")
        
        for ticker in tickers: 
            
            out_path = os.path.join(self.frate_path, ticker + ".parquet")
            if os.path.exists(out_path):
                if verbose: print("Already have {} Intraday data".format(ticker))
                continue
            
            if verbose: print("Working on getting {}".format(ticker))
            
            df_tmp_files = df_files.loc[lambda x: x.ticker == ticker]
            df_lists     = []
            
            for i, row in tqdm(df_tmp_files.iterrows(), total=len(df_tmp_files)):
                
                row_dict   = row.to_dict()
                file       = row_dict["new_file"]
                start_date = pd.Timestamp(row_dict["start_date"])
                end_date   = pd.Timestamp(row_dict["end_date"])
                full_name  = row_dict["file"].split("_1day")[0].strip()
                fed_date   = row_dict["fed_date"]
                
                ticker, expiry = full_name.split("_")
                
                with zipfile.ZipFile(min_zip) as z:
                    with z.open(file) as f: 
                        
                        df_add = (pd
                              .read_csv(
                                  filepath_or_buffer = f,
                                  header             = None,
                                  names              = self.columns)
                              .assign(prep_date = lambda x: pd.to_datetime(x.date))
                              .loc[lambda x: (start_date <= x.prep_date) & (x.prep_date <= end_date)]
                              .melt(id_vars = ["date", "prep_date"])
                              .assign(
                                  fed_date  = fed_date,
                                  full_name = full_name,
                                  ticker    = ticker,
                                  expiry    = expiry))
                        
                        df_lists.append(df_add)
                    
            if verbose: print("Saving data\n")
            df_combined = pd.concat(df_lists)
            df_combined.to_parquet(path = out_path, engine = "pyarrow")

def main() -> None: 
            
    first_rate = FirstRateFutures()
    first_rate.get_roll_dates()
    first_rate.get_spef_contracts()
    first_rate.get_intraday_futures()
    
if __name__ == "__main__": main()