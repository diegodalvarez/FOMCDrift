# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 00:34:25 2026

@author: Diego
"""

import os
import zipfile
import pandas as pd

class FirstRateFutures:
    
    def __init__(self) -> None: 
        
        self.src_path   = os.getcwd()
        self.repo_path  = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path  = os.path.join(self.repo_path, "data")
        self.guide_path = os.path.join(self.data_path, "Guides")
        
        self.first_rate_path = r"G:\FirstRateData"
        self.columns         = ["date", "open", "high", "low", "close", "volume"]
        
        self.raw_frate_path  = os.path.join(self.data_path, "RawFirstRateData")
        self.zone_frate_path = os.path.join(self.data_path, "ZoneFirstRateData")
        
        if not os.path.exists(self.raw_frate_path):
            os.makedirs(self.raw_frate_path)
            
        if not os.path.exists(self.zone_frate_path):
            os.makedirs(self.zone_frate_path)
            
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

    def get_intraday_futures(self, lookback: str = "1min", verbose: bool = True) -> None: 
        
        if verbose: print("Getting Intraday Futures around FOMC Dates")
        
        guide_path = os.path.join(self.data_path, "Guides")
        spef_path  = os.path.join(guide_path, "VolumeGuide.xlsx")
        
        df_files = (pd
                .read_excel(
                    io        = spef_path,
                    index_col = 0)
                .pivot(index = ["fomc_date", "ticker", "file"], columns = "date_name", values = "date")
                .reset_index()
                .dropna()
                .assign(new_file = lambda x: x.file.str.split("_1day").str[0])
                .rename(columns = {
                    "match_start": "start_date",
                    "match_end"  : "end_date"})
                .loc[lambda x: ~x.new_file.isin(["ZQ_F08", "ZQ_G08"])])
        
        files = df_files.dropna().new_file.drop_duplicates().sort_values().to_list()
        
        df_slicer = (df_files
                [["new_file", "start_date", "end_date", "fomc_date"]]
                .assign(
                    file     = lambda x: x.new_file + "_" + lookback + ".txt",
                    ticker   = lambda x: x.new_file.str.split("_").str[0],
                    contract = lambda x: x.new_file.str.split("_").str[1]))
    
        tickers = df_slicer.ticker.drop_duplicates().sort_values().to_list()
        for ticker in tickers: 
    
            if verbose: print("Working on {} {}".format(lookback, ticker))
            
            out_path = os.path.join(self.raw_frate_path, ticker + "_" + lookback + ".parquet")
            
            if os.path.exists(out_path):
                if verbose: print("Already have {} {}\n".format(lookback, ticker))
                continue
            
            zip_path = os.path.join(self.first_rate_path, "FutData", "fut_contract_" + lookback + "_archive.zip")
            paths    = [file + "_" + lookback + ".txt" for file in files if file.split("_")[0] == ticker]
            
            df_raw = self._read_txt_files(paths, zip_path, self.columns)
            
            df_out = (df_raw
                    .merge(right = df_slicer, how = "inner", on = ["file"])
                    .assign(date = lambda x: pd.to_datetime(x.date))
                    .loc[lambda x: (x.start_date <= x.date) & (x.date <= x.end_date)])
            
            if verbose: print("Saving data\n")
            df_out.to_parquet(path = out_path, engine = "pyarrow")
            
    def _add_times(self, df: pd.DataFrame, df_times: pd.DataFrame) -> pd.DataFrame: 
        
        date = df.date.iloc[0].date()
        
        df_tmp = (df
                .merge(right = df_times, how = "right", on = ["time"])
                .assign(date = lambda x: pd.to_datetime(str(date) + " " + x.time.astype(str)))
                .set_index("time"))
        
        cols         = df_tmp.columns.difference(["value"])
        df_tmp[cols] = df_tmp[cols].ffill().bfill()
        df_out       = df_tmp.reset_index()

        return df_out
            
    def get_trading_zones(self, verbose: bool = True) -> None:
        
        if verbose: print("Getting Trading Zones")
        
        zone_path = os.path.join(self.guide_path, "TradingZones.xlsx")
        df_zones  = pd.read_excel(io = zone_path)
        
        fed_path   = os.path.join(self.guide_path, "FedMeetings.xlsx")
        df_meeting = (pd
                .read_excel(io = fed_path)
                [["date"]]
                .reset_index(drop = True)
                .reset_index()
                .rename(columns = {
                    "index": "meeting_id",
                    "date" : "fomc_date"})
                .assign(meeting_id = lambda x: x.meeting_id + 1))

        files = os.listdir(self.raw_frate_path)
        
        for file in files: 
            for i, idx in df_zones.iterrows():
            
                ticker   = file.split("_")[0]
                idx_dict = idx.to_dict()
                path     = os.path.join(self.raw_frate_path, file)
                
                zone = idx_dict["zone"]
                name = idx_dict["name"]
                dur  = idx_dict["duration"]
                
                if verbose: 
                    print("Working on {} {}".format(ticker, zone))
                
                out_path = os.path.join(self.zone_frate_path, "{}_{}.parquet".format(ticker, zone))
                
                if os.path.exists(out_path):
                    if verbose: print("Already have data\n")
                    continue
    
                start_time   = idx_dict["start_time"]
                end_time     = idx_dict["end_time"]
                meeting_days = idx_dict["days_to_meeting"]
                
                times_path = os.path.join(self.guide_path, "AllTradingTimes.xlsx")
                df_times   = (pd
                        .read_excel(
                            io        = times_path,
                            index_col = 0)
                        .loc[lambda x: x.zone == zone]
                        [["times"]]
                        .rename(columns = {"times": "time"})
                        .assign(time = lambda x: pd.to_datetime(x.time, format = "%H:%M:%S").dt.time))
                
                df_zone_sliced = (pd
                        .read_parquet(path = path, engine = "pyarrow")
                        .loc[lambda x: x.variable.isin(["open", "close"])]
                        .assign(
                            time         = lambda x: x.date.dt.time,
                            match_date   = lambda x: (x.fomc_date - pd.Timedelta(days = meeting_days)).dt.date,
                            compare_date = lambda x: x.date.dt.date)
                        .merge(right = df_meeting, how = "left", on = ["fomc_date"])
                        
                        # match dates and slice times
                        .loc[lambda x: x.match_date == x.compare_date]
                        .loc[lambda x: (start_time <= x.time) & (x.time <= end_time)]
                        .loc[lambda x: x.meeting_id != x.meeting_id.max()]
                        
                        # get rid of the data that we don't need
                        .drop(columns = ["match_date", "compare_date"])
                        
                        # add in the extra information
                        .assign(
                            zone = zone,
                            dur  = dur,
                            name = name))
                
                df_zone_filled = (df_zone_sliced
                        .assign(group_var = lambda x: x.meeting_id.astype(str) + "_" + x.variable)
                        .groupby("group_var")
                        .apply(self._add_times, df_times)
                        .reset_index(drop = True))
                
                df_date_check = (df_zone_filled
                        [["time", "date"]]
                        .assign(tmp_date = lambda x: x.date.dt.time)
                        .loc[lambda x: x.tmp_date != x.time])
                
                if len(df_date_check) == 0:
                    if verbose: print("Filled-In-Dates time matches time")
                    
                else: 
                    if verbose: print("Filled-In-Dates time doesn't match times")
                    
                num_meetings = len(df_zone_filled.meeting_id.drop_duplicates().to_list())
                num_minutes  = int(len(df_times) * num_meetings * 2)
                df_size      = len(df_zone_filled)
                
                if num_minutes == df_size:
                    if verbose: print("Accounted for every minute during zone")
                    
                else: 
                    if verbose: print("Can't account for every minute")
                
                df_check_dup = (df_zone_sliced
                        [["contract", "fomc_date", "variable"]]
                        .drop_duplicates()
                        .groupby(["fomc_date", "variable"])
                        .agg("count")
                        .loc[lambda x: x.contract != 1]
                        .reset_index())
                
                if len(df_check_dup) != 0:
                    if verbose: print("Has duplicate contracts for the same FOMC meeting (Zone {})".format(zone))
                    
                else: 
                    if verbose: print("Doesn't have duplicate contracts for same FOMC meeting (Zone {})".format(zone))
                     
                
                if verbose: print("Saving data\n")    
                df_zone_filled.to_parquet(path = out_path, engine = "pyarrow")


def main() -> None: 
                
    first_rate = FirstRateFutures()
    #first_rate.get_intraday_futures()
    first_rate.get_trading_zones()
    
if __name__ == "__main__": main()