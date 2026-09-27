# -*- coding: utf-8 -*-
"""
Created on Thu Sep 24 08:55:05 2026

@author: Diego
"""

import os
import zipfile
import numpy as np
import pandas as pd


class GeneralTools:
    
    def __init__(self) -> None: 
        
        self.src_path   = os.getcwd()
        self.repo_path  = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path  = os.path.join(self.repo_path, "data")
        self.guide_path = os.path.join(self.data_path, "Guides")

        
        self.gen_path        = os.path.join(self.data_path, "GeneralBacktest")
        self.adj_frate_path  = os.path.join(self.data_path, "AdjustedData")
        self.first_rate_path = r"G:\FirstRateData"
        
        if not os.path.exists(self.gen_path): 
            os.makedirs(self.gen_path)
        
        if not os.path.exists(self.adj_frate_path):
            os.makedirs(self.adj_frate_path)
            
        self.columns = ["date", "open", "high", "low", "close", "volume"]
            
    def _read_open_txt_files(self, files: list, folder: str, columns: list) -> pd.DataFrame:
        
        df_list = []
        
        with zipfile.ZipFile(folder) as z:
            for file in files: 
                with z.open(file) as f: 
                    
                    df_add = (pd
                          .read_csv(
                              filepath_or_buffer = f,
                              header             = None,
                              names              = columns)
                          [["open", "date"]]
                          .assign(file = file))
                    
                    df_list.append(df_add)
                    
        df_out = pd.concat(df_list)
        return df_out
        
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
            
    def get_minutely_adj_data(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting Minutely Roll Adjusted Sharpe")
        
        out_path = os.path.join(self.adj_frate_path, "TotalPeriodSharpe.parquet")
        if os.path.exists(out_path):
            if verbose: print("Already have the data\n")
            return None
        
        px_path = os.path.join(
            self.first_rate_path, 
            "FutData", 
            "fut_1min_contin_adj_ratio.zip")
        
        tsy_path    = os.path.join(self.guide_path, "TreasuryFuturesTickers.xlsx")
        all_tickers = (pd
                .read_excel(io = tsy_path)
                .Ticker
                .drop_duplicates()
                .sort_values()
                .to_list())
        
        tickers = [ticker for ticker in all_tickers if ticker != "ZQ"]
        paths   = ["{}_full_1min_continuous_ratio_adjusted.txt".format(ticker) for ticker in tickers]
        
        df_tmp = (self
                  ._read_open_txt_files(paths, px_path, self.columns)
                  .set_index("date")
                  .groupby("file")
                  .apply(lambda x: x.sort_index().open.diff())
                  .reset_index()
                  .drop(columns = ["date"])
                  .groupby("file")
                  .agg(["mean", "std"])
                  ["open"])
        
        if verbose: print("Saving data\n")
        df_tmp.to_parquet(path = out_path, engine = "pyarrow")
        
def main() -> None: 
        
    general_tools = GeneralTools()
    #general_tools.get_full_period_open_pnl()
    general_tools.get_minutely_adj_data()
    
if __name__ == "__main__": main()