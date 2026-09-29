# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 08:01:37 2026

@author: Diego
"""

import os
import zipfile
import pandas as pd
import datetime as dt

from tqdm import tqdm

class VolEstimators:
    
    def __init__(self) -> None: 
        
        self.src_path  = os.getcwd()
        self.repo_path = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path = os.path.join(self.repo_path, "data")
        self.vol_path  = os.path.join(self.data_path, "Volatilities")
        
        if not os.path.exists(self.vol_path):
            os.makedirs(self.vol_path)
            
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
            
    def get_prior_day_vol1(self) -> None: 
        
        times_path   = os.path.join(self.data_path, "Guides", "AllTradingTimes.xlsx")
        df_all_times = pd.read_excel(io = times_path, index_col = 0)
        
        zone_path      = os.path.join(self.data_path, "ZoneImputedFirstRateData")
        df_times_guide = (pd
                .read_parquet(path = zone_path, engine = "pyarrow")
                .drop(columns = ["impute_open", "prior_close", "variable", "open", "close"])
                .drop(columns = ["name"]))
        
        df_time_looper = (df_times_guide
                .drop(columns = ["time", "date"])
                .drop_duplicates())
        
        first_rate_path = os.path.join(self.data_path, "RawFirstRateData")
        stds_list       = []
        
        for i, row in tqdm(df_time_looper.iterrows(), total = len(df_time_looper)):
            
            row_dict   = row.to_dict()
            ticker     = row_dict["ticker"]
            contract   = row_dict["contract"]
            meeting_id = row_dict["meeting_id"]
            zone       = row_dict["zone"]
            
            date_tmp = (df_times_guide
                    .loc[lambda x: x.ticker == ticker]
                    .loc[lambda x: x.contract == contract]
                    .loc[lambda x: x.meeting_id == meeting_id]
                    .loc[lambda x: x.zone == zone]
                    .assign(date_dt = lambda x: x.date.dt.date))
            
            date = (date_tmp
                    .date_dt
                    .drop_duplicates()
                    .item())
            
            match_date = date - pd.Timedelta(days = 1)
            
            match_datetimes = (df_all_times
                    .loc[lambda x: x.zone == zone]
                    [["times"]]
                    .assign(
                        match_date     = match_date,
                        match_datetime = lambda x: pd.to_datetime(
                            x.match_date.astype(str) + 
                            " " + 
                            x.times.astype(str)))
                    .match_datetime
                    .to_list())

            
            path = os.path.join(first_rate_path, ticker + "_1min.parquet")
            diff = (pd
                    .read_parquet(path = path)
                    .loc[lambda x: x.variable == "open"]
                    .loc[lambda x: x.date.isin(match_datetimes)]
                    .set_index("date")
                    [["value"]]
                    .diff()
                    .dropna())
            
            tmp_dict = {
                "prior_date_start": diff.index.min(),
                "prior_date_end"  : diff.index.max(),
                "zone_date_start" : date_tmp.date.min(),
                "zone_date_end"   : date_tmp.date.max(),
                "n"               : len(diff),
                "std"             : diff.std().item()}
            
            add_dict = {**row_dict, **tmp_dict}
            stds_list.append(add_dict)
        
        display(pd.DataFrame(stds_list))
        
    def get_prior_day_vol(self, verbose: bool = True) -> None:
        
        if verbose: 
            print("Getting Prior Day Volatility")
        
        out_path = os.path.join(self.vol_path, "PriorDayVolatility.parquet")
        
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
    
        times_path = os.path.join(
            self.data_path,
            "Guides",
            "AllTradingTimes.xlsx")
    
        df_all_times = pd.read_excel(
            io        = times_path,
            index_col = 0)
    
        zone_path = os.path.join(
            self.data_path,
            "ZoneImputedFirstRateData")
    
        df_times_guide = (pd
                          .read_parquet(
                              path   = zone_path,
                              engine = "pyarrow")
                          .drop(columns =
                                ["impute_open", "prior_close", "variable",
                                 "open", "close", "name"]))
    
        df_time_looper = (df_times_guide
            .drop(columns=["time", "date"])
            .drop_duplicates())
    
        first_rate_path = os.path.join(
            self.data_path,
            "RawFirstRateData")
    
        # ---------------------------------------------------------
        # Precompute date for each unique combination
        # ---------------------------------------------------------
    
        date_lookup = (
            df_times_guide
            .assign(date_dt=lambda x: x["date"].dt.date)
            .groupby(
                ["ticker", "contract", "meeting_id", "zone"],
                sort=False
            )
            .agg(
                date=("date_dt", "first"),
                zone_date_start=("date", "min"),
                zone_date_end=("date", "max")
            )
            .reset_index()
        )
    
        # ---------------------------------------------------------
        # Precompute trading times by zone
        # ---------------------------------------------------------
    
        zone_times = (
            df_all_times
            .groupby("zone")["times"]
            .apply(list)
            .to_dict()
        )
    
        # ---------------------------------------------------------
        # Load each ticker parquet ONCE
        # ---------------------------------------------------------
    
        tickers = df_time_looper["ticker"].unique()
    
        ticker_data = {}
    
        for ticker in tqdm(tickers, desc="Loading tickers"):
    
            path = os.path.join(
                first_rate_path,
                f"{ticker}_1min.parquet"
            )
    
            ticker_data[ticker] = (
                pd.read_parquet(path, engine="pyarrow")
                .loc[lambda x: x["variable"].eq("open")]
                .set_index("date")["value"]
                .sort_index()
            )
    
        # ---------------------------------------------------------
        # Loop
        # ---------------------------------------------------------
    
        stds_list = []
    
        for row in tqdm(
            df_time_looper.itertuples(index=False),
            total=len(df_time_looper)
        ):
    
            ticker = row.ticker
            contract = row.contract
            meeting_id = row.meeting_id
            zone = row.zone
    
            tmp = date_lookup.loc[
                (date_lookup["ticker"] == ticker)
                & (date_lookup["contract"] == contract)
                & (date_lookup["meeting_id"] == meeting_id)
                & (date_lookup["zone"] == zone)
            ].iloc[0]
    
            date = tmp["date"]
    
            match_date = date - pd.Timedelta(days=1)
    
            match_datetimes = pd.to_datetime(
                [
                    f"{match_date} {time}"
                    for time in zone_times[zone]
                ]
            )
    
            values = ticker_data[ticker].reindex(match_datetimes).dropna()
    
            diff = values.diff().dropna()
    
            tmp_dict = {
                "prior_date_start": diff.index.min(),
                "prior_date_end": diff.index.max(),
                "zone_date_start": tmp["zone_date_start"],
                "zone_date_end": tmp["zone_date_end"],
                "n": len(diff),
                "std": diff.std()
            }
    
            row_dict = {
                "ticker": ticker,
                "contract": contract,
                "meeting_id": meeting_id,
                "zone": zone
            }
    
            stds_list.append({
                **row_dict,
                **tmp_dict
            })
    
        df_out = pd.DataFrame(stds_list)
        if verbose: print("Saving data\n")
        
        df_out.to_parquet(path = out_path, engine = "pyarrow")
        
    def get_zone_vol(self, verbose: bool = True) -> None: 
        
        if verbose: 
            print("Getting Zone Volatility")
        
        out_path = os.path.join(self.vol_path, "ZoneVolatility.parquet")
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        path   = os.path.join(self.data_path, "ZoneImputedFirstRateData")
        
        display(pd
                .read_parquet(path = path, engine = "pyarrow")
                .set_index("date"))
        return-1
        
        df_out = (pd
                .read_parquet(path = path, engine = "pyarrow")
                .set_index("date")
                [[
                    "zone", "ticker","contract", "name", 
                    "impute_open", "fomc_date"]]
                .groupby(["zone", "ticker", "contract", "name", "fomc_date"])
                .apply(lambda x: x.sort_index().impute_open.diff())
                .to_frame(name = "tmp")
                .reset_index()
                .drop(columns = ["date"])
                .groupby(["zone", "ticker", "contract", "name", "fomc_date"])
                .agg(["count", "std"])
                ["tmp"]
                .reset_index())
        
        if verbose: print("Saving data\n")
        df_out.to_parquet(path = out_path, engine = "pyarrow")
        
    def get_vol_ols_models(self) -> None: 
        
        zone_vol_path = os.path.join(self.vol_path, "ZoneVolatility.parquet")
        df_zone_vol   = pd.read_parquet(path = zone_vol_path, engine = "pyarrow")
        
        display(df_zone_vol
                .set_index("fomc_date"))
        return-1
        
        df_lag_vol = (df_zone_vol
                .set_index("fomc_date")
                .groupby(["zone", "ticker", "name"])
                .apply(lambda x: x.shift())
                .add_prefix("lag_")
                .reset_index()
                .dropna())

def main() -> None: 
        
    vol_estimators = VolEstimators()
    #vol_estimators.get_prior_day_vol()
    #vol_estimators.get_zone_vol()
    #vol_estimators.get_vol_ols_models()
    
if __name__ == "__main__": main()