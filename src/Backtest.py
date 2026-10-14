# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 22:50:32 2026

@author: Diego
"""

import os
import pickle
import numpy as np
import pandas as pd

class Backtester:
    
    def __init__(self) -> None: 
        
        self.src_path     = os.getcwd()
        self.repo_path    = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path    = os.path.join(self.repo_path, "data")
        self.is_back_path = os.path.join(self.data_path, "InSampleBacktest")
        
        if not os.path.exists(self.is_back_path):
            os.makedirs(self.is_back_path)
            
        self.bad_futures = ["ZQ"]
            
    def vol_adj_backtest_insample(self, verbose: bool = True) -> None: 
        
        out_path = os.path.join(self.is_back_path, "InSampleBacktest.parquet")
        if os.path.exists(out_path): 
            if verbose: print("Already have data\n")
            return None
        
        if verbose: print("Getting Vol Targetted PnL")
        
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
        
        df_unmatched = (df_vol_weighting
                [["meeting_id", "ticker", "zone", "vol_type"]]
                .drop_duplicates()
                .groupby(["ticker", "zone", "vol_type"])
                ["meeting_id"]
                .agg(["min", "max", "count"])
                .add_suffix("_val")
                .assign(
                    exp_val  = lambda x: x.max_val - x.min_val,
                    diff_val = lambda x: x.exp_val - x.count_val)
                .reset_index()
                .sort_values("diff_val", ascending = False)
                .loc[lambda x: x.diff_val != 0])

        if len(df_unmatched) > 0: 
            if verbose: 
            
                print("There are some unmatched data points {} occurances and {} occurances >= 4".format(
                    len(df_unmatched),
                    len(df_unmatched.loc[lambda x: x.diff_val >= 4])))
                
                print(df_unmatched)
                
        else: 
            if verbose: print("Passed Vol Weighting Quality Check")
    
        df_vol_target = (df_pnl
            .assign(meeting_id = lambda x: x.meeting_id.astype(int))
            .merge(right = df_vol_weighting, how = "inner", on = ["ticker", "meeting_id", "zone", "name"])
            .assign(vol_rtn = lambda x: x.weight * x.impute_open))
        
        # since there are holes in the original data when we check for holes
        # we have to account for that
        
        df_vol_event_check = (df_vol_target
                [["ticker", "meeting_id", "zone"]]
                .drop_duplicates()
                .groupby(["ticker", "zone"])
                .agg(["min", "max", "count"])
                ["meeting_id"]
                .add_suffix("_val")
                .apply(lambda x: x.astype(int))
                .assign(exp_count = lambda x: x.max_val - x.min_val)
                [["count_val", "exp_count"]]
                .rename(columns = {
                    "count_val": "from_target_count",
                    "exp_count": "from_target_exp"})
                .reset_index())
        
        path          = os.path.join(self.data_path, "DataQuality", "ImputeDataQuality.parquet")
        df_qual_check = (pd
                .read_parquet(path = path, engine = "pyarrow")
                .reset_index(drop = True)
                [["count_count", "exp_count", "file"]]
                .assign(
                    count_count = lambda x: x.count_count.astype(int),
                    exp_count   = lambda x: x.exp_count.astype(int))
                .rename(columns = {
                    "count_count": "from_qual_count",
                    "exp_count"  : "from_qual_exp"})
                .assign(
                    ticker = lambda x: x.file.str.split("_").str[0],
                    zone   = lambda x: x.file.str.split("_").str[1].str.split(".").str[0])
                .drop(columns = ["file"])
                .loc[lambda x: ~x.ticker.isin(self.bad_futures)])
        
        df_qual_check = (df_vol_event_check
                .merge(right = df_qual_check, how = "outer", on = ["ticker", "zone"])
                .loc[lambda x: x.from_qual_count != x.from_target_count]
                [["ticker", "zone", "from_qual_count", "from_target_count"]]
                .assign(diff_val = lambda x: x.from_qual_count - x.from_target_count)
                .loc[lambda x: x.diff_val != 0])
        
        if len(df_qual_check) > 0:
            if verbose:
                print("There are {} occurances and {} occurances >= 4".format(
                    len(df_qual_check),
                    len(df_qual_check.loc[lambda x: x.diff_val >= 4])))
        
                print(df_qual_check)
                
                
        else: 
            if verbose: print("Passed Output Quality Check")
        
        df_pnl_count = (df_pnl.
                dropna()
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
            
        if verbose: print("Saving data\n")
        df_vol_target.to_parquet(path = out_path, engine = "pyarrow")

    def vol_adj_backtest_outsample(self, verbose: bool = True) -> None:

        # Out-of-sample version of vol_adj_backtest_insample. Coefficients
        # come from VolEstimators.get_expanding_ols_params, where the model
        # stored under meeting t was fit on meetings up to and including t.
        # Here they are lagged one meeting so meeting t is sized with the
        # latest model fit strictly before t, applied to t's own regressors.

        oos_back_path = os.path.join(self.data_path, "OutSampleBacktest")
        if not os.path.exists(oos_back_path):
            os.makedirs(oos_back_path)

        out_path = os.path.join(oos_back_path, "OutSampleBacktest.parquet")
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None

        if verbose: print("Getting Out-of-Sample Vol Targetted PnL")

        path    = os.path.join(self.data_path, "ZoneImputedFirstRateData")
        df_zone = (pd
            .read_parquet(path = path)
            .loc[lambda x: x.ticker != "ZQ"]
            .set_index("date")
            [["meeting_id", "ticker", "contract", "impute_open", "zone", "name"]])

        df_pnl = (df_zone
            .groupby(["meeting_id", "ticker", "contract", "zone", "name"])
            .apply(lambda x: x.sort_index().impute_open.diff())
            .reset_index())

        # regressors for each meeting (known before that meeting's zone opens)
        path   = os.path.join(self.data_path, "Volatilities", "CombinedVolEstimators.parquet")
        df_reg = (pd
            .read_parquet(path = path, engine = "pyarrow")
            [["ticker", "name", "zone", "meeting_id", "fomc_date", "lday_vol", "lzone_vol"]]
            .assign(meeting_id = lambda x: x.meeting_id.astype(int)))

        # expanding coefficients in wide format, one row per model per end meeting
        path      = os.path.join(self.data_path, "Volatilities", "ExpandingVolOLSParams.parquet")
        df_params = (pd
            .read_parquet(path = path, engine = "pyarrow")
            .assign(meeting_id = lambda x: x.meeting_id.astype(int))
            .pivot_table(
                index   = ["ticker", "name", "zone", "vol_type", "meeting_id", "n_obs"],
                columns = "index",
                values  = "param")
            .rename_axis(columns = None)
            .rename(columns = {
                "const"    : "b_const",
                "lday_vol" : "b_lday",
                "lzone_vol": "b_lzone"})
            .reset_index())

        for col in ["b_lday", "b_lzone"]:
            if col not in df_params.columns: df_params[col] = np.nan

        # every meeting x vol_type, with that meeting's own model (if any) attached
        group_cols = ["ticker", "name", "zone", "vol_type"]
        lag_cols   = ["b_const", "b_lday", "b_lzone", "n_obs", "model_meeting_id", "model_fomc_date"]
        vol_types  = pd.DataFrame({"vol_type": df_params.vol_type.drop_duplicates().to_list()})

        df_lagged = (df_reg
            .merge(right = vol_types, how = "cross")
            .merge(right = df_params, how = "left", on = group_cols + ["meeting_id"])
            .sort_values(group_cols + ["fomc_date", "meeting_id"])
            .reset_index(drop = True)
            .assign(
                model_meeting_id = lambda x: x.meeting_id.where(x.b_const.notna()),
                model_fomc_date  = lambda x: x.fomc_date.where(x.b_const.notna())))

        # lag one meeting, then carry forward the latest model so a meeting
        # whose predecessor had no model (dropped row / burn-in) uses the
        # most recent model before it
        df_lagged[lag_cols] = (df_lagged
            .groupby(group_cols)
            [lag_cols]
            .transform(lambda s: s.shift().ffill()))

        uses_lday  = df_lagged.vol_type.isin(["lday", "combined"])
        uses_lzone = df_lagged.vol_type.isin(["lzone", "combined"])

        df_vol_weighting = (df_lagged
            .assign(
                vol_est = lambda x:
                    x.b_const
                    + np.where(uses_lday , x.b_lday  * x.lday_vol , 0)
                    + np.where(uses_lzone, x.b_lzone * x.lzone_vol, 0))
            .dropna(subset = ["vol_est"])
            .assign(
                ann_vol = lambda x: x.vol_est * np.sqrt(390),
                weight  = lambda x: np.where(x.vol_est > 0, 0.01 / x.ann_vol, np.nan))
            [["meeting_id", "ticker", "name", "zone", "vol_type", "vol_est",
              "ann_vol", "weight", "n_obs", "model_meeting_id", "model_fomc_date", "fomc_date"]])

        # look-ahead check: the model used must be fit before the meeting traded
        df_bad_lag = (df_vol_weighting
            .loc[lambda x: x.model_fomc_date >= x.fomc_date])

        if len(df_bad_lag) > 0:
            if verbose:
                print("Look-ahead: {} rows use a model fit on or after the meeting".format(len(df_bad_lag)))
                print(df_bad_lag)

        else:
            if verbose: print("Passed Look-Ahead Check")

        # non-positive vol forecasts get no position (weight = NaN)
        df_nonpos = (df_vol_weighting
            .loc[lambda x: x.vol_est <= 0]
            .groupby(["ticker", "zone", "vol_type"])
            .size())

        if len(df_nonpos) > 0:
            if verbose:
                print("There are {} non-positive vol forecasts (not traded)".format(df_nonpos.sum()))
                print(df_nonpos)

        else:
            if verbose: print("Passed Non-Positive Vol Check")

        # meetings lost to burn-in / no prior model
        if verbose:
            df_burn = (df_lagged
                .loc[lambda x: x.b_const.isna()]
                .groupby(["ticker", "zone", "vol_type"])
                .size())

            print("Meetings without an out-of-sample model (burn-in), min {} max {}".format(
                df_burn.min() if len(df_burn) > 0 else 0,
                df_burn.max() if len(df_burn) > 0 else 0))

        df_vol_target = (df_pnl
            .assign(meeting_id = lambda x: x.meeting_id.astype(int))
            .merge(
                right = df_vol_weighting.drop(columns = ["fomc_date", "model_fomc_date"]),
                how   = "inner",
                on    = ["ticker", "meeting_id", "zone", "name"])
            .assign(vol_rtn = lambda x: x.weight * x.impute_open))

        # every traded meeting should keep all of its pnl bars for each vol_type
        df_pnl_count = (df_pnl
                .dropna()
                .assign(meeting_id = lambda x: x.meeting_id.astype(int))
                [["ticker", "contract", "meeting_id", "name", "impute_open"]]
                .groupby(["ticker", "contract", "meeting_id", "name"])
                .agg("count")
                .reset_index()
                .rename(columns = {"impute_open": "from_pnl"}))

        df_vol_count = (df_vol_target
                .dropna(subset = ["impute_open"])
                [["ticker", "contract", "meeting_id", "name", "impute_open", "vol_type"]]
                .groupby(["ticker", "contract", "meeting_id", "name", "vol_type"])
                .agg("count")
                .reset_index()
                .rename(columns = {"impute_open": "from_vol"}))

        df_check = (df_vol_count
                .merge(right = df_pnl_count, how = "left", on = ["ticker", "contract", "meeting_id", "name"])
                .loc[lambda x: x.from_pnl != x.from_vol])

        if len(df_check) > 0:
            if verbose: print("There is a problem with {} meetings occuring at\n{}".format(
                    len(df_check),
                    df_check[["ticker", "contract", "meeting_id", "name", "vol_type"]]))

        else:
            if verbose: print("Passed Meeting Count Test")

        if verbose: print("Saving data\n")
        df_vol_target.to_parquet(path = out_path, engine = "pyarrow")
 
def main() -> None:       
 
    backtester = Backtester()
    backtester.vol_adj_backtest_insample()
    backtester.vol_adj_backtest_outsample()
    
if __name__ == "__main__": main()