# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 08:01:37 2026

@author: Diego
"""

import os
import pickle
import zipfile
import numpy as np
import pandas as pd
import datetime as dt
import statsmodels.api as sm

from tqdm import tqdm

class VolEstimators:
    
    def __init__(self) -> None: 
        
        self.src_path  = os.getcwd()
        self.repo_path = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path = os.path.join(self.repo_path, "data")
        self.vol_path  = os.path.join(self.data_path, "Volatilities")
        
        if not os.path.exists(self.vol_path):
            os.makedirs(self.vol_path)
            
        self.exclude_tickers = ["ZQ"]
            
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
    
    def _get_date(self) -> pd.DataFrame: 
        
        path   = os.path.join(self.data_path, "Guides", "AllVolume.parquet")
        df_out = (pd
                .read_parquet(path = path, engine = "pyarrow")
                [["date"]]
                .drop_duplicates()
                .assign(date = lambda x: pd.to_datetime(x.date))
                .sort_values("date"))
        
        return df_out
    
    def get_prior_day_vol(self, verbose: bool = True) -> None:
        
        dates = (pd.DatetimeIndex(
            self._get_date()["date"]
            .drop_duplicates()
            .sort_values()))
        
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
                                 "open", "close", "name"])
                          .loc[lambda x: ~x.ticker.isin(self.exclude_tickers)])
    
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
    
            ticker     = row.ticker
            contract   = row.contract
            meeting_id = row.meeting_id
            zone       = row.zone
    
            tmp = date_lookup.loc[
                (date_lookup["ticker"] == ticker)
                & (date_lookup["contract"] == contract)
                & (date_lookup["meeting_id"] == meeting_id)
                & (date_lookup["zone"] == zone)
            ].iloc[0]
    
            date       = tmp["date"]
            idx        = dates.searchsorted(pd.Timestamp(date), side = "left") - 1
            match_date = dates[idx] if idx > 0 else pd.NaT
            
            match_datetimes = pd.DatetimeIndex(
                [
                    pd.Timestamp(match_date) + pd.Timedelta(time)
                    for time in zone_times[zone]
                ]
            )
            
            values          = ticker_data[ticker].reindex(match_datetimes).dropna()
            diff            = values.diff().dropna()
    
            tmp_dict = {
                "prior_date_start" : diff.index.min(),
                "prior_date_end"   : diff.index.max(),
                "zone_date_start"  : tmp["zone_date_start"],
                "zone_date_end"    : tmp["zone_date_end"],
                "n"                : len(diff),
                "std"              : diff.std()}
    
            row_dict = {
                "ticker"     : ticker,
                "contract"   : contract,
                "meeting_id" : meeting_id,
                "zone"       : zone}
    
            stds_list.append({
                **row_dict,
                **tmp_dict})
            
        df_out = pd.DataFrame(stds_list)
        
        df_bad1 = (df_out
            .loc[lambda x: x.prior_date_end < x.prior_date_start])
    
        if len(df_bad1) > 0: 
            if verbose: 
                print("There is an incorrect date for the prior data")
                print(df_bad1)
                
        else: 
            if verbose: print("Passed prior date check")
            
        df_bad2 = (df_out
                   .loc[lambda x: x.zone_date_end <= x.zone_date_start])
        
        if len(df_bad2) > 0: 
            if verbose: 
                print("There is an incorrect date for the zone data")
                print(df_bad2)
                
        else: 
            if verbose: print("Passed Zone date check")
            
        df_orig_count = (df_times_guide
            [["meeting_id", "ticker", "zone"]]
            .drop_duplicates()
            .groupby(["zone", "ticker"])
            .agg("count")
            .reset_index()
            .rename(columns = {"meeting_id": "orig_count"}))
        
        df_out_count = (df_out
            [["ticker", "zone", "meeting_id"]]
            .groupby(["ticker", "zone"])
            .agg("count")
            .reset_index()
            .rename(columns = {"meeting_id": "from_out"}))
        
        df_count_match = (df_orig_count
            .merge(right = df_out_count, how = "outer", on = ["zone", "ticker"])
            .loc[lambda x: x.orig_count != x.from_out])
        
        if len(df_count_match) > 0: 
            if verbose: 
                print("Missing an FOMC event for the following ticker and zone")
                print(df_count_match)
                
        else: 
            if verbose: print("Matched all events and zones")
        
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
        df_out = (pd
                .read_parquet(path = path, engine = "pyarrow")
                .set_index("date")
                [[
                    "zone", "ticker","contract", "name", 
                    "impute_open", "fomc_date"]]
                .loc[lambda x: ~x.ticker.isin(self.exclude_tickers)]
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
        
    def _lag(self, df: pd.DataFrame) -> pd.DataFrame:
    
        df_out = (df
            .set_index("fomc_date")
            .sort_index()
            .shift()
            .add_prefix("lag_"))
    
        return df_out
        
    def combine_vols(self, tol: int = 3, verbose: bool = True) -> None: 
        
        if verbose: 
            print("Getting Combined Vols")
        
        zone_path  = os.path.join(self.data_path, "Volatilities", "ZoneVolatility.parquet")
        prior_path = os.path.join(self.data_path, "Volatilities", "PriorDayVolatility.parquet")
        meet_path  = os.path.join(self.data_path, "Guides", "MeetingTimes.parquet")
        out_path   = os.path.join(self.data_path, "Volatilities", "CombinedVolEstimators.parquet")
        
        if os.path.exists(out_path):
            if verbose: print("Saving Combined Volatility Dataset")
        
        df_fomc_guide = (pd
                .read_parquet(path = meet_path, engine = "pyarrow")
                [["fomc_date", "meeting_id"]]
                .drop_duplicates()
                .assign(meeting_id = lambda x: x.meeting_id.astype(int)))
        
        df_zone = (pd
                   .read_parquet(path = zone_path, engine = "pyarrow")
                   .assign(fomc_date = lambda x: x.fomc_date + pd.Timedelta(hours = 12 + 2)))
        
        df_prior = (pd
                    .read_parquet(path = prior_path, engine = "pyarrow")
                    .drop(columns = ["n"])
                    .rename(columns = {"std": "lday_vol"})
                    .assign(meeting_id = lambda x: x.meeting_id.astype(int)))
        
        df_prior_zone = (df_zone
            .drop(columns = ["count"])
            .groupby(["ticker", "zone", "name"])
            .apply(self._lag)
            .reset_index()
            .dropna()
            .rename(columns = {"lag_std": "lzone_vol"}))
        
        df_out = (df_prior_zone
                .merge(right = df_zone      , how = "inner", on = ["ticker", "zone", "name", "fomc_date"])
                .merge(right = df_fomc_guide, how = "inner", on = ["fomc_date"])
                .merge(right = df_prior     , how = "inner", on = ["ticker", "meeting_id", "zone", "contract"])
                .rename(columns = {"std": "cur_vol"}))
        
        df_check = (df_out
                [["ticker", "zone", "meeting_id"]]
                .groupby(["ticker", "zone"])
                .agg("count")
                .reset_index()
                .rename(columns = {"meeting_id": "from_out"}))
        
        df_orig = (df_zone
                [["ticker", "zone", "count"]]
                .groupby(["ticker", "zone"])
                .agg("count")
                .reset_index()
                .rename(columns = {"count": "from_orig"}))
        
        df_check_out = (df_check
                .merge(right = df_orig, how = "outer", on = ["ticker", "zone"])
                .assign(diff_val = lambda x: x.from_orig - x.from_out)
                .loc[lambda x: np.abs(x.diff_val) > tol])
        
        if len(df_check_out) > 0:
            if verbose:
                print("There is some data being destroyed at these places")
                print(df_check_out)
        
        if verbose: 
            print("Saving data\n")
            
        df_out.to_parquet(path = out_path, engine = "pyarrow")
        
    def full_sample_ols_models(self, verbose: bool = True) -> None: 
        
        if verbose: 
            print("Getting Full Sample OLS Models")
        
        out_path = os.path.join(self.vol_path, "CombinedVolEstimators.pkl")
        
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        path   = os.path.join(self.vol_path, "CombinedVolEstimators.parquet")
        df_raw = (pd
                .read_parquet(path = path, engine = "pyarrow")
                [["ticker", "name", "zone", "lday_vol", "lzone_vol", "cur_vol", "meeting_id"]]
                .assign(group_var = lambda x: x.ticker + " " + x.name + " " + x.zone))
        
        '''
        flag0
        display(df_raw
                [["ticker", "zone", "meeting_id"]]
                .drop_duplicates()
                .groupby(["ticker", "zone"])
                ["meeting_id"]
                .agg(["min", "max", "count"])
                .add_suffix("_val")
                .assign(
                    exp_val  = lambda x: x.max_val - x.min_val,
                    diff_val = lambda x: x.exp_val - x.count_val))
        return-1
        '''
        
        models     = {}
        group_vars = df_raw.group_var.drop_duplicates().sort_values().to_list()
        
        if verbose: iter_ = tqdm(group_vars)
        else      : iter_ = group_vars
        
        for group_var in iter_: 
            
            df_tmp = (df_raw
                    .loc[lambda x: x.group_var == group_var]
                    .drop(columns = ["group_var"])
                    .dropna()
                    .set_index("meeting_id"))
            
            lday_model = (sm
                    .OLS(
                        endog = df_tmp.cur_vol,
                        exog  = sm.add_constant(df_tmp.lday_vol))
                    .fit())
            
            lzone_model = (sm
                           .OLS(
                               endog = df_tmp.cur_vol,
                               exog  = sm.add_constant(df_tmp.lzone_vol))
                           .fit())
            
            combined_model = (sm
                              .OLS(
                                  endog = df_tmp.cur_vol,
                                  exog  = sm.add_constant(df_tmp[["lzone_vol", "lday_vol"]]))
                              .fit())
            
            models[group_var + " lday"]     = lday_model
            models[group_var + " lzone"]    = lzone_model
            models[group_var + " combined"] = combined_model
    
        if verbose: print("Saving data\n")
        with open(out_path, "wb") as f: pickle.dump(models, f)
        
    def get_full_sample_ols_params(self, verbose: bool = True) -> None: 
        
        if verbose: print("Getting Full Sample OLS Params")
        
        out_path = os.path.join(self.vol_path, "CombinedVolOLSParams.parquet")
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        model_path = os.path.join(self.vol_path, "CombinedVolEstimators.pkl")
        with open(model_path, "rb") as f: models = pickle.load(f)
        
        df_lists = []

        for model_name in models.keys():
            
            tmp_model                    = models[model_name]
            ticker, name, zone, vol_type = model_name.split(" ")
            
            df_param  = tmp_model.params.to_frame(name = "param").reset_index()
            df_pvalue = tmp_model.pvalues.to_frame(name = "pvalue").reset_index()
            df_tvalue = tmp_model.tvalues.to_frame(name = "tvalue").reset_index()
            
            df_add = (df_param
                    .merge(right = df_pvalue, how = "inner", on = ["index"])
                    .merge(right = df_tvalue, how = "inner", on = ["index"])
                    .assign(
                        name   = model_name,
                        r2     = tmp_model.rsquared,
                        adj_r2 = tmp_model.rsquared_adj)
                    .assign(
                        ticker   = ticker,
                        name     = name,
                        zone     = zone,
                        vol_type = vol_type))
            
            df_lists.append(df_add)
            
        df_out = pd.concat(df_lists)

        if verbose: print("Saving data\n")
        df_out.to_parquet(path = out_path, engine = "pyarrow")

    # ---------------------------------------------------------
    # Expanding (out-of-sample) OLS volatility models
    # ---------------------------------------------------------
    # For each meeting t the model is fit on every meeting up to and
    # including t and stored under t. The one-meeting lag is applied in
    # the backtest: meeting t+1 is sized with the coefficients stored
    # under t applied to t+1's own regressors.

    def _expanding_ols_group(self, df_tmp: pd.DataFrame, x_cols: list, min_obs: int = 2) -> dict:

        import warnings

        df_tmp  = df_tmp.sort_values(["fomc_date", "meeting_id"]).reset_index(drop = True)
        min_fit = max(min_obs, len(x_cols) + 1) # need at least as many obs as params
        models  = {}

        for i in range(min_fit, len(df_tmp) + 1):

            df_train   = df_tmp.iloc[:i].set_index("meeting_id")
            meeting_id = df_train.index[-1]

            # small samples give exact fits (df_resid = 0) which raise
            # divide by zero warnings for the test statistics
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                model = (sm
                    .OLS(
                        endog = df_train.cur_vol,
                        exog  = sm.add_constant(df_train[x_cols], has_constant = "add"))
                    .fit())

            models[meeting_id] = model

        return models

    def expanding_ols_models(self, min_obs: int = 2, verbose: bool = True) -> None:

        if verbose:
            print("Getting Expanding OLS Models")

        out_path = os.path.join(self.vol_path, "ExpandingVolEstimators.pkl")

        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None

        path   = os.path.join(self.vol_path, "CombinedVolEstimators.parquet")
        df_raw = (pd
                .read_parquet(path = path, engine = "pyarrow")
                [["ticker", "name", "zone", "lday_vol", "lzone_vol", "cur_vol", "meeting_id", "fomc_date"]]
                .assign(group_var = lambda x: x.ticker + " " + x.name + " " + x.zone))

        vol_specs = {
            "lday"    : ["lday_vol"],
            "lzone"   : ["lzone_vol"],
            "combined": ["lzone_vol", "lday_vol"]}

        models     = {}
        check_list = []
        group_vars = df_raw.group_var.drop_duplicates().sort_values().to_list()

        if verbose: iter_ = tqdm(group_vars)
        else      : iter_ = group_vars

        for group_var in iter_:

            df_tmp = (df_raw
                    .loc[lambda x: x.group_var == group_var]
                    .drop(columns = ["group_var"])
                    .dropna())

            if df_tmp.meeting_id.duplicated().any():
                if verbose: print("Duplicate meeting_id in {}".format(group_var))

            for vol_type, x_cols in vol_specs.items():

                group_models = self._expanding_ols_group(
                    df_tmp  = df_tmp,
                    x_cols  = x_cols,
                    min_obs = min_obs)

                models[group_var + " " + vol_type] = group_models

                check_list.append({
                    "model_name": group_var + " " + vol_type,
                    "n_input"   : len(df_tmp),
                    "burn_in"   : max(min_obs, len(x_cols) + 1) - 1,
                    "n_models"  : len(group_models)})

        df_check = (pd
                .DataFrame(check_list)
                .assign(exp_models = lambda x: (x.n_input - x.burn_in).clip(lower = 0))
                .loc[lambda x: x.exp_models != x.n_models])

        if len(df_check) > 0:
            if verbose:
                print("Model counts don't match input counts less burn-in")
                print(df_check)

        else:
            if verbose: print("Passed model count check")

        if verbose: print("Saving data\n")
        with open(out_path, "wb") as f: pickle.dump(models, f)

    def get_expanding_ols_params(self, verbose: bool = True) -> None:

        import warnings

        if verbose: print("Getting Expanding OLS Params")

        out_path = os.path.join(self.vol_path, "ExpandingVolOLSParams.parquet")
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None

        model_path = os.path.join(self.vol_path, "ExpandingVolEstimators.pkl")
        with open(model_path, "rb") as f: models = pickle.load(f)

        df_lists = []

        if verbose: iter_ = tqdm(models.keys())
        else      : iter_ = models.keys()

        for model_name in iter_:

            ticker, name, zone, vol_type = model_name.split(" ")

            for meeting_id, tmp_model in models[model_name].items():

                train_index = tmp_model.fittedvalues.index

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")

                    df_add = (pd.DataFrame({
                            "param" : tmp_model.params,
                            "pvalue": tmp_model.pvalues,
                            "tvalue": tmp_model.tvalues})
                        .rename_axis("index")
                        .reset_index()
                        .assign(
                            model_name          = model_name,
                            meeting_id          = meeting_id,
                            r2                  = tmp_model.rsquared,
                            adj_r2              = tmp_model.rsquared_adj,
                            n_obs               = int(tmp_model.nobs),
                            train_first_meeting = train_index[0],
                            train_last_meeting  = train_index[-1],
                            ticker              = ticker,
                            name                = name,
                            zone                = zone,
                            vol_type            = vol_type))

                df_lists.append(df_add)

        df_out = pd.concat(df_lists, ignore_index = True)

        # the model stored under meeting t must end its training at t
        df_bad1 = df_out.loc[lambda x: x.train_last_meeting != x.meeting_id]

        if len(df_bad1) > 0:
            if verbose:
                print("Training window doesn't end at the stored meeting")
                print(df_bad1)

        else:
            if verbose: print("Passed training window end check")

        # training sample should grow by exactly one meeting at a time
        df_bad2 = (df_out
                [["model_name", "meeting_id", "n_obs"]]
                .drop_duplicates()
                .sort_values(["model_name", "n_obs"])
                .assign(n_diff = lambda x: x.groupby("model_name").n_obs.diff())
                .loc[lambda x: x.n_diff.notna() & (x.n_diff != 1)])

        if len(df_bad2) > 0:
            if verbose:
                print("Training sample isn't expanding one meeting at a time")
                print(df_bad2)

        else:
            if verbose: print("Passed expanding window check")

        if verbose: print("Saving data\n")
        df_out.to_parquet(path = out_path, engine = "pyarrow")

def main() -> None: 
        
    vol_estimators = VolEstimators()
    #vol_estimators.get_prior_day_vol()
    #vol_estimators.get_zone_vol()
    #vol_estimators.combine_vols()
    #vol_estimators.full_sample_ols_models()
    #vol_estimators.get_full_sample_ols_params()
    
    #vol_estimators.expanding_ols_models()
    #vol_estimators.get_expanding_ols_params()
    
if __name__ == "__main__": main()