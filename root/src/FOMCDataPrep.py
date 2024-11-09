# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 07:02:30 2024

@author: Diego
"""

import os
import numpy as np
import pandas as pd
import datetime as dt
import pandas_datareader as web

class DataPrep:
    
    def __init__(self):
        
        self.data_path   = os.path.join(os.getcwd().split("\\root")[0], "data", "RawData")
        self.bbg_path    = r"C:\Users\Diego\Desktop\app_prod\BBGData"
        self.bbg_fut     = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager"
        self.survey_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\SurveyData"
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        
    def _get_fut_rtn(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        return(df.sort_values(
            "date").
            assign(
                PX_diff = lambda x: x.PX_LAST.diff(),
                PX_pct  = lambda x: x.PX_LAST.pct_change()).
            dropna())
    
    def _get_yld_diff(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        return(df.sort_values(
            "date").
            assign(val_diff = lambda x: x.value.diff()).
            dropna())
        
    def get_tsy_yields(self, verbose: bool = False) -> pd.DataFrame:
        
        path = os.path.join(self.data_path, "TSYFredYields.parquet")
        try:
            
            if verbose == True: print("Searching for Treasury data")
            df_out = pd.read_parquet(path = path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except:
            
            tsy_tickers = [
                "DGS1MO", "DGS3MO", "DGS1", "DGS2", "DGS5", "DGS7", "DGS10", 
                "DGS20", "DGS30"]
            
            start_date = dt.date(year = 1900, month = 1, day = 1)
            end_date   = dt.date.today()
            
            if verbose == True: print("Collecting Data")
            df_out = (web.DataReader(
                name        = tsy_tickers,
                data_source = "fred",
                start       = start_date,
                end         = end_date).
                reset_index().
                rename(columns = {"DATE": "date"}).
                melt(id_vars = "date").
                dropna().
                groupby("variable").
                apply(self._get_yld_diff).
                reset_index(drop = True))
            
            df_out.to_parquet(path = path, engine = "pyarrow")
            
        return df_out
    
    def get_sentiment(self, verbose: bool = False) -> pd.DataFrame:
        
        read_path = os.path.join(self.data_path, "SentimentData.parquet")
        try:
            
            if verbose == True: print("Trying to find Sentiment data")
            df_out = pd.read_parquet(path = read_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except:
            
            if verbose == True: print("Collecting data")
        
            tickers_path = os.path.join(self.bbg_path, "root", "BBGTickers.xlsx")
            keywords     = ["Decomposition", "Federal"]
            
            df_tickers = (pd.read_excel(
                io = tickers_path, sheet_name = "tickers").
                assign(
                    tmp1 = lambda x: x.Description.str.split(" ").str[0],
                    tmp2 = lambda x: x.Description.str.split(" ").str[1],
                    tmp3 = lambda x: x.Description.str.split(" ").str[2]).
                query("tmp1 == 'Bloomberg' & tmp2 == 'Economics' & tmp3 == @keywords")
                [["Security", "Description"]].
                rename(columns = {"Security": "security"}))
            
            tickers = (df_tickers.assign(
                ticker = lambda x: x.security.str.split(" ").str[0]).
                ticker.
                drop_duplicates().
                to_list())
            
            file_paths = [
                os.path.join(self.bbg_path, "data", ticker + ".parquet")
                for ticker in tickers]
            
            df_raw = (pd.read_parquet(
                path = file_paths, engine = "pyarrow").
                drop(columns = ["variable"]).
                assign(ending = lambda x: x.security.str[0]))
            
            df_ben, df_a = df_raw.query("ending != 'A'"), df_raw.query("ending == 'A'")
            
            df_a_prep = (df_a.drop(
                columns = ["ending"]).
                pivot(index = "date", columns = "security", values = "value").
                cumsum().
                reset_index().
                melt(id_vars = "date"))
            
            df_out = (pd.concat(
                [df_ben.drop(columns = ["ending"]), df_a_prep]).
                merge(right = df_tickers, how = "inner", on = ["security"]).
                assign(
                    security = lambda x: x.security.str.split(" ").str[0],
                    date     = lambda x: pd.to_datetime(x.date).dt.date))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = read_path, engine = "pyarrow")
        
        return df_out
    
    def get_tsy_futures(self, verbose = False) -> pd.DataFrame: 
        
        read_path = os.path.join(self.data_path, "TreasuryFutures.parquet")
        try:
            
            if verbose == True: print("Looking for Treasury data")
            df_out = pd.read_parquet(path = read_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Coouldn't find Data")
            tickers = ["TU", "TY", "US", "FV", "UXY", "WN"]
            
            px_paths = [
                os.path.join(self.bbg_fut, "data", "PXFront", file + ".parquet")
                for file in tickers]
            
            deliv_paths = [
                os.path.join(self.bbg_fut, "data", "BondDeliverableRisk", file + ".parquet")
                for file in tickers]
            
            df_px = (pd.read_parquet(
                path = px_paths, engine = "pyarrow").
                groupby("security").
                apply(self._get_fut_rtn).
                reset_index(drop = True))
            
            df_deliv = (pd.read_parquet(
                path = deliv_paths, engine = "pyarrow").
                pivot(index = ["date", "security"], columns = "variable", values = "value").
                reset_index().
                rename(columns = {
                    "FUT_EQV_CNVX_NOTL"            : "FUT_CNVX",
                    "CONVENTIONAL_CTD_FORWARD_FRSK": "CTD_DUR"}).
                dropna())
            
            df_out = (df_px.merge(
                right = df_deliv, how = "inner", on = ["date", "security"]).
                assign(PX_bps = lambda x: x.PX_diff / x.CTD_DUR))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = read_path, engine = "pyarrow")
        
        return df_out
    
    def get_equity_futures(self, verbose: bool = False) -> pd.DataFrame: 
        
        read_path = os.path.join(self.data_path, "EquityFutures.parquet")
        try:
            
            if verbose == True: print("Looking for Equity Futures data")
            df_out = pd.read_parquet(path = read_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't equity future data")
            tickers  = ["ES", "UX"]
            px_paths = [
                os.path.join(self.bbg_fut, "data", "PXFront", file + ".parquet")
                for file in tickers]
            
            df_out = (pd.read_parquet(
                path = px_paths, engine = "pyarrow").
                assign(
                    security = lambda x: x.security.str.split(" ").str[0],
                    date     = lambda x: pd.to_datetime(x.date).dt.date).
                groupby("security").
                apply(self._get_fut_rtn).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = read_path, engine = "pyarrow")
        
        return df_out
    
    def get_fed_funds(self, verbose: bool = False) -> pd.DataFrame: 
        
        read_path = os.path.join(self.data_path, "FFRate.parquet")
        try:
            
            if verbose == True: print("Looking for Fed Funds Data data")
            df_out = pd.read_parquet(path = read_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't Fed Funds data")
            
            file_path = os.path.join(self.bbg_path, "data", "FDTR.parquet")
            df_out = (pd.read_parquet(
                path = file_path, engine = "pyarrow").
                assign(
                    date     = lambda x: pd.to_datetime(x.date).dt.date,
                    security = lambda x: x.security.str.split(" ").str[0],
                    val_diff = lambda x: x.value.diff()).
                dropna().
                drop(columns = ["val_diff", "variable", "security"]).
                rename(columns = {"value": "FDTR"}))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = read_path, engine = "pyarrow")
            
        return df_out
    
    def get_fed_survery_estimate(self, verbose: bool = False) -> pd.DataFrame: 
        
        read_path = os.path.join(self.data_path, "FedEstimate.parquet")
        try:
            
            if verbose == True: print("Looking for Fed Funds Esimate data")
            df_tmp = pd.read_parquet(path = read_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except:
        
            if verbose == True: print("Generating Data")
            file_path = os.path.join(self.survey_path, "fdtr.parquet")
            
            df_result_namer = (pd.DataFrame({
                "result_tri"    : [-1, 0, 1],
                "result_outcome": ["Undershoot", "Match", "Overshoot"]}))
            
            df_tmp = (pd.read_parquet(
                path = file_path, engine = "pyarrow").
                drop(columns = ["security"]).
                pivot(index = "date", columns = "variable", values = "value").
                rename(columns = {
                    "BN_SURVEY_LOW"                : "bn_low",
                    "BN_SURVEY_HIGH"               : "bn_high",
                    "BN_SURVEY_MEDIAN"             : "bn_median",
                    "BN_SURVEY_AVERAGE"            : "bn_average",
                    "BN_SURVEY_NUMBER_OBSERVATIONS": "num_obs"}).
                dropna().
                reset_index().
                melt(id_vars = ["date", "num_obs"]).
                assign(date = lambda x: pd.to_datetime(x.date).dt.date).
                merge(right = self.get_fed_funds(), how = "inner", on = ["date"]).
                rename(columns = {
                    "value": "predicted",
                    "FDTR" : "actual"}).
                assign(
                    raw_result = lambda x: x.predicted - x.actual,
                    result_tri = lambda x: np.sign(x.raw_result)).
                merge(right = df_result_namer, how = "inner", on = ["result_tri"]).
                drop(columns = ["result_tri"]))
            
            if verbose == True: print("Saving Data\n")
            df_tmp.to_parquet(path = read_path, engine = "pyarrow")
            
        return df_tmp
    
def main():
    
    data_prep = DataPrep()
    data_prep.get_sentiment(verbose = True)
    data_prep.get_fed_funds(verbose = True)
    data_prep.get_tsy_yields(verbose = True)
    data_prep.get_tsy_futures(verbose = True)
    data_prep.get_equity_futures(verbose = True)
    data_prep.get_fed_survery_estimate(verbose = True)

#if __name__ == "__main__": main()