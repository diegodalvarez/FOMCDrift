# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 07:02:30 2024

@author: Diego
"""

import os
import requests
import numpy as np
import pandas as pd
import datetime as dt
#import pandas_datareader as web

class DataPrep:
    
    def __init__(self):
        
        self.script_dir = (os.path.abspath(__file__))
        self.src_path   = os.path.abspath(os.path.join(self.script_dir, os.pardir))
        self.root_path  = os.path.abspath(os.path.join(self.src_path, os.pardir))
        self.repo_path  = os.path.abspath(os.path.join(self.root_path, os.pardir))
        self.data_path  = os.path.join(self.repo_path, "data")
        self.raw_path   = os.path.join(self.data_path, "RawData")
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        if os.path.exists(self.raw_path) == False: os.makedirs(self.raw_path)
        
        #self.bbg_path = r"C:\Users\Diego\Desktop\app_prod\BBGData"
        #if os.path.exists(self.bbg_path) == False: 
        #    self.bbg_path = r"/Users/diegoalvarez/Desktop/BBGData"
        
        #self.bbg_fut = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager"
        #if os.path.exists(self.bbg_fut) == False: 
        #    self.bbg_fut = r"/Users/diegoalvarez/Desktop/BBGFuturesManager"
        
        #self.survey_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\SurveyData"
        #if os.path.exists(self.survey_path) == False:
        #    self.survey_path = r"/Users/diegoalvarez/Desktop/BBGData/SurveyData"
            
        self.bbg_path    = r"A:\BBGData"
        self.bbg_fut     = r"A:\BBGFuturesManager_backup_backup"
        self.survey_path = r"A:\BBGData\SurveyData"
        
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
        
        path = os.path.join(self.raw_path, "TSYFredYields.parquet")
        
        if os.path.exists(path) == False:
            
            tmp_path = os.path.join(self.bbg_path, "data")
            tickers  =([
                ticker for ticker in os.listdir(tmp_path)
                if ticker[0:4] == "USGG" and ticker[5] != 'B'])
            
            paths  = [os.path.join(tmp_path, ticker) for ticker in tickers]
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                assign(tmp = lambda x: x.security.str[4]).
                query("tmp != ['B', 'F', 'T']").
                drop(columns = ["tmp"]).
                dropna().
                set_index("date").
                groupby("variable").
                apply(self._get_yld_diff, include_groups = False).
                reset_index().
                assign(security = lambda x: x.security.str.split(" ").str[0]))
            
            if verbose: print("Saving Treasury Yield Data")
            df_out.to_parquet(path = path, engine = "pyarrow")
            
        else: 
            print("Treasury Yields Data Already collected")
    
    def get_sentiment(self, verbose: bool = False) -> None:
        
        read_path = os.path.join(self.raw_path, "SentimentData.parquet")
        if os.path.exists(read_path) == False: 
            
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
            
        else: 
            print("Sentiment Data Already collected")
    
    def get_tsy_futures(self, verbose = False) -> None: 
        
        read_path = os.path.join(self.raw_path, "TreasuryFutures.parquet")
        
        if os.path.exists(read_path) == False: 
        
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
                apply(self._get_fut_rtn, include_groups = False).
                reset_index().
                drop(columns = ["level_1"]))
            
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
        
        else: 
            if verbose: print("Treasury Futures already collected")
    
    def get_equity_futures(self, verbose: bool = False) -> pd.DataFrame: 
        
        read_path = os.path.join(self.raw_path, "EquityFutures.parquet")
        
        if os.path.exists(read_path) == False: 
            
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
        
        else:
            if verbose == True: print("Equity Futures already collected")
    
    def get_fed_funds(self, verbose: bool = False) -> None: 
        
        read_path = os.path.join(self.raw_path, "FFRate.parquet")
        
        if os.path.exists(read_path) == False:
        
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
            
        else: 
            if verbose: print("Fed Funds Data Already collected")
    
    def get_fed_survery_estimate(self, verbose: bool = False) -> None: 
        
        read_path = os.path.join(self.raw_path, "FedEstimate.parquet")
        ff_path   = os.path.join(self.raw_path, "FFRate.parquet")
        
        if os.path.exists(read_path) == False: 
        
            if verbose == True: print("Couldn't find Fed Funds Estimate Data")
            file_path = os.path.join(self.survey_path, "fdtr.parquet")
            
            df_result_namer = (pd.DataFrame({
                "result_tri"    : [-1, 0, 1],
                "result_outcome": ["Undershoot", "Match", "Overshoot"]}))
            
            df_surprise = (pd.read_parquet(
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
                melt(id_vars = ["date", "num_obs"]))
            
            df_tmp = (pd.read_parquet(
                path = ff_path, engine = "pyarrow").
                assign(date = lambda x: pd.to_datetime(x.date)).
                merge(right = df_surprise, how = "inner", on = ["date"]).
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
            
        else:
            if verbose: print("Already Collected Fed Feds Survery Data")
    
    def get_mai_data(self, verbose: bool = False) -> None: 
        
        file_path = os.path.join(self.raw_path, "MAIData.parquet")
        if os.path.exists(file_path) == False: 
        
            if verbose == True: print("Couldn't find data, collecting it now")
            
            url = r"https://www.dropbox.com/scl/fi/31egxyr781taa4q88x6v1"\
                "/Fisher_Martineau_Sheng_MAI-Data.xlsx"\
                    "?rlkey=svwon9ambcjtpw44y5zoum1rl&dl=1"
                
            sheet_names = ["Daily Data", "Monthly Data"]
            df_out      = pd.DataFrame()
            
            for sheet_name in sheet_names: 
                
                df_tmp = (pd.read_excel(
                    io = url, sheet_name = sheet_name).
                    melt(id_vars = "date").
                    dropna().
                    assign(group = sheet_name.lower().replace(" ", "_")))
                
                df_out = pd.concat([df_out, df_tmp])
                
            if verbose == True: print("Collecting data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        else:
            if verbose: print("Already Collected MAI Data")
    
def main():
    
    data_prep = DataPrep()
    #data_prep.get_sentiment(verbose = True)
    data_prep.get_fed_funds(verbose = True)
    #data_prep.get_tsy_yields(verbose = True)
    #data_prep.get_tsy_futures(verbose = True)
    #data_prep.get_equity_futures(verbose = True)
    #data_prep.get_fed_survery_estimate(verbose = True)
    #data_prep.get_mai_data(verbose = True)

if __name__ == "__main__": main()