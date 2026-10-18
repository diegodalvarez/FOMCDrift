# -*- coding: utf-8 -*-
"""
Created on Sat Oct 17 20:40:03 2026

@author: Diego
"""

import os
import requests
import pandas as pd

class AltDataCollectors:
    
    def __init__(self) -> None: 
        
        self.src_path  = os.getcwd()
        self.repo_path = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path = os.path.join(self.repo_path, "data")
        self.alt_path  = os.path.join(self.data_path, "AltData")
        
        if not os.path.exists(self.alt_path): 
            os.makedirs(self.alt_path)
            
    def collect_mai_data(self, verbose: bool = True) -> None: 
        
        if verbose: print("Collecting MAI data")
        
        out_path = os.path.join(self.alt_path, "MAI.parquet")
        if os.path.exists(out_path):
            if verbose: print("Already have data\n")
            return None
        
        url = (
            "https://api.github.com/repos/charlesmartineau/mai_rfs/contents/"
            "MAI%20Data/monetary%20and%20unemployment%20updated%20to%202025"
        )
        
        response = requests.get(url)
        response.raise_for_status()
        
        files = response.json()
        files = [(file["name"], file["download_url"]) for file in files]
        
        df_list = []        
        
        for name, download_url in files:
        
            if download_url is None:
                continue
        
            if name.endswith(".csv"):
                df_add = pd.read_csv(download_url)
        
            elif name.endswith((".xlsx", ".xls")):
                df_add = pd.read_excel(download_url)
        
            elif name.endswith(".parquet"):
                df_add = pd.read_parquet(download_url)
        
            else:
                continue
        
            df_append = (df_add
                    .melt(id_vars = "date")
                    .assign(file = name))
            
            df_list.append(df_append)
            
        df_combined = pd.concat(df_list)
        if verbose: print("Saving data\n")
        df_combined.to_parquet(path = out_path)
        
        
AltDataCollectors().collect_mai_data()