# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 20:26:02 2026

@author: Diego
"""

from FOMCDataPrep import DataPrep
from FOMCDataPreprocess import FOMCPreprocess

def main() -> None: 

    data_prep = DataPrep()
    data_prep.get_sentiment(verbose = True)
    data_prep.get_fed_funds(verbose = True)
    data_prep.get_tsy_yields(verbose = True)
    data_prep.get_tsy_futures(verbose = True)
    data_prep.get_equity_futures(verbose = True)
    data_prep.get_fed_survery_estimate(verbose = True)
    data_prep.get_mai_data(verbose = True)
    
    print(" ")
    fomc_preprocess = FOMCPreprocess()
    fomc_preprocess.prep_nlp(verbose = True)
    fomc_preprocess.prep_mai(verbose = True)
    fomc_preprocess.get_days()
    
if __name__ == "__main__": main()