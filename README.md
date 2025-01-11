# FOMCDrift
FOMC Drift

Replicating the results found ***The Pre-FOMC Drift and the Secular Decline in Long-Term Interest Rates*** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4764451)

First start by decomposing the Treasury yield based on cumulative summed differences around FOMC annoucements
![image](https://github.com/user-attachments/assets/ac918835-3958-4565-a502-84d852450527)

Naive backtests can be built from analyzing returns (in basis points) around FOMC days and playing back those returns. In this case the backtests are conditioned on just one variable. The backtests include 
1. No Condition
2. Bloomberg NLP data
3. Macro Attention Indices (MAI)
4. Bloomberg Economics Median Survey Data

Below is an example of the returns playback for Survey Data. 
<img width="1467" alt="image" src="https://github.com/user-attachments/assets/377a60fd-c58b-46db-8567-d4545f739100" />

Building a simplistic backtest into this model yields the following returns
<img width="1464" alt="image" src="https://github.com/user-attachments/assets/1f4843fc-8b93-48c2-88c4-145a1dd2faa2" />

Now all of the backtests can be put together and fit via OLS. In this case regress the survey data, NLP data against the Treasury Futures return and then trade the residuals. The full-sample model takes the form. This is obviously overfit, but it sets a benchmark for modelling the returns. Some of the following steps will be to relax these conditions and built out an in-sample out-of-sample model. 

<img width="1187" alt="image" src="https://github.com/user-attachments/assets/00a68869-93ee-4636-890a-67ee3535df5a" />

# Todo
1. Naive Model Comparison
2. Formalize OLS Regression
3. Boostrapped OLS
4. Rolling OLS
5. Expanding OLS
