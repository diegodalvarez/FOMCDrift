# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 2026

Compares strategy returns against a reference strategy: Sharpe ratios, the
Sharpe spread and ratio to the reference, and a t-test on the matched return
spreads. Model-dependent counterpart to FeatureEvaluation.py, which stays
model-free. Turns notebook 3 cells 99-104 into one reusable tool.

    sc  = StrategyComparison(df_rtn = df_ts, reference = "mean__median")
    out = sc.compare(level = "event")
    out["sharpe"]   # one row per model x feature_set
    out["spread"]   # one row per model x feature_set compared to the reference
    sc.alpha_table(level = "event")   # alpha of the reference over each benchmark

Inputs
    df_rtn    : minute returns with date, the by column, group_cols and
                rtn_col, e.g. df_ts from StrategyEvaluation.feature_set_returns
                (any number of ticker / zone rows per minute) or df_model_rtn
                (already one row per minute). Event level needs meeting_id,
                so it works on df_ts but not on df_model_rtn
    reference : the value of the by column everything is compared against
    by        : the column holding the strategies being compared
    group_cols: comparisons are made within each of these (e.g. model)

Book
    Every row at the same date is averaged first (the equal weight book
    across tickers and zones, as in df_model_rtn).
    level = "minute": one observation per minute of that book
    level = "event" : the book summed to one PnL per meeting

Annualizer
    sqrt(observations per year), measured from the data: minutes per year at
    minute level (about sqrt(1020 * 8) for the four zones), meetings per year
    at event level (about sqrt(8))

Spread
    sharpe_spread = Sharpe(reference) - Sharpe(other)
    spread_ratio  = sharpe_spread / Sharpe(other)  (hard to read when the
                    other Sharpe is near zero or negative)
    t_stat        = one sample t-test that the matched return spread
                    reference - other has mean zero. At minute level the
                    minutes are not independent, so prefer event level
"""

import numpy as np
import pandas as pd

import statsmodels.api as sm

from scipy.stats import ttest_1samp


class StrategyComparison:

    def __init__(
            self,
            df_rtn    : pd.DataFrame,
            reference : str,
            by        : str  = "feature_set",
            group_cols: list = ["model"],
            rtn_col   : str  = "signal_rtn",
            verbose   : bool = True) -> None:

        self.by         = by
        self.reference  = reference
        self.group_cols = list(group_cols)
        self.rtn_col    = rtn_col
        self.verbose    = verbose

        missing = [col for col in ["date", by, rtn_col] + self.group_cols if col not in df_rtn.columns]
        if len(missing) > 0:
            raise ValueError("df_rtn is missing {}".format(missing))

        if reference not in df_rtn[by].unique():
            raise ValueError("reference {} not in {}, options are {}".format(reference, by, sorted(df_rtn[by].unique())))

        keep_cols   = ["date", by, rtn_col] + self.group_cols + (["meeting_id"] if "meeting_id" in df_rtn.columns else [])
        self.df_rtn = (df_rtn
            [keep_cols]
            .assign(date = lambda x: pd.to_datetime(x.date)))

        self.years = (self.df_rtn.date.max() - self.df_rtn.date.min()).days / 365.25

        if verbose:
            print("Comparing {} {} values against {} within {}".format(
                self.df_rtn[by].nunique(), by, reference, self.group_cols))

    # ------------------------------------------------------------ helpers

    def _book(self, level: str) -> tuple:

        # equal weight book across every row at the same date, then one
        # observation per minute or per meeting. Returns (df_book, time_col)
        if level not in ["minute", "event"]:
            raise ValueError("level must be minute or event")

        keys = self.group_cols + [self.by]

        if level == "minute":

            df_book = (self.df_rtn
                .groupby(keys + ["date"])
                [self.rtn_col]
                .mean()
                .reset_index())

            return df_book, "date"

        if "meeting_id" not in self.df_rtn.columns:
            raise ValueError("event level needs meeting_id, pass df_ts rather than df_model_rtn")

        df_book = (self.df_rtn
            .groupby(keys + ["date", "meeting_id"])
            [self.rtn_col]
            .mean()
            .reset_index()
            .groupby(keys + ["meeting_id"])
            [self.rtn_col]
            .sum()
            .reset_index())

        return df_book, "meeting_id"

    def _annualizer(self, df_book: pd.DataFrame, time_col: str) -> float:

        return np.sqrt(df_book[time_col].nunique() / self.years)

    # ------------------------------------------------------------- results

    def sharpe_table(self, level: str = "event") -> pd.DataFrame:

        # one row per group_cols x by: mean, std, annualized Sharpe, n_obs
        df_book, time_col = self._book(level)
        annualizer        = self._annualizer(df_book, time_col)

        df_out = (df_book
            .groupby(self.group_cols + [self.by])
            [self.rtn_col]
            .agg(
                mean_rtn = "mean",
                std_rtn  = "std",
                n_obs    = "count")
            .reset_index()
            .assign(
                sharpe     = lambda x: x.mean_rtn / x.std_rtn * annualizer,
                annualizer = annualizer,
                level      = level))

        return df_out

    def _tstat(self, df: pd.DataFrame) -> pd.Series:

        spread          = df.rtn_spread.dropna()
        t_stat, p_value = ttest_1samp(a = spread, popmean = 0)

        return pd.Series({
            "mean_rtn_spread": spread.mean(),
            "t_stat"         : t_stat,
            "p_value"        : p_value,
            "n_matched"      : len(spread)})

    def spread_table(self, level: str = "event", df_sharpe: pd.DataFrame = None) -> pd.DataFrame:

        # one row per group_cols x other strategy: the Sharpe spread and
        # ratio to the reference, and the t-test on the matched return spread
        if df_sharpe is None: df_sharpe = self.sharpe_table(level)
        df_book, time_col = self._book(level)

        df_sharpe_spread = (df_sharpe
            .pivot(index = self.group_cols, columns = self.by, values = "sharpe")
            .reset_index()
            .melt(id_vars = self.group_cols + [self.reference], var_name = self.by, value_name = "sharpe")
            .rename(columns = {self.reference: "reference_sharpe"})
            .dropna(subset = ["sharpe", "reference_sharpe"])
            .assign(
                sharpe_spread = lambda x: x.reference_sharpe - x.sharpe,
                spread_ratio  = lambda x: x.sharpe_spread / x.sharpe))

        # matched on the same minutes / meetings, as in df_rtn_spread
        df_tstat = (df_book
            .pivot(index = self.group_cols + [time_col], columns = self.by, values = self.rtn_col)
            .reset_index()
            .melt(id_vars = self.group_cols + [time_col, self.reference], var_name = self.by, value_name = "other_rtn")
            .assign(rtn_spread = lambda x: x[self.reference] - x.other_rtn)
            .groupby(self.group_cols + [self.by])
            [["rtn_spread"]]
            .apply(self._tstat)
            .reset_index())

        df_out = (df_sharpe_spread
            .merge(right = df_tstat, how = "left", on = self.group_cols + [self.by])
            .assign(
                reference = self.reference,
                level     = level)
            [self.group_cols + ["reference", self.by, "reference_sharpe", "sharpe", "sharpe_spread",
             "spread_ratio", "mean_rtn_spread", "t_stat", "p_value", "n_matched", "level"]])

        return df_out

    def compare(self, level: str = "event") -> dict:

        # Returns {"sharpe": sharpe_table, "spread": spread_table}
        df_sharpe = self.sharpe_table(level)
        df_spread = self.spread_table(level, df_sharpe = df_sharpe)

        if self.verbose:

            print("{} level, annualizer {:.2f}, {} observations per strategy".format(
                level, df_sharpe.annualizer.iloc[0], int(df_sharpe.n_obs.max())))

            for _, row in df_spread.iterrows():
                print("    {} | {} vs {}: Sharpe {:.3f} vs {:.3f}, spread {:+.3f}, t {:.2f} (p {:.3f})".format(
                    " | ".join(str(row[col]) for col in self.group_cols),
                    self.reference, row[self.by],
                    row.reference_sharpe, row.sharpe, row.sharpe_spread,
                    row.t_stat, row.p_value))

        return {"sharpe": df_sharpe, "spread": df_spread}

    # --------------------------------------------------------------- alpha

    def _alpha_regression(self, y: pd.Series, x: pd.Series, cov_type: str, max_lags: int) -> dict:

        # y = alpha + beta * x + e with heteroskedasticity robust (HC1) or
        # Newey-West (HAC) standard errors
        X   = sm.add_constant(x.to_numpy(dtype = float))
        cov = {"cov_type": "HC1"} if cov_type == "HC1" else {"cov_type": "HAC", "cov_kwds": {"maxlags": max_lags}}
        res = sm.OLS(y.to_numpy(dtype = float), X).fit(**cov)

        return {
            "alpha"    : res.params[0],
            "t_alpha"  : res.tvalues[0],
            "p_alpha"  : res.pvalues[0],
            "beta"     : res.params[1],
            "t_beta"   : res.tvalues[1],
            "p_beta"   : res.pvalues[1],
            "r2"       : res.rsquared,
            "resid_std": np.sqrt(res.mse_resid),
            "n_obs"    : int(res.nobs)}

    def alpha_table(
            self,
            level   : str = "event",
            cov_type: str = None,
            max_lags: int = None) -> pd.DataFrame:

        # Does the reference earn alpha over each other strategy (the
        # benchmark)? Within each group_cols, on matched minutes / meetings:
        #
        #   r_reference = alpha + beta * r_benchmark + e
        #
        # alpha > 0 and significant: the reference earns returns the
        # benchmark doesn't explain. beta near 1 and a high r2: the two mostly
        # take the same positions.
        #
        # cov_type: "HC1" (robust) or "HAC" (Newey-West). Defaults to HC1 at
        #   event level and HAC at minute level, where neighbouring minutes of
        #   a held position are correlated
        # max_lags: HAC lags, defaults to floor(4 * (n / 100) ^ (2 / 9))
        #
        # alpha_ann       = alpha * observations per year
        # info_ratio      = alpha / resid_std * sqrt(observations per year),
        #                   the Sharpe of the part not explained by the benchmark

        if cov_type is None: cov_type = "HC1" if level == "event" else "HAC"
        if cov_type not in ["HC1", "HAC"]:
            raise ValueError("cov_type must be HC1 or HAC")

        df_book, time_col = self._book(level)
        annualizer        = self._annualizer(df_book, time_col)

        df_wide = (df_book
            .pivot(index = self.group_cols + [time_col], columns = self.by, values = self.rtn_col)
            .reset_index()
            .sort_values(self.group_cols + [time_col]))

        others = [col for col in df_book[self.by].unique() if col != self.reference]
        rows   = []

        for group_keys, df_group in df_wide.groupby(self.group_cols):

            group_keys = group_keys if isinstance(group_keys, tuple) else (group_keys,)

            for other in others:

                df_pair = df_group[[self.reference, other]].dropna()
                if len(df_pair) < 10: continue

                lags = max_lags
                if lags is None: lags = int(np.floor(4 * (len(df_pair) / 100) ** (2 / 9)))

                res = self._alpha_regression(
                    y        = df_pair[self.reference],
                    x        = df_pair[other],
                    cov_type = cov_type,
                    max_lags = lags)

                rows.append({
                    **dict(zip(self.group_cols, group_keys)),
                    "reference" : self.reference,
                    "benchmark" : other,
                    **res,
                    "alpha_ann" : res["alpha"] * annualizer ** 2,
                    "info_ratio": res["alpha"] / res["resid_std"] * annualizer,
                    "cov_type"  : cov_type if cov_type == "HC1" else "HAC({})".format(lags),
                    "level"     : level})

        df_out = pd.DataFrame(rows)

        if self.verbose and len(df_out) > 0:

            print("Alpha of {} over each benchmark, {} level, {} errors".format(self.reference, level, cov_type))
            for _, row in df_out.iterrows():
                print("    {} | vs {}: alpha {:.2e} (t {:.2f}, p {:.3f}), beta {:.2f}, r2 {:.2f}, IR {:.2f}".format(
                    " | ".join(str(row[col]) for col in self.group_cols),
                    row.benchmark, row.alpha, row.t_alpha, row.p_alpha,
                    row.beta, row.r2, row.info_ratio))

        return df_out
