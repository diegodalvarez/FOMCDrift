# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 2026

Tests whether a candidate feature should be added to a baseline feature set.
Every fit goes through EventModels.fit_quick, so the z-scoring, per group fits
(GROUP_COLS) and expanding out-of-sample windows match the rest of the
pipeline.

    tester = FeatureTester(baseline = ["lag_mean_rtn"])
    df_sum, details = tester.run(["lag_std_rtn", "lag_skew_rtn_w"])

Stages (each is also a method returning a DataFrame)

    hygiene      coverage, events lost vs the baseline, look-ahead, constant groups
    univariate   correlation with the target per group and pooled, sign
                 agreement across groups (binomial test), by sub-period
    redundancy   correlation with each baseline feature, VIF, partial
                 correlation (candidate and target residualized on the baseline)
    incremental  baseline vs baseline + candidate, fit expanding out-of-sample
                 on the same events:
                   ols   change in MSE, Clark-West test (nested models)
                   logit change in log-loss, Diebold-Mariano style test
                   all   change in hit rate, paired test on event PnL,
                         event-level Sharpe of both
                 pooled tests cluster by meeting_id (the tenors of one meeting
                 are not independent). In-sample nested F-test as a secondary
    stability    incremental by sub-period, how often the candidate's expanding
                 coefficient disagrees with its latest sign
    run          all of the above for several candidates, Benjamini-Hochberg
                 adjusted p-values across them, pass / fail flags
    final_check  incremental on the held-out meetings only, use once per
                 feature at the end

The last `holdout` share of meetings is kept out of every stage except
final_check. Event PnL is sign(signal) * cum_rtn, which equals the sum of the
minute signal returns that StrategyEvaluation trades.

Output (run with save = True): data/FeatureTests/<baseline>__<candidate>/
    Summary.parquet, one parquet per detail table, Spec.json
"""

import os
import json
import warnings
import numpy as np
import pandas as pd

from scipy import stats
from EventFeatures import EventFeatures, EVENT_COLS, GROUP_COLS
from EventModels import EventModels
from ModelRegistry import get_model

EVENT_ANNUALIZER = np.sqrt(8)  # 8 FOMC meetings a year

DEFAULT_PERIODS = {
    "to_2008"  : (None        , "2008-12-31"),
    "2009_2015": ("2009-01-01", "2015-12-31"),
    "2016_2019": ("2016-01-01", "2019-12-31"),
    "2020_on"  : ("2020-01-01", None)}

DEFAULT_THRESHOLDS = {
    "max_coverage_loss" : 0.05,   # share of baseline events the candidate drops
    "min_sign_agree"    : 0.625,  # share of groups agreeing with the pooled sign (15 / 24)
    "max_baseline_corr" : 0.80,   # |correlation| with any baseline feature
    "max_adj_pvalue"    : 0.10,   # BH adjusted incremental p-value (primary model)
    "min_period_tstat"  : -2.0}   # no sub-period where the candidate is significantly worse


# ------------------------------------------------------------------ helpers

def _cluster_t(d: pd.Series, clusters: pd.Series) -> tuple:

    # t-stat of mean(d) with standard errors clustered by `clusters`
    d        = pd.Series(np.asarray(d, dtype = float))
    clusters = pd.Series(np.asarray(clusters))
    mask     = d.notna()
    d, clusters = d[mask], clusters[mask]

    n, mean = len(d), d.mean()
    if n < 3: return mean, np.nan

    sums = (d - mean).groupby(clusters.values).sum()
    g    = len(sums)
    if g < 2: return mean, np.nan

    se = np.sqrt((sums ** 2).sum() * g / (g - 1)) / n
    return mean, (mean / se if se > 0 else np.nan)

def _simple_t(d: pd.Series) -> tuple:

    d = pd.Series(np.asarray(d, dtype = float)).dropna()
    if len(d) < 3 or d.std() == 0: return d.mean(), np.nan
    return d.mean(), d.mean() / (d.std() / np.sqrt(len(d)))

def _bh_adjust(pvalues: pd.Series) -> pd.Series:

    # Benjamini-Hochberg adjusted p-values (NaN kept as NaN)
    p     = pd.Series(pvalues, dtype = float)
    valid = p.dropna().sort_values()
    m     = len(valid)

    if m == 0: return p

    ranks    = np.arange(1, m + 1)
    adjusted = np.minimum.accumulate((valid.values * m / ranks)[::-1])[::-1]
    out      = p.copy()
    out.loc[valid.index] = np.minimum(adjusted, 1.0)

    return out

def _zscore_by_group(df: pd.DataFrame, cols: list) -> pd.DataFrame:

    grouped = df.groupby(GROUP_COLS)[cols]
    return (df[cols] - grouped.transform("mean")) / grouped.transform("std")

def _event_sharpe(df: pd.DataFrame, col: str) -> float:

    # Sharpe of the equal weight book: average PnL across groups per meeting
    pnl = df.groupby("meeting_id")[col].mean()
    return pnl.mean() / pnl.std() * EVENT_ANNUALIZER if pnl.std() > 0 else np.nan


class FeatureTester:

    def __init__(
            self,
            baseline  : list  = ["lag_mean_rtn"],
            data      : pd.DataFrame = None,
            scheme    : str   = "out_sample",
            target    : str   = "cum_rtn",
            models    : list  = ["ols", "logit"],
            primary   : str   = "ols",
            vol_types : list  = ["combined"],
            min_obs   : int   = 30,
            holdout   : float = 0.2,
            periods   : dict  = None,
            thresholds: dict  = None) -> None:

        # baseline : features already in the model ([] = intercept only, i.e.
        #            the expanding mean of the target)
        # data     : event table with EVENT_COLS, target and every feature.
        #            Defaults to EventFeatures().load(scheme); out_sample keeps
        #            the features free of full sample vol normalization
        # primary  : model whose incremental p-value decides the pass flag

        self.src_path  = os.getcwd()
        self.repo_path = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path = os.path.join(self.repo_path, "data")
        self.test_path = os.path.join(self.data_path, "FeatureTests")

        self.event_models = EventModels()
        self.baseline     = list(baseline)
        self.scheme       = scheme
        self.target       = target
        self.models       = list(models)
        self.primary      = primary
        self.min_obs      = min_obs
        self.holdout      = holdout
        self.periods      = DEFAULT_PERIODS if periods is None else periods
        self.thresholds   = {**DEFAULT_THRESHOLDS, **(thresholds or {})}

        if primary not in self.models:
            raise ValueError("primary must be one of models {}".format(self.models))

        if data is None:
            data = EventFeatures().load(scheme)

        data = data.assign(meeting_id = lambda x: x.meeting_id.astype(int))

        if vol_types is not None and "vol_type" in data.columns:
            data = data.loc[lambda x: x.vol_type.isin(vol_types)]

        # event_start is needed for the sub-periods
        if "event_start" not in data.columns:
            try:
                df_start = (EventFeatures()
                    .load(scheme)
                    [EVENT_COLS + ["event_start"]]
                    .assign(meeting_id = lambda x: x.meeting_id.astype(int)))
                data = data.merge(right = df_start, how = "left", on = EVENT_COLS)
            except Exception:
                warnings.warn("no event_start in data, sub-periods are skipped")

        n_dup = data.duplicated(subset = EVENT_COLS).sum()
        if n_dup > 0:
            raise ValueError("data has {} duplicated events on {}".format(n_dup, EVENT_COLS))

        missing = [col for col in self.baseline + [target] if col not in data.columns]
        if len(missing) > 0:
            raise ValueError("{} not in data".format(missing))

        # the last holdout share of meetings is kept for final_check
        meetings            = np.sort(data.meeting_id.unique())
        n_hold              = int(np.ceil(len(meetings) * holdout))
        self.holdout_start  = int(meetings[-n_hold]) if n_hold > 0 else int(meetings.max()) + 1
        self.data           = data.sort_values(EVENT_COLS).reset_index(drop = True)
        self.dev            = self.data.loc[lambda x: x.meeting_id < self.holdout_start]

    # ---------------------------------------------------------------- data

    def _check_candidate(self, candidate: str) -> None:

        if candidate not in self.data.columns:
            raise ValueError("{} not in data".format(candidate))

        if candidate in self.baseline:
            raise ValueError("{} is already in the baseline".format(candidate))

    def _common(self, candidate: str, df: pd.DataFrame = None) -> pd.DataFrame:

        # events with the target, every baseline feature and the candidate, so
        # both models are fit and scored on exactly the same events
        df = self.dev if df is None else df
        return df.dropna(subset = self.baseline + [candidate, self.target])

    def _periods_of(self, df: pd.DataFrame) -> pd.Series:

        period = pd.Series(np.nan, index = df.index, dtype = object)
        if "event_start" not in df.columns: return period

        for name, (start, end) in self.periods.items():
            mask = pd.Series(True, index = df.index)
            if start is not None: mask &= df.event_start >= pd.Timestamp(start)
            if end   is not None: mask &= df.event_start <= pd.Timestamp(end) + pd.Timedelta(days = 1)
            period.loc[mask] = name

        return period

    # --------------------------------------------------------------- stage 1

    def hygiene(self, candidate: str) -> pd.DataFrame:

        self._check_candidate(candidate)

        df       = self.dev
        base_ok  = df[self.baseline + [self.target]].notna().all(axis = 1)
        cand_ok  = df[candidate].notna()

        n_base   = int(base_ok.sum())
        n_lost   = int((base_ok & ~cand_ok).sum())

        group_std = df.groupby(GROUP_COLS)[candidate].std()
        n_const   = int(((group_std == 0) | group_std.isna()).sum())

        n_lookahead = np.nan
        if "prior_meeting_id" in df.columns:
            n_lookahead = int(df
                .loc[lambda x: x.prior_meeting_id.notna()]
                .loc[lambda x: ~(x.prior_meeting_id < x.meeting_id)]
                .shape[0])

        coverage_by_group = df.groupby(GROUP_COLS)[candidate].apply(lambda s: s.notna().mean())

        df_out = pd.DataFrame([{
            "candidate"         : candidate,
            "coverage"          : cand_ok.mean(),
            "min_group_coverage": coverage_by_group.min(),
            "baseline_events"   : n_base,
            "events_lost"       : n_lost,
            "coverage_loss"     : n_lost / n_base if n_base > 0 else np.nan,
            "constant_groups"   : n_const,
            "lookahead_events"  : n_lookahead}])

        return df_out

    # --------------------------------------------------------------- stage 2

    def univariate(self, candidate: str) -> pd.DataFrame:

        # correlation of the candidate with the target, per group and pooled
        # (both z-scored within group, t-stat clustered by meeting)

        self._check_candidate(candidate)
        df = self.dev.dropna(subset = [candidate, self.target])

        rows = []
        for keys, df_group in df.groupby(GROUP_COLS):

            n = len(df_group)
            if n < 5 or df_group[candidate].std() == 0: continue

            r   = df_group[candidate].corr(df_group[self.target])
            rho = df_group[candidate].rank().corr(df_group[self.target].rank())  # spearman
            t   = r * np.sqrt((n - 2) / (1 - r ** 2)) if abs(r) < 1 else np.nan

            rows.append({**dict(zip(GROUP_COLS, keys)), "level": "group", "period": "all",
                         "n": n, "corr": r, "spearman": rho, "tstat": t})

        df_groups = pd.DataFrame(rows)

        df_z = (_zscore_by_group(df, [candidate, self.target])
            .assign(meeting_id = df.meeting_id.values, period = self._periods_of(df).values))

        def pooled(df_sub: pd.DataFrame, period: str) -> dict:

            prod       = df_sub[candidate] * df_sub[self.target]
            corr, tval = _cluster_t(prod, df_sub.meeting_id)
            return {"level": "pooled", "period": period, "n": int(prod.notna().sum()),
                    "corr": corr, "tstat": tval}

        pooled_rows = [pooled(df_z, "all")]
        for period, df_period in df_z.dropna(subset = ["period"]).groupby("period"):
            pooled_rows.append(pooled(df_period, period))

        df_pooled = pd.DataFrame(pooled_rows)

        # sign agreement of the groups with the pooled correlation
        pooled_sign = np.sign(df_pooled.loc[lambda x: x.period == "all", "corr"].iloc[0])
        n_groups    = len(df_groups)
        n_agree     = int((np.sign(df_groups["corr"]) == pooled_sign).sum()) if n_groups > 0 else 0
        p_sign      = stats.binomtest(n_agree, n_groups, 0.5).pvalue if n_groups > 0 else np.nan

        df_pooled = df_pooled.assign(
            groups           = n_groups,
            sign_agree       = n_agree,
            sign_agree_share = n_agree / n_groups if n_groups > 0 else np.nan,
            sign_pvalue      = p_sign)

        df_out = pd.concat([df_pooled, df_groups], ignore_index = True).assign(candidate = candidate)
        return df_out

    # --------------------------------------------------------------- stage 3

    def redundancy(self, candidate: str, others: list = None) -> pd.DataFrame:

        # correlation with each baseline feature (and other candidates), VIF of
        # the candidate on the baseline, and partial correlation: candidate and
        # target residualized on the baseline within each group

        self._check_candidate(candidate)
        others = [col for col in (others or []) if col != candidate and col in self.data.columns]
        rows   = []

        for col in self.baseline + others:

            df  = self.dev.dropna(subset = [candidate, col])
            dfz = _zscore_by_group(df, [candidate, col])

            rows.append({
                "candidate" : candidate,
                "other"     : col,
                "kind"      : "baseline" if col in self.baseline else "candidate",
                "pooled_corr": dfz[candidate].corr(dfz[col]),
                "mean_group_corr": (df
                    .groupby(GROUP_COLS)
                    [[candidate, col]]
                    .apply(lambda x: x[candidate].corr(x[col]))
                    .mean())})

        df_corr = pd.DataFrame(rows, columns = ["candidate", "other", "kind", "pooled_corr", "mean_group_corr"])

        # VIF and partial correlation need a baseline
        vif, partial_corr, partial_t = np.nan, np.nan, np.nan

        if len(self.baseline) > 0:

            df   = self._common(candidate)
            resid_list = []

            for _, df_group in df.groupby(GROUP_COLS):

                if len(df_group) <= len(self.baseline) + 2: continue

                X = np.column_stack([np.ones(len(df_group)), df_group[self.baseline].values])

                def resid(y: np.ndarray) -> np.ndarray:
                    beta = np.linalg.lstsq(X, y, rcond = None)[0]
                    return y - X @ beta

                x_res = resid(df_group[candidate].values.astype(float))
                y_res = resid(df_group[self.target].values.astype(float))
                x_tot = df_group[candidate].values - df_group[candidate].mean()

                resid_list.append(pd.DataFrame({
                    "x_res"     : x_res / (x_res.std() if x_res.std() > 0 else np.nan),
                    "y_res"     : y_res / (y_res.std() if y_res.std() > 0 else np.nan),
                    "x_ss_res"  : x_res ** 2,
                    "x_ss_tot"  : x_tot ** 2,
                    "meeting_id": df_group.meeting_id.values}))

            if len(resid_list) > 0:

                df_res = pd.concat(resid_list, ignore_index = True)
                r2     = 1 - df_res.x_ss_res.sum() / df_res.x_ss_tot.sum()
                vif    = 1 / (1 - r2) if r2 < 1 else np.inf

                partial_corr, partial_t = _cluster_t(df_res.x_res * df_res.y_res, df_res.meeting_id)

        df_corr = df_corr.assign(vif = vif, partial_corr = partial_corr, partial_tstat = partial_t)

        if len(df_corr) == 0:
            df_corr = pd.DataFrame([{"candidate": candidate, "vif": vif,
                                     "partial_corr": partial_corr, "partial_tstat": partial_t}])

        return df_corr

    # --------------------------------------------------------------- stage 4

    def _intercept_only(self, df: pd.DataFrame, model: str) -> pd.DataFrame:

        # expanding forecast with no features: mean of the target (ols) or
        # share of positive events (logit) over the events before t
        rows = []
        for _, df_group in df.sort_values(EVENT_COLS).groupby(GROUP_COLS):

            y = df_group[self.target].astype(float)
            y = (y > 0).astype(float) if model == "logit" else y

            prior_mean = y.expanding().mean().shift()
            n_prior    = pd.Series(np.arange(len(y)), index = y.index)

            df_add = (df_group
                [EVENT_COLS]
                .assign(
                    target     = df_group[self.target].values,
                    prediction = prior_mean.values,
                    last_fit_meeting = df_group.meeting_id.shift().values)
                .loc[(n_prior >= self.min_obs).values])

            rows.append(df_add)

        df_out = pd.concat(rows, ignore_index = True) if len(rows) > 0 else pd.DataFrame(columns = EVENT_COLS)

        if model == "rule":
            df_out["prediction"] = 0.0
        elif model == "logit":
            df_out["prediction"] = df_out.prediction.clip(1e-6, 1 - 1e-6)

        df_out["signal"] = get_model(model).to_signal(df_out.prediction.to_numpy(dtype = float))
        return df_out

    def _oos_fit(self, df: pd.DataFrame, features: list, model: str) -> tuple:

        # expanding out-of-sample predictions (and params) for one feature set
        if len(features) == 0:
            return self._intercept_only(df, model), pd.DataFrame()

        df_pred, df_params = self.event_models.fit_quick(
            df_signals = df[EVENT_COLS + features + [self.target]],
            features   = features,
            models     = [model],
            target     = self.target,
            schemes    = ["out_sample"],
            min_obs    = self.min_obs,
            verbose    = False)

        return df_pred, df_params

    def _compare(self, df: pd.DataFrame, candidate: str, model: str) -> tuple:

        # event-level table of baseline vs candidate predictions on df
        pred_b, _        = self._oos_fit(df, self.baseline, model)
        pred_c, params_c = self._oos_fit(df, self.baseline + [candidate], model)

        if len(pred_b) == 0 or len(pred_c) == 0:
            return pd.DataFrame(), params_c

        # event PnL uses cum_rtn (target if the target is cum_rtn)
        extra  = [col for col in ["cum_rtn", "event_start"] if col in df.columns and not (col == "cum_rtn" and self.target == "cum_rtn")]
        keep   = EVENT_COLS + ["target", "prediction", "signal"]
        df_cmp = (pred_b[keep]
            .merge(
                right    = pred_c[keep].drop(columns = ["target"]),
                how      = "inner",
                on       = EVENT_COLS,
                suffixes = ("_base", "_cand"))
            .merge(right = df[EVENT_COLS + extra], how = "left", on = EVENT_COLS))

        pnl_col = "cum_rtn" if "cum_rtn" in df_cmp.columns else "target"
        y       = df_cmp.target.astype(float)

        df_cmp = df_cmp.assign(
            model    = model,
            hit_base = (np.sign(df_cmp.signal_base) == np.sign(y)).astype(float),
            hit_cand = (np.sign(df_cmp.signal_cand) == np.sign(y)).astype(float),
            pnl_base = np.sign(df_cmp.signal_base) * df_cmp[pnl_col],
            pnl_cand = np.sign(df_cmp.signal_cand) * df_cmp[pnl_col])

        if model == "ols":

            e_b = y - df_cmp.prediction_base
            e_c = y - df_cmp.prediction_cand

            # Clark-West: positive when the larger model forecasts better
            df_cmp = df_cmp.assign(
                loss_base = e_b ** 2,
                loss_cand = e_c ** 2,
                loss_diff = e_b ** 2 - e_c ** 2,
                test_stat = e_b ** 2 - (e_c ** 2 - (df_cmp.prediction_base - df_cmp.prediction_cand) ** 2))

        elif model == "logit":

            y_bin = (y > 0).astype(float)

            def logloss(p: pd.Series) -> pd.Series:
                p = p.astype(float).clip(1e-12, 1 - 1e-12)
                return -(y_bin * np.log(p) + (1 - y_bin) * np.log(1 - p))

            l_b, l_c = logloss(df_cmp.prediction_base), logloss(df_cmp.prediction_cand)

            df_cmp = df_cmp.assign(
                loss_base = l_b,
                loss_cand = l_c,
                loss_diff = l_b - l_c,
                test_stat = l_b - l_c)

        else:

            # rule: hit rate is the loss
            df_cmp = df_cmp.assign(
                loss_base = 1 - df_cmp.hit_base,
                loss_cand = 1 - df_cmp.hit_cand,
                loss_diff = df_cmp.hit_cand - df_cmp.hit_base,
                test_stat = df_cmp.hit_cand - df_cmp.hit_base)

        df_cmp["period"] = self._periods_of(df_cmp).values
        return df_cmp, params_c

    def _summarize(self, df_cmp: pd.DataFrame, candidate: str, model: str, period: str = "all") -> dict:

        if len(df_cmp) == 0:
            return {"candidate": candidate, "model": model, "period": period, "events": 0}

        stat_mean, stat_t = _cluster_t(df_cmp.test_stat, df_cmp.meeting_id)
        hit_mean , hit_t  = _cluster_t(df_cmp.hit_cand - df_cmp.hit_base, df_cmp.meeting_id)
        pnl_mean , pnl_t  = _cluster_t(df_cmp.pnl_cand - df_cmp.pnl_base, df_cmp.meeting_id)

        return {
            "candidate"    : candidate,
            "model"        : model,
            "period"       : period,
            "events"       : len(df_cmp),
            "meetings"     : df_cmp.meeting_id.nunique(),
            "loss_base"    : df_cmp.loss_base.mean(),
            "loss_cand"    : df_cmp.loss_cand.mean(),
            "loss_diff"    : df_cmp.loss_diff.mean(),   # > 0: candidate better
            "test_mean"    : stat_mean,
            "test_tstat"   : stat_t,                    # Clark-West (ols) / DM (logit)
            "test_pvalue"  : stats.norm.sf(stat_t) if pd.notna(stat_t) else np.nan,  # one-sided
            "hit_base"     : df_cmp.hit_base.mean(),
            "hit_cand"     : df_cmp.hit_cand.mean(),
            "hit_diff_t"   : hit_t,
            "pnl_diff"     : pnl_mean,
            "pnl_diff_t"   : pnl_t,
            "pnl_diff_p"   : 2 * stats.norm.sf(abs(pnl_t)) if pd.notna(pnl_t) else np.nan,
            "sharpe_base"  : _event_sharpe(df_cmp, "pnl_base"),
            "sharpe_cand"  : _event_sharpe(df_cmp, "pnl_cand")}

    def _insample_f(self, df: pd.DataFrame, candidate: str) -> dict:

        # secondary: nested in-sample OLS F-test per group
        pvalues = []
        for _, df_group in df.groupby(GROUP_COLS):

            n, k = len(df_group), len(self.baseline) + 2
            if n <= k + 1: continue

            y   = df_group[self.target].values.astype(float)
            X_r = np.column_stack([np.ones(n)] + [df_group[col].values for col in self.baseline])
            X_f = np.column_stack([X_r, df_group[candidate].values])

            ssr = lambda X: np.sum((y - X @ np.linalg.lstsq(X, y, rcond = None)[0]) ** 2)
            ssr_r, ssr_f = ssr(X_r), ssr(X_f)

            if ssr_f <= 0: continue
            F = (ssr_r - ssr_f) / (ssr_f / (n - k))
            pvalues.append(stats.f.sf(F, 1, n - k))

        pvalues = pd.Series(pvalues, dtype = float)
        return {
            "insample_f_median_p": pvalues.median(),
            "insample_f_share_05": (pvalues < 0.05).mean() if len(pvalues) > 0 else np.nan}

    def incremental(self, candidate: str, return_events: bool = False):

        # baseline vs baseline + candidate, expanding out-of-sample on the
        # same development events
        self._check_candidate(candidate)
        df = self._common(candidate)

        summaries, events, params = [], [], []
        for model in self.models:

            df_cmp, df_params = self._compare(df, candidate, model)
            summaries.append({**self._summarize(df_cmp, candidate, model), **self._insample_f(df, candidate)})
            events.append(df_cmp)

            if len(df_params) > 0: params.append(df_params)

        df_out = pd.DataFrame(summaries)

        if return_events:
            df_events = pd.concat([e for e in events if len(e) > 0], ignore_index = True) if any(len(e) > 0 for e in events) else pd.DataFrame()
            df_params = pd.concat(params, ignore_index = True) if len(params) > 0 else pd.DataFrame()
            return df_out, df_events, df_params

        return df_out

    # --------------------------------------------------------------- stage 5

    def stability(self, candidate: str, df_events: pd.DataFrame = None, df_params: pd.DataFrame = None) -> tuple:

        # incremental by sub-period and the sign history of the candidate's
        # expanding coefficient. Pass the events / params from incremental to
        # avoid refitting
        if df_events is None or df_params is None:
            _, df_events, df_params = self.incremental(candidate, return_events = True)

        rows = []
        if len(df_events) > 0:
            for (model, period), df_sub in df_events.dropna(subset = ["period"]).groupby(["model", "period"]):
                rows.append(self._summarize(df_sub, candidate, model, period))

        df_period = pd.DataFrame(rows)

        coef_rows = []
        if len(df_params) > 0 and "param" in df_params.columns:

            df_coef = (df_params
                .loc[lambda x: x.param == candidate]
                .dropna(subset = ["predict_meeting"])
                .sort_values("predict_meeting"))

            for keys, df_group in df_coef.groupby(["model"] + GROUP_COLS):

                sign       = np.sign(df_group.coeff)
                final_sign = sign.iloc[-1]

                coef_rows.append({
                    **dict(zip(["model"] + GROUP_COLS, keys)),
                    "fits"            : len(sign),
                    "final_coeff"     : df_group.coeff.iloc[-1],
                    "share_off_final" : (sign != final_sign).mean(),
                    "sign_changes"    : int((sign.diff().fillna(0) != 0).sum())})

        df_coef_out = pd.DataFrame(coef_rows).assign(candidate = candidate) if len(coef_rows) > 0 else pd.DataFrame()

        return df_period, df_coef_out

    # ---------------------------------------------------------------- run

    def _flags(self, row: dict) -> dict:

        th = self.thresholds

        lookahead    = row.get("lookahead_events", np.nan)
        pass_hygiene = (
            (row.get("coverage_loss", np.nan) <= th["max_coverage_loss"]) and
            (pd.isna(lookahead) or lookahead == 0))

        pass_signal     = row.get("sign_agree_share", np.nan) >= th["min_sign_agree"]
        pass_redundancy = not (row.get("max_baseline_corr", 0) >= th["max_baseline_corr"])
        pass_increment  = row.get("{}_adj_pvalue".format(self.primary), np.nan) <= th["max_adj_pvalue"]
        pass_stability  = not (row.get("{}_worst_period_t".format(self.primary), 0) <= th["min_period_tstat"])

        flags = {
            "pass_hygiene"    : bool(pass_hygiene),
            "pass_signal"     : bool(pass_signal),
            "pass_redundancy" : bool(pass_redundancy),
            "pass_incremental": bool(pass_increment),
            "pass_stability"  : bool(pass_stability)}

        flags["pass_all"] = all(flags.values())
        return flags

    def _folder(self, candidate: str) -> str:

        base = "-".join(self.baseline) if len(self.baseline) > 0 else "intercept"
        return os.path.join(self.test_path, "{}__{}".format(base, candidate))

    def _spec(self, candidate: str) -> dict:

        return {
            "baseline"     : self.baseline,
            "candidate"    : candidate,
            "scheme"       : self.scheme,
            "target"       : self.target,
            "models"       : self.models,
            "primary"      : self.primary,
            "min_obs"      : self.min_obs,
            "holdout"      : self.holdout,
            "holdout_start": self.holdout_start,
            "periods"      : self.periods,
            "thresholds"   : self.thresholds}

    def run(self, candidates: list, save: bool = False, overwrite: bool = False, verbose: bool = True) -> tuple:

        # Runs every stage for each candidate. Returns (summary, details):
        #   summary: one row per candidate with the key numbers, BH adjusted
        #            p-values across candidates and pass flags
        #   details: {table name: DataFrame} stacked across candidates
        # With save = True each candidate gets data/FeatureTests/<baseline>__<candidate>/
        # and an existing folder is loaded instead of refit unless overwrite

        rows    = []
        details = {key: [] for key in ["hygiene", "univariate", "redundancy", "incremental", "periods", "coefficients"]}

        for candidate in candidates:

            self._check_candidate(candidate)
            folder = self._folder(candidate)

            if save and not overwrite and os.path.exists(os.path.join(folder, "Summary.parquet")):

                if verbose: print("Already have {}, loading".format(candidate))
                rows.append(pd.read_parquet(os.path.join(folder, "Summary.parquet")).iloc[0].to_dict())

                for key in details.keys():
                    path = os.path.join(folder, "{}.parquet".format(key.capitalize()))
                    if os.path.exists(path): details[key].append(pd.read_parquet(path))

                continue

            if verbose: print("Testing {} against {}".format(candidate, self.baseline if self.baseline else "intercept"))

            df_hyg = self.hygiene(candidate)
            df_uni = self.univariate(candidate)
            df_red = self.redundancy(candidate, others = [col for col in candidates if col != candidate])

            df_inc, df_events, df_params = self.incremental(candidate, return_events = True)
            df_per, df_coef              = self.stability(candidate, df_events, df_params)

            pooled = df_uni.loc[lambda x: (x.level == "pooled") & (x.period == "all")].iloc[0]
            base_r = df_red.loc[lambda x: x.kind == "baseline"] if "kind" in df_red.columns else pd.DataFrame()

            row = {
                "candidate"        : candidate,
                **df_hyg.iloc[0].drop(labels = ["candidate"]).to_dict(),
                "pooled_corr"      : pooled["corr"],
                "pooled_corr_t"    : pooled["tstat"],
                "sign_agree"       : pooled["sign_agree"],
                "groups"           : pooled["groups"],
                "sign_agree_share" : pooled["sign_agree_share"],
                "sign_pvalue"      : pooled["sign_pvalue"],
                "max_baseline_corr": base_r.pooled_corr.abs().max() if len(base_r) > 0 else np.nan,
                "vif"              : df_red.vif.iloc[0],
                "partial_corr"     : df_red.partial_corr.iloc[0],
                "partial_tstat"    : df_red.partial_tstat.iloc[0]}

            for _, r in df_inc.iterrows():

                m = r["model"]
                row.update({
                    "{}_events".format(m)     : r.get("events"),
                    "{}_loss_diff".format(m)  : r.get("loss_diff"),
                    "{}_tstat".format(m)      : r.get("test_tstat"),
                    "{}_pvalue".format(m)     : r.get("test_pvalue"),
                    "{}_hit_base".format(m)   : r.get("hit_base"),
                    "{}_hit_cand".format(m)   : r.get("hit_cand"),
                    "{}_pnl_diff_t".format(m) : r.get("pnl_diff_t"),
                    "{}_sharpe_base".format(m): r.get("sharpe_base"),
                    "{}_sharpe_cand".format(m): r.get("sharpe_cand")})

                if len(df_per) > 0:
                    row["{}_worst_period_t".format(m)] = df_per.loc[lambda x: x.model == m].test_tstat.min()

                if len(df_coef) > 0:
                    row["{}_coef_off_final".format(m)] = df_coef.loc[lambda x: x.model == m].share_off_final.mean()

            row.update({
                "insample_f_median_p": df_inc.insample_f_median_p.iloc[0],
                "insample_f_share_05": df_inc.insample_f_share_05.iloc[0]})

            rows.append(row)

            for key, df_add in zip(
                    details.keys(),
                    [df_hyg, df_uni, df_red, df_inc, df_per, df_coef]):
                if len(df_add) > 0: details[key].append(df_add.assign(candidate = candidate))

            if save:

                if not os.path.exists(folder): os.makedirs(folder)

                for key, df_add in zip(
                        details.keys(),
                        [df_hyg, df_uni, df_red, df_inc, df_per, df_coef]):
                    if len(df_add) > 0:
                        df_add.to_parquet(os.path.join(folder, "{}.parquet".format(key.capitalize())), engine = "pyarrow")

                with open(os.path.join(folder, "Spec.json"), "w") as f:
                    json.dump(self._spec(candidate), f, indent = 4, default = str)

        df_sum = pd.DataFrame(rows)

        # multiple testing: BH across the candidates in this run, per model
        for m in self.models:
            col = "{}_pvalue".format(m)
            if col in df_sum.columns:
                df_sum["{}_adj_pvalue".format(m)] = _bh_adjust(df_sum[col]).values

        df_flags = pd.DataFrame([self._flags(row) for row in df_sum.to_dict("records")])
        df_sum   = pd.concat([df_sum.reset_index(drop = True), df_flags], axis = 1)

        if save:
            for row in df_sum.to_dict("records"):
                folder = self._folder(row["candidate"])
                if os.path.exists(folder):
                    pd.DataFrame([row]).to_parquet(os.path.join(folder, "Summary.parquet"), engine = "pyarrow")

        details = {
            key: pd.concat(value, ignore_index = True) if len(value) > 0 else pd.DataFrame()
            for key, value in details.items()}

        if verbose:
            cols = ["candidate", "coverage_loss", "pooled_corr", "sign_agree_share", "max_baseline_corr",
                    "{}_loss_diff".format(self.primary), "{}_adj_pvalue".format(self.primary), "pass_all"]
            print(df_sum[[col for col in cols if col in df_sum.columns]])

        return df_sum, details

    # -------------------------------------------------------------- final

    def final_check(self, candidate: str) -> pd.DataFrame:

        # incremental test scored only on the held-out meetings. The expanding
        # fits run over all meetings (still out-of-sample at every step) and
        # only the holdout predictions are kept. Use once per feature
        warnings.warn("final_check uses the holdout meetings; run it once per feature, after deciding on it")

        self._check_candidate(candidate)
        df = self._common(candidate, self.data)

        rows = []
        for model in self.models:
            df_cmp, _ = self._compare(df, candidate, model)
            df_cmp    = df_cmp.loc[lambda x: x.meeting_id >= self.holdout_start] if len(df_cmp) > 0 else df_cmp
            rows.append(self._summarize(df_cmp, candidate, model, period = "holdout"))

        return pd.DataFrame(rows).assign(holdout_start = self.holdout_start)
