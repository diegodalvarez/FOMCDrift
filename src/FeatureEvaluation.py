# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 2026

Evaluates candidate features on the event tables built by EventFeatures.py,
one stage at a time. Each stage is a method that returns DataFrames and
prints a short report in the same style as the rest of src/.

    stage        status
    hygiene      built   is the feature usable and free of look-ahead
    (next)               univariate, redundancy, strategy comparison,
                         incremental out-of-sample, stability

    evaluator = FeatureEvaluator(baseline = ["lag_mean_rtn"])
    hygiene   = evaluator.hygiene(["lag_median_rtn", "lag_std_rtn"])
    hygiene["summary"]

Hygiene checks, per feature (severity in brackets)
    exists / numeric                         [fail]
    infinite values                          [fail]
    coverage below min_coverage              [warn]
    baseline events lost above max_events_lost  [warn]
    constant in a ticker x zone group        [warn]
    look-ahead: prior_meeting_id not before meeting_id (table level)  [fail]
    look-ahead: near copy of a same-event column (TARGETS)            [fail]
    correlation with the current target implausibly high              [warn]
    few distinct values (discrete)           [info]
    extreme within-group z-scores            [warn]
    never negative, not centred on zero      [info]

A feature passes hygiene when it has no fail items.
"""

import os
import numpy as np
import pandas as pd

from EventFeatures import EventFeatures, TARGETS, EVENT_COLS, GROUP_COLS

HYGIENE_THRESHOLDS = {
    "min_coverage"      : 0.95,  # share of events with a value
    "max_events_lost"   : 0.05,  # share of baseline events the feature drops
    "leak_corr"         : 0.95,  # |corr| with a same-event column that marks a leak
    "suspect_corr"      : 0.30,  # |corr| with the current target worth a second look
    "min_unique"        : 10,    # median distinct values per group below this is discrete
    "outlier_z"         : 10.0,  # max |within-group z| above this is extreme
    "tail_z"            : 5.0}   # z used for the tail share


class FeatureEvaluator:

    def __init__(
            self,
            baseline  : list = [],
            data      : pd.DataFrame = None,
            scheme    : str  = "out_sample",
            target    : str  = "cum_rtn",
            vol_types : list = ["combined"]) -> None:

        # baseline : features already in the model, used for events lost
        # data     : event table with EVENT_COLS, the target and the features.
        #            Defaults to EventFeatures().load(scheme)
        # vol_types: filter on vol_type, None keeps all

        self.src_path  = os.getcwd()
        self.repo_path = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path = os.path.join(self.repo_path, "data")
        self.eval_path = os.path.join(self.data_path, "FeatureEvaluation")

        self.baseline = list(baseline)
        self.scheme   = scheme
        self.target   = target

        if data is None:
            data = EventFeatures().load(scheme)

        data = data.assign(meeting_id = lambda x: x.meeting_id.astype(int))

        if vol_types is not None and "vol_type" in data.columns:
            data = data.loc[lambda x: x.vol_type.isin(vol_types)]

        missing = [col for col in EVENT_COLS + [target] + self.baseline if col not in data.columns]
        if len(missing) > 0:
            raise ValueError("{} not in data".format(missing))

        self.data = (data
            .sort_values(EVENT_COLS)
            .reset_index(drop = True))

    # ------------------------------------------------------------ helpers

    def _same_event_cols(self) -> list:

        # columns that describe the event itself (known only after it ends)
        return [col for col in TARGETS.keys() if col in self.data.columns]

    # ------------------------------------------------------------ hygiene

    def _table_checks(self) -> dict:

        # checks on the event table itself, shared by every feature
        n_dup = int(self.data.duplicated(subset = EVENT_COLS).sum())

        n_lookahead = np.nan
        if "prior_meeting_id" in self.data.columns:
            n_lookahead = int(self.data
                .loc[lambda x: x.prior_meeting_id.notna()]
                .loc[lambda x: ~(x.prior_meeting_id < x.meeting_id)]
                .shape[0])

        return {"duplicated_events": n_dup, "lookahead_rows": n_lookahead}

    def _feature_groups(self, feature: str, data: pd.DataFrame) -> pd.DataFrame:

        # per ticker x zone: coverage, spread, distinct values, first meeting
        def stats(df: pd.DataFrame) -> pd.Series:

            s      = df[feature]
            valid  = s.dropna()
            counts = valid.value_counts(normalize = True)

            return pd.Series({
                "events"       : len(s),
                "coverage"     : s.notna().mean(),
                "std"          : valid.std(),
                "n_unique"     : valid.nunique(),
                "mode_share"   : counts.iloc[0] if len(counts) > 0 else np.nan,
                "share_zero"   : (valid == 0).mean() if len(valid) > 0 else np.nan,
                "first_meeting": df.loc[s.notna(), "meeting_id"].min(),
                "last_meeting" : df.loc[s.notna(), "meeting_id"].max()})

        df_out = (data
            .groupby(GROUP_COLS)
            [[feature, "meeting_id"]]
            .apply(stats)
            .reset_index()
            .assign(feature = feature))

        return df_out

    def _feature_summary(self, feature: str, data: pd.DataFrame, df_group: pd.DataFrame, table: dict, th: dict) -> tuple:

        # data has infinite values already set to NaN, they are counted on
        # the raw column so they don't distort the other statistics
        s     = data[feature]
        valid = s.dropna()
        flags = []

        # events lost: usable for the baseline + target, not with the feature
        base_ok  = self.data[self.baseline + [self.target]].notna().all(axis = 1)
        n_base   = int(base_ok.sum())
        n_lost   = int((base_ok & s.isna()).sum())
        lost_shr = n_lost / n_base if n_base > 0 else np.nan

        # look-ahead: near copies of same-event columns, and the current target
        same_event = {
            col: s.corr(self.data[col])
            for col in self._same_event_cols()
            if self.data[col].notna().sum() > 2}

        leak_col  = max(same_event, key = lambda k: abs(same_event[k])) if len(same_event) > 0 else None
        leak_corr = same_event[leak_col] if leak_col is not None else np.nan
        tgt_corr  = s.corr(self.data[self.target])

        grouped   = data.groupby(GROUP_COLS)[feature]
        z         = (s - grouped.transform("mean")) / grouped.transform("std")
        n_inf     = int(np.isinf(self.data[feature]).sum())
        n_const   = int(((df_group["std"] == 0) | df_group["std"].isna()).sum())
        med_uniq  = df_group.n_unique.median()

        row = {
            "feature"           : feature,
            "events"            : len(s),
            "coverage"          : s.notna().mean(),
            "min_group_coverage": df_group.coverage.min(),
            "latest_first_meeting": df_group.first_meeting.max(),
            "baseline_events"   : n_base,
            "events_lost"       : n_lost,
            "events_lost_share" : lost_shr,
            "infinite_values"   : n_inf,
            "constant_groups"   : n_const,
            "median_n_unique"   : med_uniq,
            "median_mode_share" : df_group.mode_share.median(),
            "max_abs_z"         : z.abs().max(),
            "tail_share"        : (z.abs() > th["tail_z"]).mean(),
            "share_negative"    : (valid < 0).mean() if len(valid) > 0 else np.nan,
            "same_event_col"    : leak_col,
            "same_event_corr"   : leak_corr,
            "target_corr"       : tgt_corr,
            **table}

        # ------------------------------------------------ flags
        if n_inf > 0:
            flags.append(("fail", "{} infinite values".format(n_inf)))

        if pd.notna(table["lookahead_rows"]) and table["lookahead_rows"] > 0:
            flags.append(("fail", "{} rows lag from a meeting on or after their own".format(table["lookahead_rows"])))

        if pd.notna(leak_corr) and abs(leak_corr) >= th["leak_corr"]:
            flags.append(("fail", "near copy of same-event {} (corr {:.2f})".format(leak_col, leak_corr)))

        if pd.notna(tgt_corr) and abs(tgt_corr) >= th["suspect_corr"]:
            flags.append(("warn", "corr {:.2f} with the current target, check timing".format(tgt_corr)))

        if row["coverage"] < th["min_coverage"]:
            flags.append(("warn", "coverage {:.1%}".format(row["coverage"])))

        if pd.notna(lost_shr) and lost_shr > th["max_events_lost"]:
            flags.append(("warn", "drops {:.1%} of baseline events".format(lost_shr)))

        if n_const > 0:
            flags.append(("warn", "constant in {} groups".format(n_const)))

        if pd.notna(row["max_abs_z"]) and row["max_abs_z"] > th["outlier_z"]:
            flags.append(("warn", "max |z| {:.1f}".format(row["max_abs_z"])))

        if pd.notna(med_uniq) and med_uniq < th["min_unique"]:
            flags.append(("info", "discrete, median {:.0f} distinct values per group".format(med_uniq)))

        if row["share_negative"] == 0:
            flags.append(("info", "never negative, not centred on zero"))

        row.update({
            "fails"       : sum(1 for sev, _ in flags if sev == "fail"),
            "warnings"    : sum(1 for sev, _ in flags if sev == "warn"),
            "flags"       : "; ".join("[{}] {}".format(sev, msg) for sev, msg in flags),
            "pass_hygiene": not any(sev == "fail" for sev, _ in flags)})

        return row, flags

    def hygiene(self, features: list, thresholds: dict = None, save: bool = False, verbose: bool = True) -> dict:

        # Returns {"summary": one row per feature,
        #          "groups" : one row per feature x ticker x zone,
        #          "time"   : coverage per feature x year (if event_start exists)}
        # Features that don't exist or aren't numeric get a summary row with
        # the fail and no other stats

        th    = {**HYGIENE_THRESHOLDS, **(thresholds or {})}
        table = self._table_checks()

        if verbose:
            print("Feature hygiene on {} events ({})".format(len(self.data), self.scheme))
            if table["duplicated_events"] > 0: print("There are {} duplicated events".format(table["duplicated_events"]))
            else: print("Passed Duplicate Event Check")

        summary_rows, group_list, time_list = [], [], []

        for feature in features:

            if feature not in self.data.columns:
                summary_rows.append({"feature": feature, "fails": 1, "warnings": 0,
                                     "flags": "[fail] not in data", "pass_hygiene": False})
                continue

            if not pd.api.types.is_numeric_dtype(self.data[feature]):
                summary_rows.append({"feature": feature, "fails": 1, "warnings": 0,
                                     "flags": "[fail] not numeric ({})".format(self.data[feature].dtype),
                                     "pass_hygiene": False})
                continue

            data       = self.data.assign(**{feature: self.data[feature].replace([np.inf, -np.inf], np.nan)})
            df_group   = self._feature_groups(feature, data)
            row, flags = self._feature_summary(feature, data, df_group, table, th)

            summary_rows.append(row)
            group_list.append(df_group)

            if "event_start" in self.data.columns:
                time_list.append(data
                    .assign(year = lambda x: pd.to_datetime(x.event_start).dt.year)
                    .groupby("year")
                    [feature]
                    .apply(lambda s: s.notna().mean())
                    .to_frame(name = "coverage")
                    .reset_index()
                    .assign(feature = feature))

            if verbose:
                status = "Passed" if row["pass_hygiene"] else "Failed"
                print("{} Hygiene Check: {}".format(status, feature))
                for sev, msg in flags: print("    [{}] {}".format(sev, msg))

        out = {
            "summary": pd.DataFrame(summary_rows),
            "groups" : pd.concat(group_list, ignore_index = True) if len(group_list) > 0 else pd.DataFrame(),
            "time"   : pd.concat(time_list , ignore_index = True) if len(time_list)  > 0 else pd.DataFrame()}

        if save:

            out_dir = os.path.join(self.eval_path, "Hygiene")
            if not os.path.exists(out_dir): os.makedirs(out_dir)

            for name, df in out.items():
                if len(df) > 0:
                    df.to_parquet(os.path.join(out_dir, "{}.parquet".format(name.capitalize())), engine = "pyarrow")

            if verbose: print("Saving data\n")

        return out

    # -------------------------------------------------------- association

    def _wide(self, feature: str) -> tuple:

        # meeting_id x group arrays for the feature and the target, aligned on
        # the same rows and columns. Infinite values are treated as missing
        df_wide = (self.data
            .assign(**{
                feature    : lambda x: x[feature].replace([np.inf, -np.inf], np.nan),
                self.target: lambda x: x[self.target].replace([np.inf, -np.inf], np.nan)})
            .set_index(["meeting_id"] + GROUP_COLS)
            [[feature, self.target]]
            .unstack(GROUP_COLS)
            .sort_index())

        x      = df_wide[feature].to_numpy(dtype = float)
        y      = df_wide[self.target].to_numpy(dtype = float)
        groups = df_wide[feature].columns.to_frame(index = False)

        return x, y, groups

    def _col_corr(self, x: np.ndarray, y: np.ndarray, mask: np.ndarray) -> np.ndarray:

        # column-wise Pearson correlation using only rows where both exist
        with np.errstate(invalid = "ignore", divide = "ignore"):

            n  = mask.sum(axis = 0)
            xm = np.where(mask, x, 0).sum(axis = 0) / n
            ym = np.where(mask, y, 0).sum(axis = 0) / n
            xd = np.where(mask, x - xm, 0)
            yd = np.where(mask, y - ym, 0)

            cov = (xd * yd).sum(axis = 0)
            den = np.sqrt((xd ** 2).sum(axis = 0) * (yd ** 2).sum(axis = 0))
            r   = cov / den

        r[(den == 0) | (n < 3)] = np.nan

        return r

    def _assoc_stats(self, x: np.ndarray, y: np.ndarray) -> dict:

        # per group: events, Pearson, Spearman (Pearson on ranks of the shared
        # rows) and sign agreement (rows where both are non-zero)
        mask = ~np.isnan(x) & ~np.isnan(y)

        x_rank = pd.DataFrame(np.where(mask, x, np.nan)).rank(axis = 0).to_numpy()
        y_rank = pd.DataFrame(np.where(mask, y, np.nan)).rank(axis = 0).to_numpy()

        signed = mask & (np.sign(np.where(mask, x, 0)) != 0) & (np.sign(np.where(mask, y, 0)) != 0)
        n_sign = signed.sum(axis = 0)

        with np.errstate(invalid = "ignore", divide = "ignore"):
            agree = (signed & (np.sign(np.where(signed, x, 0)) == np.sign(np.where(signed, y, 0)))).sum(axis = 0) / n_sign

        return {
            "n"         : mask.sum(axis = 0),
            "pearson"   : self._col_corr(x, y, mask),
            "spearman"  : self._col_corr(x_rank, y_rank, mask),
            "sign_agree": agree,
            "n_sign"    : n_sign}

    def _pool(self, stats: dict, keep: np.ndarray) -> dict:

        # equal weight average across the groups kept, so groups with larger
        # returns don't dominate
        with np.errstate(invalid = "ignore"):
            return {
                "pearson"       : np.nanmean(stats["pearson"][keep])    if keep.any() else np.nan,
                "spearman"      : np.nanmean(stats["spearman"][keep])   if keep.any() else np.nan,
                "sign_agree"    : np.nanmean(stats["sign_agree"][keep]) if keep.any() else np.nan,
                "share_positive": np.nanmean(stats["pearson"][keep] > 0) if keep.any() else np.nan}

    def association(
            self,
            features        : list,
            n_perm          : int  = 1000,
            null            : str  = "permute",
            min_obs         : int  = 30,
            include_baseline: bool = True,
            seed            : int  = 0,
            save            : bool = False,
            verbose         : bool = True) -> dict:

        # Model-free association between each feature and the target, per
        # ticker x zone x name x vol_type group and pooled across groups.
        #
        # Per group: Pearson, Spearman (rank) and sign agreement, the share of
        # events where sign(feature) == sign(target), both non-zero.
        # Pooled: equal weight mean across groups with at least min_obs events,
        # plus the share of groups with a positive correlation.
        #
        # p-values come from an empirical null built n_perm times:
        #   null = "permute": shuffle which meeting the feature belongs to. The
        #          same shuffle is used for every group, so all contracts and
        #          zones of a meeting move together and the cross-sectional
        #          correlation is kept
        #   null = "shift"  : rotate the meetings by a random offset instead,
        #          which also keeps the feature's own autocorrelation
        # Two-sided: |null| >= |observed| (sign agreement around 0.5).
        #
        # include_baseline adds the baseline features as rows (role =
        # baseline) so candidates can be read against them.
        #
        # Returns {"summary": one row per feature,
        #          "groups" : one row per feature x group,
        #          "null"   : one row per feature x draw, the pooled null stats}

        if null not in ["permute", "shift"]:
            raise ValueError("null must be permute or shift")

        n_dup = int(self.data.duplicated(subset = EVENT_COLS).sum())
        if n_dup > 0:
            raise ValueError("There are {} duplicated events, run hygiene".format(n_dup))

        roles = {feature: "candidate" for feature in features}
        if include_baseline:
            roles = {**{feature: "baseline" for feature in self.baseline if feature not in roles}, **roles}

        bad = [col for col in roles if col not in self.data.columns or not pd.api.types.is_numeric_dtype(self.data[col])]
        if len(bad) > 0:
            raise ValueError("{} missing or not numeric, run hygiene".format(bad))

        if verbose:
            print("Feature association on {} events ({}), {} {} draws".format(len(self.data), self.scheme, n_perm, null))

        rng = np.random.default_rng(seed)
        summary_rows, group_list, null_list = [], [], []

        for feature, role in roles.items():

            x, y, groups = self._wide(feature)
            n_meet       = x.shape[0]

            obs  = self._assoc_stats(x, y)
            keep = obs["n"] >= min_obs
            pool = self._pool(obs, keep)

            # null draws, the same rng stream so every feature sees its own
            # draws but the run is reproducible
            null_rows  = []
            null_group = np.full((n_perm, x.shape[1]), np.nan)

            for draw in range(n_perm):

                if null == "permute":
                    order = rng.permutation(n_meet)
                else:
                    order = np.roll(np.arange(n_meet), rng.integers(1, n_meet))

                stats_null       = self._assoc_stats(x[order], y)
                null_group[draw] = stats_null["pearson"]
                null_rows.append({"draw": draw, **self._pool(stats_null, keep)})

            df_null = pd.DataFrame(null_rows).assign(feature = feature)

            def p_value(stat: str, centre: float = 0.0) -> float:
                if pd.isna(pool[stat]): return np.nan
                hits = (df_null[stat] - centre).abs() >= abs(pool[stat] - centre)
                return (1 + hits.sum()) / (1 + df_null[stat].notna().sum())

            with np.errstate(invalid = "ignore"):
                p_group = (1 + (np.abs(null_group) >= np.abs(obs["pearson"])).sum(axis = 0)) / (1 + np.isfinite(null_group).sum(axis = 0))
            p_group[np.isnan(obs["pearson"])] = np.nan

            df_group = (groups
                .assign(
                    feature    = feature,
                    role       = role,
                    n          = obs["n"],
                    pearson    = obs["pearson"],
                    spearman   = obs["spearman"],
                    sign_agree = obs["sign_agree"],
                    n_sign     = obs["n_sign"],
                    p_pearson  = p_group,
                    pooled     = keep))

            row = {
                "feature"         : feature,
                "role"            : role,
                "events"          : int(obs["n"].sum()),
                "groups"          : int(keep.sum()),
                "pearson"         : pool["pearson"],
                "p_pearson"       : p_value("pearson"),
                "spearman"        : pool["spearman"],
                "p_spearman"      : p_value("spearman"),
                "sign_agree"      : pool["sign_agree"],
                "p_sign_agree"    : p_value("sign_agree", 0.5),
                "share_positive"  : pool["share_positive"],
                "null_sd_pearson" : df_null.pearson.std(),
                "groups_p_below_5": int((p_group[keep] < 0.05).sum())}

            summary_rows.append(row)
            group_list.append(df_group)
            null_list.append(df_null)

            if verbose:
                print("{} ({}): corr {:.3f} (p {:.3f}), rank corr {:.3f} (p {:.3f}), sign agree {:.1%} (p {:.3f}), positive in {:.0%} of {} groups".format(
                    feature, role,
                    row["pearson"], row["p_pearson"],
                    row["spearman"], row["p_spearman"],
                    row["sign_agree"], row["p_sign_agree"],
                    row["share_positive"], row["groups"]))

        out = {
            "summary": pd.DataFrame(summary_rows),
            "groups" : pd.concat(group_list, ignore_index = True),
            "null"   : pd.concat(null_list , ignore_index = True)}

        if save:

            out_dir = os.path.join(self.eval_path, "Association")
            if not os.path.exists(out_dir): os.makedirs(out_dir)

            for name, df in out.items():
                if len(df) > 0:
                    df.to_parquet(os.path.join(out_dir, "{}.parquet".format(name.capitalize())), engine = "pyarrow")

            if verbose: print("Saving data\n")

        return out
