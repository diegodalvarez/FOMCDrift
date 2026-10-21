# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 2026

Builds one event-level table per scheme from the volatility normalized
returns in data/VolatilityNormalizedReturns/ and saves it to
data/EventFeatures/

    scheme       reads / writes
    in_sample    InSample.parquet
    train_test   TrainTest.parquet
    out_sample   OutSample.parquet

One row per ticker x zone x name x vol_type x meeting_id with
    split            full / train / test / oos (from the vol normalized returns)
    event_start      first minute of the event
    n_bars           number of non-missing minute returns in the event
    <targets>        one column per entry in TARGETS (endog candidates)
    prior_meeting_id meeting the lagged features come from
    <features>       one column per entry in FEATURES (exog candidates)

Targets describe the event itself. Features may only use information from
meetings before the event, in every scheme (in-sample included), so the only
look-ahead in an in-sample result comes from fitting a model on the full
sample, never from how the features are built.

To add a target: add a name -> aggregation over the event's minute vol_rtn
to TARGETS (a pandas aggregation name or a function of a Series).

To add a feature: add a name -> function of the event table to FEATURES. The
table is sorted by GROUP_COLS + meeting_id and holds every target column;
use _prior_event to lag a column one event within its group.
"""

import os
import numpy as np
import pandas as pd

SCHEME_FILES = {
    "in_sample" : "InSample.parquet",
    "train_test": "TrainTest.parquet",
    "out_sample": "OutSample.parquet"}

GROUP_COLS = ["ticker", "zone", "name", "vol_type"]
EVENT_COLS = GROUP_COLS + ["meeting_id"]

def _prior_event(df: pd.DataFrame, col: str, n: int = 1) -> pd.Series:

    # value of col n events earlier within the same ticker / zone / vol_type.
    # df must be sorted by GROUP_COLS + meeting_id
    return df.groupby(GROUP_COLS)[col].shift(n)

# endog candidates: aggregation of the event's minute vol_rtn
TARGETS = {
    "cum_rtn" : "sum",
    "mean_rtn": "mean"}

# exog candidates: functions of the event table, prior events only
FEATURES = {
    "lag_mean_rtn": lambda df: _prior_event(df, "mean_rtn")}


class EventFeatures:

    def __init__(self) -> None:

        self.src_path     = os.getcwd()
        self.repo_path    = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path    = os.path.join(self.repo_path, "data")
        self.vol_path     = os.path.join(self.data_path, "VolatilityNormalizedReturns")
        self.feature_path = os.path.join(self.data_path, "EventFeatures")

        if not os.path.exists(self.feature_path):
            os.makedirs(self.feature_path)
            
    # ------------------------------------------------------------------ data

    def _get_vol_rtn(self, scheme: str) -> pd.DataFrame:

        path   = os.path.join(self.vol_path, SCHEME_FILES[scheme])
        df_out = (pd
            .read_parquet(path = path, engine = "pyarrow")
            .assign(meeting_id = lambda x: x.meeting_id.astype(int)))

        return df_out

    def _get_event_base(self, scheme: str) -> pd.DataFrame:

        # one row per event with every target. split is constant per event
        target_aggs = {name: ("vol_rtn", agg) for name, agg in TARGETS.items()}

        df_out = (self
            ._get_vol_rtn(scheme)
            [EVENT_COLS + ["split", "date", "vol_rtn"]]
            .dropna(subset = ["vol_rtn"])
            .groupby(EVENT_COLS + ["split"])
            .agg(
                event_start = ("date", "min"),
                n_bars      = ("vol_rtn", "count"),
                **target_aggs)
            .reset_index()
            .sort_values(EVENT_COLS)
            .reset_index(drop = True))

        return df_out

    def _add_features(self, df_event: pd.DataFrame) -> pd.DataFrame:

        df_out = (df_event
            .sort_values(EVENT_COLS)
            .reset_index(drop = True))

        df_out["prior_meeting_id"] = _prior_event(df_out, "meeting_id")

        for name, func in FEATURES.items():
            df_out[name] = func(df_out)

        return df_out

    # -------------------------------------------------------------- checks

    def _check_features(self, df_out: pd.DataFrame, scheme: str, verbose: bool = True) -> None:

        if not verbose: return None

        # one row per event
        n_dup = df_out.duplicated(subset = EVENT_COLS).sum()

        if n_dup > 0: print("There are {} duplicated events".format(n_dup))
        else: print("Passed Duplicate Event Check")

        # lagged features must come from an earlier meeting
        df_bad = (df_out
            .loc[lambda x: x.prior_meeting_id.notna()]
            .loc[lambda x: ~(x.prior_meeting_id < x.meeting_id)])

        if len(df_bad) > 0:
            print("Look-ahead: {} events lag from a meeting on or after their own".format(len(df_bad)))
            print(df_bad[EVENT_COLS + ["prior_meeting_id"]])
        else: print("Passed Look-Ahead Check")

        # prior event more than one meeting back (gap in the data)
        df_gap = (df_out
            .loc[lambda x: x.prior_meeting_id.notna()]
            .assign(gap = lambda x: x.meeting_id - x.prior_meeting_id)
            .loc[lambda x: x.gap > 1])

        if len(df_gap) > 0:
            print("There are {} events whose prior event is more than one meeting back".format(len(df_gap)))
            print(df_gap
                .groupby(["ticker", "zone", "vol_type"])
                .gap
                .agg(["count", "max"]))
        else: print("Passed Meeting Gap Check")

        print("Missing values per feature ({})".format(scheme))
        print(df_out
            [list(FEATURES.keys())]
            .isna()
            .sum()
            .to_frame(name = "missing"))

        print("Events per split")
        print(df_out
            .groupby("split")
            .size()
            .to_frame(name = "events"))

    # --------------------------------------------------------------- build

    def build(self, overwrite: bool = False, verbose: bool = True) -> None:

        for scheme, file in SCHEME_FILES.items():

            out_path = os.path.join(self.feature_path, file)
            if os.path.exists(out_path) and not overwrite:
                if verbose: print("Already have {}\n".format(file))
                continue
            
            if verbose: print("Building {} event features".format(scheme))

            # split must be constant within an event
            df_event = self._get_event_base(scheme)
            n_split  = (df_event
                .groupby(EVENT_COLS)
                .split
                .nunique()
                .loc[lambda x: x > 1])

            if len(n_split) > 0:
                if verbose: print("There are {} events with more than one split".format(len(n_split)))
            else:
                if verbose: print("Passed Split Check")

            df_out = self._add_features(df_event)
            self._check_features(df_out, scheme, verbose = verbose)

            if verbose: print("Saving data\n")
            df_out.to_parquet(path = out_path, engine = "pyarrow")

    def load(self, scheme: str) -> pd.DataFrame:

        path   = os.path.join(self.feature_path, SCHEME_FILES[scheme])
        df_out = pd.read_parquet(path = path, engine = "pyarrow")

        return df_out


def main() -> None:

    event_features = EventFeatures()
    event_features.build()

if __name__ == "__main__": main()
