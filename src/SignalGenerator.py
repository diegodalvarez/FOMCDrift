# -*- coding: utf-8 -*-
"""
Created on Sat Oct 17 2026

Generates event-level trading signals that are applied to the volatility
normalized returns built in VolatilityNormalizedReturns.py. Event returns are
read from data/EventFeatures/ (run EventFeatures.py first). apply_model
trades the predictions saved by EventModels.py through the same path.

A signal is one number per ticker x zone x name x vol_type x meeting_id. It
is known before that meeting's zone opens and is held constant through the
zone, so signals are stored at the event level and applied to the minutely
vol_rtn with apply_signal.

Each scheme is built from the event table, and applied to the vol normalized
returns, of the same sample (same file names in data/EventFeatures/ and
data/VolatilityNormalizedReturns/)

    scheme       reads               signal for meeting t
    in_sample    InSample.parquet    mean over all meetings (look-ahead by design)
    train_test   TrainTest.parquet   mean over train meetings only, held fixed,
                                     split taken from the file (TrainTestSplit.parquet)
    out_sample   OutSample.parquet   expanding mean over meetings < t

Output: data/Signals/<SignalName>.parquet, one row per
    signal_name, scheme, split, ticker, zone, name, vol_type, meeting_id,
    signal, n_events, last_meeting_id

last_meeting_id is the latest meeting whose returns went into the signal,
used for the look-ahead checks.
"""

import os
import numpy as np
import pandas as pd

# each scheme reads the vol normalized returns of the same sample
SCHEME_FILES = {
    "in_sample" : "InSample.parquet",
    "train_test": "TrainTest.parquet",
    "out_sample": "OutSample.parquet"}

GROUP_COLS  = ["ticker", "zone", "name", "vol_type"]
EVENT_COLS  = GROUP_COLS + ["meeting_id"]
SIGNAL_COLS = [
    "signal_name", "scheme", "split", "ticker", "zone", "name", "vol_type",
    "meeting_id", "signal", "n_events", "last_meeting_id"]


class SignalGenerator:

    def __init__(self) -> None:

        self.src_path    = os.getcwd()
        self.repo_path   = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path   = os.path.join(self.repo_path, "data")
        self.vol_path     = os.path.join(self.data_path, "VolatilityNormalizedReturns")
        self.feature_path = os.path.join(self.data_path, "EventFeatures")
        self.model_path   = os.path.join(self.data_path, "Models")
        self.signal_path  = os.path.join(self.data_path, "Signals")

        if not os.path.exists(self.signal_path):
            os.makedirs(self.signal_path)

    # ------------------------------------------------------------------ data

    def _get_vol_rtn(self, scheme: str) -> pd.DataFrame:

        path   = os.path.join(self.vol_path, SCHEME_FILES[scheme])
        df_out = (pd
            .read_parquet(path = path, engine = "pyarrow")
            .assign(meeting_id = lambda x: x.meeting_id.astype(int)))

        return df_out

    def _get_event_rtn(self, scheme: str) -> pd.DataFrame:

        # one row per event: mean vol_rtn within that meeting's zone, read
        # from the EventFeatures table (mean_rtn). Signals are built from
        # these so every event gets equal weight regardless of how many
        # minutes it has. split is carried through (full / train / test / oos)
        path   = os.path.join(self.feature_path, SCHEME_FILES[scheme])
        df_out = (pd
            .read_parquet(path = path, engine = "pyarrow")
            [EVENT_COLS + ["split", "mean_rtn"]]
            .rename(columns = {"mean_rtn": "event_rtn"})
            .dropna(subset = ["event_rtn"])
            .assign(meeting_id = lambda x: x.meeting_id.astype(int))
            .sort_values(EVENT_COLS)
            .reset_index(drop = True))

        return df_out

    # ------------------------------------------------------------- schemes

    def _in_sample(self, df_event: pd.DataFrame, estimator: str) -> pd.DataFrame:

        df_stat = (df_event
            .groupby(GROUP_COLS)
            .agg(
                signal          = ("event_rtn", estimator),
                n_events        = ("event_rtn", "count"),
                last_meeting_id = ("meeting_id", "max"))
            .reset_index())

        df_out = (df_event
            [EVENT_COLS + ["split"]]
            .merge(right = df_stat, how = "inner", on = GROUP_COLS))

        return df_out

    def _train_test(self, df_event: pd.DataFrame, estimator: str) -> pd.DataFrame:

        # statistic over train meetings only, applied to every meeting. The
        # split comes from the vol normalized returns so the signal freezes at
        # the same meeting as the vol model
        df_stat = (df_event
            .loc[lambda x: x.split == "train"]
            .groupby(GROUP_COLS)
            .agg(
                signal          = ("event_rtn", estimator),
                n_events        = ("event_rtn", "count"),
                last_meeting_id = ("meeting_id", "max"))
            .reset_index())

        # groups with no train meetings get no signal (inner merge)
        df_out = (df_event
            [EVENT_COLS + ["split"]]
            .merge(right = df_stat, how = "inner", on = GROUP_COLS))

        return df_out

    def _out_sample(self, df_event: pd.DataFrame, min_events: int, estimator: str) -> pd.DataFrame:

        # expanding statistic over prior events only: shift(1) so meeting t
        # uses meetings strictly before t. Grouped by vol_type too so a row
        # never picks up the same meeting from a different vol estimator
        df_event = df_event.sort_values(EVENT_COLS).reset_index(drop = True)
        grouped  = df_event.groupby(GROUP_COLS)

        df_out = (df_event
            .assign(
                signal = grouped.event_rtn.transform(
                    lambda s: getattr(s.expanding(min_periods = min_events), estimator)().shift()),
                n_events        = grouped.event_rtn.transform(lambda s: s.expanding().count().shift()),
                last_meeting_id = grouped.meeting_id.shift())
            .dropna(subset = ["signal"])
            .drop(columns = ["event_rtn"])
            .assign(
                n_events        = lambda x: x.n_events.astype(int),
                last_meeting_id = lambda x: x.last_meeting_id.astype(int)))

        return df_out

    # -------------------------------------------------------------- checks

    def _check_signals(self, df_signal: pd.DataFrame, verbose: bool = True) -> None:

        if not verbose: return None

        # one signal per event per scheme
        key_cols = ["scheme"] + EVENT_COLS
        n_dup    = df_signal.duplicated(subset = key_cols).sum()

        if n_dup > 0: print("There are {} duplicated signal rows".format(n_dup))
        else: print("Passed Duplicate Signal Check")

        # look-ahead: out_sample and test rows must only use earlier meetings
        df_bad = (df_signal
            .loc[lambda x: x.split.isin(["oos", "test"])]
            .loc[lambda x: ~(x.last_meeting_id < x.meeting_id)])

        if len(df_bad) > 0:
            print("Look-ahead: {} rows use a meeting on or after the traded meeting".format(len(df_bad)))
            print(df_bad)
        else: print("Passed Look-Ahead Check")

        # each scheme should only carry its own splits
        exp_splits = {
            "in_sample" : ["full"],
            "train_test": ["train", "test"],
            "out_sample": ["oos"]}

        df_bad = (df_signal
            [["scheme", "split"]]
            .drop_duplicates()
            .loc[lambda x: [split not in exp_splits.get(scheme, []) for scheme, split in zip(x.scheme, x.split)]])

        if len(df_bad) > 0:
            print("Unexpected scheme / split pairs")
            print(df_bad)
        else: print("Passed Scheme Split Check")

        print("Signal rows per scheme and split")
        print(df_signal
            .groupby(["scheme", "split"])
            .size()
            .to_frame(name = "rows"))

    # ------------------------------------------------------------- signals

    def _build_signal(
            self,
            signal_name: str,
            estimator  : str,
            min_events : int,
            verbose    : bool) -> pd.DataFrame:

        df_lists = []
        for scheme in SCHEME_FILES.keys():

            if verbose: print("Building {} {} signals from {}".format(signal_name, scheme, SCHEME_FILES[scheme]))
            df_event = self._get_event_rtn(scheme)

            if scheme == "in_sample":
                df_add = self._in_sample(df_event, estimator)

            elif scheme == "train_test":
                df_add = self._train_test(df_event, estimator)

            elif scheme == "out_sample":
                df_add = self._out_sample(df_event, min_events, estimator)

            df_lists.append(df_add.assign(scheme = scheme))

        df_out = (pd
            .concat(df_lists)
            .assign(signal_name = signal_name)
            [SIGNAL_COLS]
            .reset_index(drop = True))

        return df_out

    def historical_event_mean(
            self,
            min_events: int  = 30,
            overwrite : bool = False,
            verbose   : bool = True) -> None:

        # Strategy 1: trade the sign of the historical mean event return per
        # ticker / zone. min_events is the out_sample burn-in, defaulted to the
        # min_obs used by the expanding vol models in VolatilityEstimator.py.
        # The train / test split is set in VolatilityEstimator.train_test_split

        out_path = os.path.join(self.signal_path, "HistoricalEventMean.parquet")
        if os.path.exists(out_path) and not overwrite:
            if verbose: print("Already have data\n")
            return None

        df_signal = self._build_signal(
            signal_name = "HistoricalEventMean",
            estimator   = "mean",
            min_events  = min_events,
            verbose     = verbose)

        self._check_signals(df_signal, verbose = verbose)

        if verbose: print("Saving data\n")
        df_signal.to_parquet(path = out_path, engine = "pyarrow")

    # --------------------------------------------------------------- apply

    def load_signal(self, signal_name: str) -> pd.DataFrame:

        path   = os.path.join(self.signal_path, "{}.parquet".format(signal_name))
        df_out = pd.read_parquet(path = path, engine = "pyarrow")

        return df_out

    def apply_signal(
            self,
            signal_name: str,
            scheme     : str,
            vol_types  : list = None,
            df_signal  : pd.DataFrame = None) -> pd.DataFrame:

        # rule-based signal from data/Signals/<signal_name>.parquet

        if df_signal is None: df_signal = self.load_signal(signal_name)
        return self.apply_event_signal(df_signal, scheme, vol_types)

    def apply_model(
            self,
            spec_name: str,
            scheme   : str,
            vol_types: list = None) -> pd.DataFrame:

        # model predictions from data/Models/<spec_name>/Predictions.parquet,
        # traded on the sign of signal (ols: prediction, logit: p - 0.5)

        path      = os.path.join(self.model_path, spec_name, "Predictions.parquet")
        df_signal = pd.read_parquet(path = path, engine = "pyarrow")

        return self.apply_event_signal(df_signal, scheme, vol_types)

    def apply_event_signal(
            self,
            df_signal: pd.DataFrame,
            scheme   : str,
            vol_types: list = None) -> pd.DataFrame:

        # Merges any event-level table with EVENT_COLS + signal (and scheme)
        # onto the vol normalized returns of the same sample and returns
        # minutely signal_rtn = sign(signal) * vol_rtn. split is kept so
        # train_test can be cut into train / test

        if scheme not in SCHEME_FILES:
            raise ValueError("scheme must be one of {}".format(list(SCHEME_FILES.keys())))

        id_cols = [col for col in ["signal_name", "spec_name", "scheme"] if col in df_signal.columns]

        if "scheme" in df_signal.columns:
            df_signal = df_signal.loc[lambda x: x.scheme == scheme]

        df_signal = (df_signal
            [id_cols + EVENT_COLS + ["signal"]]
            .assign(meeting_id = lambda x: x.meeting_id.astype(int)))

        df_vol_rtn = self._get_vol_rtn(scheme)

        if vol_types is not None:
            df_signal  = df_signal.loc[lambda x: x.vol_type.isin(vol_types)]
            df_vol_rtn = df_vol_rtn.loc[lambda x: x.vol_type.isin(vol_types)]

        df_out = (df_vol_rtn
            .merge(right = df_signal, how = "inner", on = EVENT_COLS)
            .assign(
                position   = lambda x: np.sign(x.signal),
                signal_rtn = lambda x: x.position * x.vol_rtn))

        return df_out


def main() -> None:

    signal_generator = SignalGenerator()
    signal_generator.historical_event_mean()

if __name__ == "__main__": main()
