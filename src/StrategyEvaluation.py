# -*- coding: utf-8 -*-
"""
Created on Mon Nov 2 2026

Measures the results of any signals or models, independent of how many there
are or how they were built.

    backtest          trade one or more sources on the vol normalized returns
    sharpe_table      annualized Sharpe at the minute or event level
    cumulative_pnl    date x strategy PnL, for plotting
    prediction_loss   MSE (ols) / log-loss (logit) and hit rate by split
    params_table      coefficients, t-values or p-values per ticker x zone

A source is any of
    - an event-level DataFrame with EVENT_COLS + signal (e.g. the df_pred
      returned by EventModels.fit_quick, optionally with model / scheme)
    - a saved model spec name in data/Models/ (EventModels.run)
    - a saved signal name in data/Signals/ (SignalGenerator)

Positions are sign(signal) and are applied to the vol normalized returns of
the matching scheme through SignalGenerator.apply_event_signal, so a signal on
meeting t trades meeting t (signals must already be lagged).

Nothing here plots; the notebook keeps control of charts.
"""

import os
import numpy as np
import pandas as pd

from EventFeatures import EVENT_COLS
from EventModels import EventModels
from SignalGenerator import SignalGenerator

MINUTE_ANNUALIZER = np.sqrt(390 * 8)  # matches the existing notebook tables
EVENT_ANNUALIZER  = np.sqrt(8)        # 8 FOMC meetings a year


class StrategyEvaluation:

    def __init__(self) -> None:

        self.signal_generator = SignalGenerator()
        self.event_models     = EventModels()

    # -------------------------------------------------------------- source

    def _resolve(self, source) -> pd.DataFrame:

        # a DataFrame is used as is, a string is looked up as a saved model
        # spec first and then as a saved signal
        if isinstance(source, pd.DataFrame):
            return source

        if isinstance(source, str):

            model_path  = os.path.join(self.event_models.model_path, source, "Predictions.parquet")
            signal_path = os.path.join(self.signal_generator.signal_path, "{}.parquet".format(source))

            if os.path.exists(model_path):
                return (self.event_models
                    .load_predictions(source)
                    .assign(model = source.split("__")[0]))

            if os.path.exists(signal_path):
                return (self.signal_generator
                    .load_signal(source)
                    .assign(model = "rule"))

            raise ValueError("{} is not a saved model spec or signal".format(source))

        raise ValueError("a source must be a DataFrame or a saved spec / signal name")

    # ------------------------------------------------------------ backtest

    def backtest(self, inputs: dict, scheme: str, vol_types: list = None) -> pd.DataFrame:

        # inputs: {strategy_name: source}. Each source is split by its model
        # column (if any) and traded on the vol normalized returns of scheme.
        # Returns the minutely rows stacked with strategy and model columns

        keep_cols = [
            "date", "split", "contract"] + EVENT_COLS + [
            "signal", "position", "vol_rtn", "signal_rtn"]

        df_list = []
        for strategy, source in inputs.items():

            df_source = self._resolve(source)

            missing = [col for col in EVENT_COLS + ["signal"] if col not in df_source.columns]
            if len(missing) > 0:
                raise ValueError("{} is missing {}".format(strategy, missing))

            if "model" not in df_source.columns:
                df_source = df_source.assign(model = "signal")

            for model, df_model in df_source.groupby("model"):

                df_add = self.signal_generator.apply_event_signal(
                    df_signal = df_model,
                    scheme    = scheme,
                    vol_types = vol_types)

                if len(df_add) == 0: continue

                df_add = (df_add
                    [[col for col in keep_cols if col in df_add.columns]]
                    .assign(
                        strategy = strategy,
                        model    = model,
                        scheme   = scheme))

                df_list.append(df_add)

        if len(df_list) == 0:
            raise ValueError("no source produced any trades for scheme {}".format(scheme))

        return pd.concat(df_list, ignore_index = True)

    # ------------------------------------------------------------- results

    def _sharpe(self, x: pd.Series, annualizer: float) -> float:

        return x.mean() / x.std() * annualizer

    def sharpe_table(
            self,
            df_bt    : pd.DataFrame,
            by       : list = ["strategy", "model"],
            level    : str  = "minute",
            portfolio: bool = False) -> pd.DataFrame:

        # level
        #   "minute": every minute signal_rtn, annualized with sqrt(390 * 8)
        #   "event" : summed to one PnL per event, annualized with sqrt(8)
        # portfolio
        #   False: every ticker / zone row counts as its own observation
        #   True : summed across everything not in by at each date (minute)
        #          or meeting (event) first, i.e. the Sharpe of the combined book

        if level not in ["minute", "event"]:
            raise ValueError("level must be minute or event")

        if level == "minute":

            time_col   = "date"
            annualizer = MINUTE_ANNUALIZER
            df_rtn     = df_bt

        else:

            time_col   = "meeting_id"
            annualizer = EVENT_ANNUALIZER
            event_cols = list(dict.fromkeys(by + EVENT_COLS))
            df_rtn     = (df_bt
                .groupby(event_cols)
                .signal_rtn
                .sum()
                .reset_index())

        if portfolio:
            df_rtn = (df_rtn
                .groupby(list(dict.fromkeys(by + [time_col])))
                .signal_rtn
                .sum()
                .reset_index())

        df_out = (df_rtn
            .dropna(subset = ["signal_rtn"])
            .groupby(by)
            .signal_rtn
            .agg(
                sharpe = lambda x: self._sharpe(x, annualizer),
                n_obs  = "count")
            .reset_index())

        return df_out

    def cumulative_pnl(
            self,
            df_bt     : pd.DataFrame,
            by        : list = ["strategy", "model"],
            cumulative: bool = True) -> pd.DataFrame:

        # date x (by) frame of PnL summed across everything not in by,
        # cumulated unless cumulative = False

        df_out = (df_bt
            .groupby(by + ["date"])
            .signal_rtn
            .sum()
            .unstack(by)
            .sort_index()
            .fillna(0))

        if cumulative: df_out = df_out.cumsum()

        return df_out

    def prediction_loss(self, source, by: list = ["scheme", "model", "split"]) -> pd.DataFrame:

        # source: fit_quick predictions or a saved spec name. Needs target,
        # prediction and model. ols: MSE and sign hit rate; logit: log-loss
        # and hit rate of p > 0.5 against target > 0

        df_pred = self._resolve(source)

        missing = [col for col in ["target", "prediction", "model"] + by if col not in df_pred.columns]
        if len(missing) > 0:
            raise ValueError("source is missing {}".format(missing))

        # newer pandas drops the grouping columns inside apply, so the model
        # is read from a copy
        def _loss(df: pd.DataFrame) -> pd.Series:

            model = df.model_.iloc[0]
            y     = df.target
            pred  = df.prediction

            if model == "logit":

                y_bin = (y > 0).astype(int)
                p     = pred.clip(1e-12, 1 - 1e-12)

                return pd.Series({
                    "n_events": len(df),
                    "mse"     : np.nan,
                    "log_loss": -(y_bin * np.log(p) + (1 - y_bin) * np.log(1 - p)).mean(),
                    "hit_rate": ((p > 0.5).astype(int) == y_bin).mean()})

            return pd.Series({
                "n_events": len(df),
                "mse"     : ((y - pred) ** 2).mean(),
                "log_loss": np.nan,
                "hit_rate": (np.sign(pred) == np.sign(y)).mean()})

        df_out = (df_pred
            .dropna(subset = ["target", "prediction"])
            .assign(model_ = lambda x: x.model)
            .groupby(by)
            .apply(_loss)
            .reset_index())

        return df_out

    def params_table(
            self,
            df_params: pd.DataFrame,
            param    : str  = None,
            value    : str  = "tvalue",
            index    : list = ["scheme", "model", "ticker"],
            columns  : str  = "name") -> pd.DataFrame:

        # df_params: from fit_quick or EventModels.load_params. For
        # out_sample (one fit per meeting) only the latest fit per group is
        # shown. value: coeff, bse, tvalue, pvalue, r2 ...

        df_tmp = df_params.copy()

        if param is not None:
            df_tmp = df_tmp.loc[lambda x: x.param == param]

        if "predict_meeting" in df_tmp.columns and df_tmp.predict_meeting.notna().any():

            # keep static fits, and the last expanding fit per group / param
            group_cols = [col for col in ["scheme", "model", "ticker", "zone", "name", "vol_type", "param"] if col in df_tmp.columns]
            df_static  = df_tmp.loc[lambda x: x.predict_meeting.isna()]
            df_last    = (df_tmp
                .loc[lambda x: x.predict_meeting.notna()]
                .sort_values("predict_meeting")
                .groupby(group_cols)
                .tail(1))

            df_tmp = pd.concat([df_static, df_last])

        index = [col for col in index if col in df_tmp.columns]
        if param is None: index = index + ["param"]

        df_out = df_tmp.pivot_table(index = index, columns = columns, values = value, aggfunc = "first")

        return df_out
