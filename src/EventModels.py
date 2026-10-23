# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 2026

Fits event-level models on the tables built by EventFeatures.py under each
scheme and saves everything needed to probe them later.

A model spec is a dict
    {"model": "ols" | "logit", "target": <TARGETS name>, "features": [<FEATURES names>]}
named <model>__<target>__<feature-feature-...>, e.g. ols__cum_rtn__lag_mean_rtn.

Models are fit per ticker x zone x name x vol_type (GROUP_COLS)

    scheme       fit on                          predicts meeting t with
    in_sample    all events                      the full sample model
    train_test   train events                    the frozen train model (train + test rows)
    out_sample   events before t (refit each t)  the model fit through t-1, after min_obs events

Features are z-scored with the statistics of the fit sample only (full,
train, or expanding), so coefficients are per one standard deviation of the
feature and x_mean / x_std are saved with the params. Logit fits on
target > 0 and predicts the probability of a positive event.

Output: data/Models/<spec_name>/
    Predictions.parquet  one row per scheme x event: split, target (actual),
                         prediction, signal (ols: prediction, logit: p - 0.5),
                         n_obs, first / last_fit_meeting, converged
    Params.parquet       one row per fit x param: coeff, bse, tvalue, pvalue,
                         r2 (ols) / pseudo r2 (logit), n_obs, converged, fit
                         window, x_mean, x_std. out_sample has one fit per
                         predicted meeting (predict_meeting)
    Models.pkl           fitted statsmodels results for in_sample and
                         train_test keyed "<scheme> <ticker> <name> <zone> <vol_type>"
    Spec.json            the spec and settings that produced the folder
"""

import os
import json
import pickle
import warnings
import numpy as np
import pandas as pd
import statsmodels.api as sm

from tqdm import tqdm
from EventFeatures import EventFeatures, SCHEME_FILES, GROUP_COLS, EVENT_COLS

MODELS = ["ols", "logit"]

# default specs, add new ones here
SPECS = [
    {"model": "ols"  , "target": "cum_rtn", "features": ["lag_mean_rtn"]},
    {"model": "logit", "target": "cum_rtn", "features": ["lag_mean_rtn"]},
    {"model": "ols"  , "target": "cum_rtn", "features": ["lag_mean_rtn_nz"]},
    {"model": "logit", "target": "cum_rtn", "features": ["lag_mean_rtn_nz"]}]


class EventModels:

    def __init__(self) -> None:

        self.src_path   = os.getcwd()
        self.repo_path  = os.path.abspath(os.path.join(self.src_path, ".."))
        self.data_path  = os.path.join(self.repo_path, "data")
        self.model_path = os.path.join(self.data_path, "Models")

        if not os.path.exists(self.model_path):
            os.makedirs(self.model_path)

        self.event_features = EventFeatures()

    # ---------------------------------------------------------------- spec

    def spec_name(self, spec: dict) -> str:

        return "{}__{}__{}".format(spec["model"], spec["target"], "-".join(spec["features"]))

    def _check_spec(self, spec: dict, df: pd.DataFrame) -> None:

        if spec["model"] not in MODELS:
            raise ValueError("model must be one of {}".format(MODELS))

        missing = [col for col in [spec["target"]] + spec["features"] if col not in df.columns]
        if len(missing) > 0:
            raise ValueError("{} not in EventFeatures, add them to TARGETS / FEATURES and rebuild".format(missing))

    # ----------------------------------------------------------------- fit

    def _endog(self, target: pd.Series, model: str) -> pd.Series:

        if model == "logit": return (target > 0).astype(int)
        return target

    def _design(self, df: pd.DataFrame, features: list, x_mean: pd.Series, x_std: pd.Series) -> pd.DataFrame:

        X = (df[features] - x_mean) / x_std
        return sm.add_constant(X, has_constant = "add")

    def _fit(self, model: str, y: pd.Series, X: pd.DataFrame) -> tuple:

        # small or separable samples raise warnings (and sometimes errors);
        # failures return None and non-convergence is flagged, not dropped
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            try:
                if model == "ols":
                    res       = sm.OLS(endog = y, exog = X).fit()
                    converged = True

                elif model == "logit":
                    res       = sm.Logit(endog = y, exog = X).fit(disp = 0, maxiter = 100)
                    converged = bool(res.mle_retvals.get("converged", False))

            except Exception:
                return None, False

        return res, converged

    def _fit_predict(self, df_fit: pd.DataFrame, df_pred: pd.DataFrame, spec: dict) -> dict:

        # fits on df_fit and predicts df_pred. Returns None (with a reason)
        # when the fit sample can't support the model
        model, target, features = spec["model"], spec["target"], spec["features"]

        y = self._endog(df_fit[target], model)

        if len(df_fit) <= len(features) + 1:
            return {"reason": "too few events"}

        if model == "logit" and y.nunique() < 2:
            return {"reason": "one class only"}

        x_mean = df_fit[features].mean()
        x_std  = df_fit[features].std()

        if x_std.isna().any() or (x_std == 0).any():
            return {"reason": "constant feature"}

        res, converged = self._fit(model, y, self._design(df_fit, features, x_mean, x_std))

        if res is None:
            return {"reason": "fit failed"}

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            prediction = np.asarray(res.predict(self._design(df_pred, features, x_mean, x_std)))

        return {
            "res"       : res,
            "converged" : converged,
            "x_mean"    : x_mean,
            "x_std"     : x_std,
            "prediction": prediction}

    # -------------------------------------------------------------- output

    def _prediction_frame(self, df_fit: pd.DataFrame, df_pred: pd.DataFrame, out: dict, spec: dict) -> pd.DataFrame:

        signal = out["prediction"] - 0.5 if spec["model"] == "logit" else out["prediction"]

        df_out = (df_pred
            [EVENT_COLS + ["split", spec["target"]]]
            .rename(columns = {spec["target"]: "target"})
            .assign(
                prediction        = out["prediction"],
                signal            = signal,
                n_obs             = len(df_fit),
                first_fit_meeting = int(df_fit.meeting_id.min()),
                last_fit_meeting  = int(df_fit.meeting_id.max()),
                converged         = out["converged"]))

        return df_out

    def _params_frame(self, df_fit: pd.DataFrame, out: dict, spec: dict, keys: dict, predict_meeting: float = np.nan) -> pd.DataFrame:

        res = out["res"]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            df_out = (pd.DataFrame({
                    "coeff" : res.params,
                    "bse"   : res.bse,
                    "tvalue": res.tvalues,
                    "pvalue": res.pvalues})
                .rename_axis("param")
                .reset_index()
                .assign(
                    r2                = res.rsquared if spec["model"] == "ols" else res.prsquared,
                    n_obs             = int(res.nobs),
                    converged         = out["converged"],
                    first_fit_meeting = int(df_fit.meeting_id.min()),
                    last_fit_meeting  = int(df_fit.meeting_id.max()),
                    predict_meeting   = predict_meeting,
                    x_mean            = lambda x: x.param.map(out["x_mean"]),
                    x_std             = lambda x: x.param.map(out["x_std"]),
                    **keys))

        return df_out

    # ------------------------------------------------------------- schemes

    def _fit_scheme(self, df: pd.DataFrame, scheme: str, spec: dict, min_obs: int, verbose: bool) -> tuple:

        pred_list, param_list, skip_list, models = [], [], [], {}

        cols   = EVENT_COLS + ["split", spec["target"]] + spec["features"]
        groups = (df
            [cols]
            .dropna()
            .sort_values(EVENT_COLS)
            .groupby(GROUP_COLS))

        iter_ = tqdm(groups, desc = scheme) if verbose else groups

        for group_keys, df_group in iter_:

            keys      = dict(zip(GROUP_COLS, group_keys))
            df_group  = df_group.reset_index(drop = True)
            model_key = " ".join([scheme, keys["ticker"], keys["name"], keys["zone"], keys["vol_type"]])

            if scheme in ["in_sample", "train_test"]:

                df_fit = df_group if scheme == "in_sample" else df_group.loc[lambda x: x.split == "train"]
                out    = self._fit_predict(df_fit, df_group, spec)

                if "reason" in out:
                    skip_list.append({"scheme": scheme, **keys, "reason": out["reason"]})
                    continue

                pred_list.append(self._prediction_frame(df_fit, df_group, out, spec))
                param_list.append(self._params_frame(df_fit, out, spec, keys))
                models[model_key] = out["res"]

            elif scheme == "out_sample":

                # meeting i is predicted by a model fit on the i events before it
                for i in range(min_obs, len(df_group)):

                    df_fit  = df_group.iloc[:i]
                    df_pred = df_group.iloc[[i]]
                    out     = self._fit_predict(df_fit, df_pred, spec)

                    if "reason" in out:
                        skip_list.append({
                            "scheme"         : scheme,
                            **keys,
                            "predict_meeting": int(df_pred.meeting_id.iloc[0]),
                            "reason"         : out["reason"]})
                        continue

                    pred_list.append(self._prediction_frame(df_fit, df_pred, out, spec))
                    param_list.append(self._params_frame(
                        df_fit, out, spec, keys, predict_meeting = int(df_pred.meeting_id.iloc[0])))

        return pred_list, param_list, skip_list, models

    # -------------------------------------------------------------- checks

    def _check_models(self, df_pred: pd.DataFrame, df_params: pd.DataFrame, df_skip: pd.DataFrame, verbose: bool = True) -> None:

        if not verbose: return None

        # one prediction per event per scheme
        n_dup = df_pred.duplicated(subset = ["scheme"] + EVENT_COLS).sum()

        if n_dup > 0: print("There are {} duplicated predictions".format(n_dup))
        else: print("Passed Duplicate Prediction Check")

        # look-ahead: out_sample and test predictions only use earlier meetings
        df_bad = (df_pred
            .loc[lambda x: x.split.isin(["oos", "test"])]
            .loc[lambda x: ~(x.last_fit_meeting < x.meeting_id)])

        if len(df_bad) > 0:
            print("Look-ahead: {} predictions use a model fit on or after the meeting".format(len(df_bad)))
            print(df_bad[["scheme"] + EVENT_COLS + ["last_fit_meeting"]])
        else: print("Passed Look-Ahead Check")

        # non-converged fits are kept but flagged
        df_conv = (df_pred
            .loc[lambda x: ~x.converged]
            .groupby(["scheme", "ticker", "zone", "vol_type"])
            .size())

        if len(df_conv) > 0:
            print("There are {} predictions from fits that didn't converge".format(df_conv.sum()))
            print(df_conv)
        else: print("Passed Convergence Check")

        if len(df_skip) > 0:
            print("There are {} fits skipped".format(len(df_skip)))
            print(df_skip
                .groupby(["scheme", "reason"])
                .size()
                .to_frame(name = "fits"))
        else: print("Passed Skipped Fit Check")

        print("Predictions per scheme and split")
        print(df_pred
            .groupby(["scheme", "split"])
            .size()
            .to_frame(name = "rows"))

    # ----------------------------------------------------------------- run

    def run(self, spec: dict, min_obs: int = 30, overwrite: bool = False, verbose: bool = True) -> None:

        # min_obs is the out_sample burn-in, defaulted to the min_obs used by
        # the expanding vol models in VolatilityEstimator.py

        spec_name = self.spec_name(spec)
        out_dir   = os.path.join(self.model_path, spec_name)
        pred_path = os.path.join(out_dir, "Predictions.parquet")

        if os.path.exists(pred_path) and not overwrite:
            if verbose: print("Already have {}\n".format(spec_name))
            return None

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        if verbose: print("Fitting {}".format(spec_name))

        pred_list, param_list, skip_list, models = [], [], [], {}

        for scheme in SCHEME_FILES.keys():

            df = self.event_features.load(scheme)
            self._check_spec(spec, df)

            preds, params, skips, scheme_models = self._fit_scheme(df, scheme, spec, min_obs, verbose)

            pred_list  += [df_add.assign(scheme = scheme) for df_add in preds]
            param_list += [df_add.assign(scheme = scheme) for df_add in params]
            skip_list  += skips
            models.update(scheme_models)

        df_pred   = pd.concat(pred_list , ignore_index = True).assign(spec_name = spec_name)
        df_params = pd.concat(param_list, ignore_index = True).assign(spec_name = spec_name)
        df_skip   = pd.DataFrame(skip_list)

        self._check_models(df_pred, df_params, df_skip, verbose = verbose)

        if verbose: print("Saving data\n")

        df_pred.to_parquet(path = pred_path, engine = "pyarrow")
        df_params.to_parquet(path = os.path.join(out_dir, "Params.parquet"), engine = "pyarrow")

        with open(os.path.join(out_dir, "Models.pkl"), "wb") as f: pickle.dump(models, f)

        with open(os.path.join(out_dir, "Spec.json"), "w") as f:
            json.dump({**spec, "spec_name": spec_name, "min_obs": min_obs, "group_cols": GROUP_COLS}, f, indent = 4)

    # --------------------------------------------------------------- probe

    def list_specs(self) -> list:

        return sorted([
            folder for folder in os.listdir(self.model_path)
            if os.path.exists(os.path.join(self.model_path, folder, "Predictions.parquet"))])

    def load_predictions(self, spec_name: str) -> pd.DataFrame:

        path = os.path.join(self.model_path, spec_name, "Predictions.parquet")
        return pd.read_parquet(path = path, engine = "pyarrow")

    def load_params(self, spec_name: str) -> pd.DataFrame:

        path = os.path.join(self.model_path, spec_name, "Params.parquet")
        return pd.read_parquet(path = path, engine = "pyarrow")

    def load_models(self, spec_name: str) -> dict:

        path = os.path.join(self.model_path, spec_name, "Models.pkl")
        with open(path, "rb") as f: models = pickle.load(f)

        return models


def main() -> None:

    event_models = EventModels()
    for spec in SPECS: event_models.run(spec)

if __name__ == "__main__": main()
