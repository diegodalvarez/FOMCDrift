# -*- coding: utf-8 -*-
"""
Created on Thu Nov 5 2026

One place that defines every model the pipeline can fit or trade. EventModels
and StrategyEvaluation look models up here by name and never branch on a
model's name, so adding a model means adding one class and registering it.

Each model says
    needs_fit   False for rules that only read the features
    endog       how the target is transformed before fitting
    check       a reason the fit sample can't support the model (or None)
    fit         fit on (y, X) and return (result, converged)
    predict     predictions for new rows
    to_signal   prediction -> tradeable signal (position = sign(signal))
    params      coefficient table of a fitted result
    fit_stat    one goodness-of-fit number (r2, pseudo r2, ...)
    loss        how predictions are scored against the actual target

X is the design matrix of z-scored features with a constant; X_raw holds the
unscaled feature columns (what a rule reads).

Built in
    ols     OLS of the target, signal = prediction
    logit   Logit of target > 0, prediction = probability, signal = p - 0.5
    rule    no fit, signal = majority sign of the features (flat on a tie)

To add a model
    class MyModel(BaseModel):
        name        = "my_model"
        description = "..."
        def fit(self, y, X): ...
    register(MyModel())
"""

import numpy as np
import pandas as pd
import statsmodels.api as sm


class BaseModel:

    name        = None
    description = ""
    needs_fit   = True

    def endog(self, target: pd.Series) -> pd.Series:

        return target

    def check(self, y: pd.Series):

        # return a short reason string to skip the fit, or None
        return None

    def fit(self, y: pd.Series, X: pd.DataFrame) -> tuple:

        raise NotImplementedError

    def predict(self, res, X: pd.DataFrame, X_raw: pd.DataFrame) -> np.ndarray:

        return np.asarray(res.predict(X), dtype = float)

    def to_signal(self, prediction: np.ndarray) -> np.ndarray:

        return prediction

    def params(self, res) -> pd.DataFrame:

        # statsmodels-style results by default
        df_out = (pd.DataFrame({
                "coeff" : res.params,
                "bse"   : res.bse,
                "tvalue": res.tvalues,
                "pvalue": res.pvalues})
            .rename_axis("param")
            .reset_index())

        return df_out

    def fit_stat(self, res) -> float:

        return np.nan

    def loss(self, target: pd.Series, prediction: pd.Series) -> dict:

        # default: does the sign of the signal match the sign of the target
        signal = self.to_signal(np.asarray(prediction, dtype = float))
        return {"hit_rate": (np.sign(signal) == np.sign(np.asarray(target, dtype = float))).mean()}


class OLSModel(BaseModel):

    name        = "ols"
    description = "OLS of the target on the features, signal = fitted value"

    def fit(self, y: pd.Series, X: pd.DataFrame) -> tuple:

        return sm.OLS(endog = y, exog = X).fit(), True

    def fit_stat(self, res) -> float:

        return res.rsquared

    def loss(self, target: pd.Series, prediction: pd.Series) -> dict:

        y    = np.asarray(target, dtype = float)
        pred = np.asarray(prediction, dtype = float)

        return {
            "mse"     : np.mean((y - pred) ** 2),
            "hit_rate": (np.sign(pred) == np.sign(y)).mean()}


class LogitModel(BaseModel):

    name        = "logit"
    description = "Logit of target > 0 on the features, signal = probability - 0.5"

    def endog(self, target: pd.Series) -> pd.Series:

        return (target > 0).astype(int)

    def check(self, y: pd.Series):

        return "one class only" if y.nunique() < 2 else None

    def fit(self, y: pd.Series, X: pd.DataFrame) -> tuple:

        res = sm.Logit(endog = y, exog = X).fit(disp = 0, maxiter = 100)
        return res, bool(res.mle_retvals.get("converged", False))

    def to_signal(self, prediction: np.ndarray) -> np.ndarray:

        return prediction - 0.5

    def fit_stat(self, res) -> float:

        return res.prsquared

    def loss(self, target: pd.Series, prediction: pd.Series) -> dict:

        y_bin = (np.asarray(target, dtype = float) > 0).astype(int)
        p     = np.clip(np.asarray(prediction, dtype = float), 1e-12, 1 - 1e-12)

        return {
            "log_loss": -np.mean(y_bin * np.log(p) + (1 - y_bin) * np.log(1 - p)),
            "hit_rate": ((p > 0.5).astype(int) == y_bin).mean()}


class RuleModel(BaseModel):

    name        = "rule"
    description = "No fit, signal = majority sign of the features (flat on a tie)"
    needs_fit   = False

    def predict(self, res, X: pd.DataFrame, X_raw: pd.DataFrame) -> np.ndarray:

        return np.sign(np.sign(X_raw).sum(axis = 1)).to_numpy(dtype = float)

    def params(self, res) -> pd.DataFrame:

        return pd.DataFrame(columns = ["param", "coeff", "bse", "tvalue", "pvalue"])


# ---------------------------------------------------------------- registry

REGISTRY = {}

def register(model: BaseModel) -> None:

    if not isinstance(model, BaseModel) or not model.name:
        raise ValueError("a model must subclass BaseModel and set a name")

    REGISTRY[model.name] = model

def get_model(name: str) -> BaseModel:

    if name not in REGISTRY:
        raise ValueError("unknown model {}, registered: {}".format(name, model_names()))

    return REGISTRY[name]

def model_names() -> list:

    return list(REGISTRY.keys())

def list_models() -> pd.DataFrame:

    df_out = pd.DataFrame([{
            "model"      : model.name,
            "needs_fit"  : model.needs_fit,
            "description": model.description}
        for model in REGISTRY.values()])

    return df_out

for _model in [OLSModel(), LogitModel(), RuleModel()]: register(_model)
