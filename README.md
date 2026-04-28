# FOMCDrift
FOMC Drift

Replicating the results found ***The Pre-FOMC Drift and the Secular Decline in Long-Term Interest Rates*** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4764451)

This repository develops an event-driven trading framework for Treasury futures centered around FOMC announcement days. The approach is motivated by the findings of Peng and Pan in Pre-FOMC Announcement Drift, which documents predictable behavior in Treasury yields leading up to FOMC meetings.

While the original work focuses on yield-based risk premia, this project extends the intuition to Treasury futures and constructs a set of empirical backtests to evaluate whether the effect is tradable and persistent.


---

## Data

### Treasury Futures

Treasury futures are sourced from Bloomberg and include standard roll-adjusted contracts (GFUT methodology). Contracts span multiple maturities:

| Security | Description | Start Date | End Date |
|----------|------------|------------|----------|
| FV1 | 5Y Treasury Note | 2004-05-24 | 2024-09-20 |
| TU1 | 2Y Treasury Note | 2004-05-24 | 2024-09-20 |
| TY1 | 10Y Treasury Note | 2004-05-24 | 2024-09-20 |
| US1 | 20Y Treasury Bond | 2004-05-24 | 2024-09-20 |
| UXY1 | 10Y Treasury Note | 2016-01-13 | 2024-09-20 |
| WN1 | 30Y Treasury Bond | 2010-01-13 | 2024-09-20 |

Although optimal rolling would use open interest crossover, Bloomberg’s GFUT methodology is used as a standard approximation.

---

### Macro & Yield Data

- Treasury yields (1Y+) sourced from FRED
- FOMC / FDTR event dates sourced from Bloomberg LP

---

### NLP Sentiment Data

Bloomberg Economics sentiment indicators are used, including:

- Federal Reserve sentiment (hawk/dovish probabilities)
- Rates decomposition signals
- Equity and macro risk sentiment measures

These are internally computed NLP-based signals accessed via the Bloomberg Terminal.

---

## Methodology

### PCA Analysis of Yield Dynamics

A principal component analysis is applied to Treasury yield changes around FOMC events to test whether:

- Yield movements are dominated by a common factor
- That factor strengthens around announcement windows

**Key finding:**  
The first principal component explains significantly more variance around FOMC dates than in the full sample, suggesting a strong event-driven structure in rates.

---

### Sentiment Conditioning

A 10-day exponentially weighted z-score is computed on NLP sentiment. The sample is split into:

- Positive sentiment regime
- Negative sentiment regime

PCA is then recomputed under each regime to assess whether sentiment meaningfully alters rate dynamics.

---

### Hypothetical Yield Construction

Using observed yield changes around FOMC events, hypothetical yield paths are constructed and compared against realized yields.

**Result:**  
High correlation between constructed and realized paths suggests that FOMC-window dynamics are structurally stable and potentially exploitable.

---

## Backtesting Framework

### Strategy 1: Sign-Based Naive Strategy

- Trade direction is based on the sign of the FOMC-day return
- Implemented in-sample and extended out-of-sample

---

### Strategy 2: Playback Strategy

Two variations:

1. **Cumulative-based strategy**
   - Long 2 days before FOMC
   - Based on cumulative return drift

2. **Average-return strategy**
   - Trades based on average return profile across FOMC window

---

## Performance Results

- Both strategies generate strong in-sample Sharpe ratios
- Out-of-sample performance deteriorates meaningfully

| Strategy | In-Sample Sharpe | Out-of-Sample Sharpe |
|----------|------------------|----------------------|
| Avg Return Strategy | High | Significant drop (~1 Sharpe loss) |
| Naive Strategy | Moderate | Material degradation |

### Key Insight

Most predictive power appears concentrated in-sample. Once implemented in an expanding out-of-sample framework, the edge largely disappears, indicating limited robustness.

---

## Key Takeaways

- FOMC windows exhibit strong structural common-factor behavior in rates
- PCA confirms elevated co-movement around announcement periods
- Sentiment conditioning introduces regime structure but limited standalone edge
- Simple strategies show strong in-sample performance but weak generalization

---

## Next Steps

- Improve robustness across regimes
- Explore nonlinear factor models on PCA components
- Incorporate volatility-adjusted signals
- Add execution and transaction cost modeling