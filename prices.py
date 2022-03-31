import pandas as pd
import numpy as np


def prices(tickers=None, start=None, end=None):
    df = pd.read_parquet("prices.parquet")
    filters = pd.array([True] * len(df.index))
    if start:
        filters &= df.index >= start

    if end:
        filters &= df.index <= end

    if tickers:
        df = df[filters & df["Symbol"].isin(tickers).array]
    else:
        df = df[filters]

    return df.pivot(columns="Symbol", values="Close").ffill().replace({np.nan: None})
