import numpy as np
import pandas as pd
import pandas_ta as ta
from dbhelper import DBHelper

def rsi2(df, column="close", period=14):
    # wilder's RSI
    delta = df[column].diff()
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    r_up = up.ewm(com=period - 1,  adjust=False).mean()
    r_down = down.ewm(com=period - 1, adjust=False).mean().abs()
    rsi = 100 - 100 / (1 + r_up / r_down)
    rsi.name = 'RSI_{}'.format(period)
    # print(rsi)
    df['RSI2_{}'.format(period)] = rsi
    return df

def rsi(df, column="close", n=14):
    # RSI = 100 - (100 / (1 + RS))
    # where RS = (Wilder-smoothed n-period average of gains / Wilder-smoothed n-period average of -losses)
    # Note that losses above should be positive values
    # Wilder-smoothing = ((previous smoothed avg * (n-1)) + current value to average) / n
    # For the very first "previous smoothed avg" (aka the seed value), we start with a straight average.
    # Therefore, our first RSI value will be for the n+2nd period:
    #     0: first delta is nan
    #     1:
    #     ...
    #     n: lookback period for first Wilder smoothing seed value
    #     n+1: first RSI

    # First, calculate the gain or loss from one price to the next. The first value is nan so replace with 0.
    prices = df[column]
    deltas = (prices - prices.shift(1)).fillna(0)

    # Calculate the straight average seed values.
    # The first delta is always zero, so we will use a slice of the first n deltas starting at 1,
    # and filter only deltas > 0 to get gains and deltas < 0 to get losses
    avg_of_gains = deltas[1:n + 1][deltas > 0].sum() / n
    avg_of_losses = -deltas[1:n + 1][deltas < 0].sum() / n

    # Set up pd.Series container for RSI values
    rsi_series = pd.Series(0.0, deltas.index)

    # Now calculate RSI using the Wilder smoothing method, starting with n+1 delta.
    up = lambda x: x if x > 0 else 0
    down = lambda x: -x if x < 0 else 0
    i = n + 1
    for d in deltas[n + 1:]:
        avg_of_gains = ((avg_of_gains * (n - 1)) + up(d)) / n
        avg_of_losses = ((avg_of_losses * (n - 1)) + down(d)) / n
        if avg_of_losses != 0:
            rs = avg_of_gains / avg_of_losses
            rsi_series[i] = 100 - (100 / (1 + rs))
        else:
            rsi_series[i] = 100
        i += 1

    df['RSI_{}'.format(n)] = rsi_series
    return df


db = DBHelper()
df = db.query_stock('YAHOO', 'HK', '0700.HK', letter_case=False)
# print(df.tail(5))
print(rsi(df))
print(rsi2(df))
print(df.ta.rsi(length=14, append=True))