import pandas as pd
import pandas_ta as ta
from dbhelper import DBHelper

db = DBHelper()
df = db.query_stock('YAHOO', 'HK', '0700.HK', letter_case=False)

df.ta.log_return(cumulative=True, append=True)
df.ta.percent_return(cumulative=True, append=True)
df.ta.sma(length=10, append=True)
df.ta.macd(fast=12, slow=26, signal=9, append=True)

for i in range(2, df.shape[0]):
    current = df.iloc[i,:]
    prev = df.iloc[i-1,:]
    prev_2 = df.iloc[i-2,:]
realbody = abs(current['open'] - current['close'])
candle_range = current['high'] - current['low']
idx = df.index[i]

df.loc[idx, 'Bullish swing'] = current['low'] > prev['low'] and prev['low'] < prev_2['low']
df.loc[idx, 'Bearish swing'] = current['high'] < prev['high'] and prev['high'] > prev_2['high']
df.loc[idx, 'Bullish pinbar'] = realbody <= candle_range/3 and min(current['open'], current['close']) > (current['high'] + current['low'])/2 and current['low'] < prev['low']
df.loc[idx, 'Bearish pinbar'] = realbody <= candle_range/3 and max(current['open'], current['close']) < (current['high'] + current['low'])/2 and current['high'] > prev['high']
df.loc[idx, 'Inside bar'] = current['high'] < prev['high'] and current['low'] > prev['low']
df.loc[idx, 'Outside bar'] = current['high'] > prev['high'] and current['low'] < prev['low']
df.loc[idx, 'Bullish engulfing'] = current['high'] > prev['high'] and current['low'] < prev['low'] and realbody >= 0.8 * candle_range and current['close'] > current['open']
df.loc[idx, 'Bearish engulfing'] = current['high'] > prev['high'] and current['low'] < prev['low'] and realbody >= 0.8 * candle_range and current['close'] < current['open']

df.fillna(False, inplace=True)
print(df.tail(5))