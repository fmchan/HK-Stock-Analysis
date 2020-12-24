import pandas as pd
import numpy as np
from collections import defaultdict
from patterns.all_patterns import Patterns
import pandas_ta as ta


volume_rolling_period = 5

def compute_vcp_features(df):
    df['avg_volume_{}d'.format(volume_rolling_period)] = df['volume'].rolling(volume_rolling_period).mean()
    df['sma_10'] = df.ta.sma(df.close, length=int(10))
    df['sma_150'] = df.ta.sma(df.close, length=int(150))
    df['sma_200'] = df.ta.sma(df.close, length=int(200))
    df['52w_low'] = df.close.rolling(250).min()
    return df

def find_patterns(df):
    patterns = defaultdict(list)
    start_window_unit = 3
    last_row_no = 30
    temp_df = df.copy()
    temp_df = compute_vcp_features(temp_df)
    temp_df = temp_df.iloc[-last_row_no:]
    # print(temp_df[['close', 'sma_10', 'sma_150', 'sma_200', '52w_low']])
    for i in range(start_window_unit, len(temp_df)+1):
        temp_sub_df = temp_df.iloc[i-start_window_unit:i]
        # print(temp_sub_df)
        if temp_sub_df.iloc[-1]['sma_200'] < temp_sub_df.iloc[-1]['sma_150'] \
          and temp_sub_df.iloc[-1]['sma_150'] < temp_sub_df.iloc[-1]['close'] \
          and temp_sub_df.iloc[-1]['52w_low'] * 1.25 < temp_sub_df.iloc[-1]['close'] \
          and temp_sub_df.iloc[-1]['avg_volume_{}d'.format(volume_rolling_period)] < temp_sub_df.iloc[-1]['volume'] \
          and temp_sub_df.iloc[-2]['close'] < temp_sub_df.iloc[-1]['close']:
            patterns[Patterns.VCP.name].append(temp_sub_df.iloc[-1]['date'])
    return patterns