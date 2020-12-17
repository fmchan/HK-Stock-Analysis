import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
from datetime import datetime, timedelta
import os
import sys
import multiprocessing
from scipy.signal import argrelextrema, argrelmin, argrelmax, find_peaks, find_peaks_cwt
from collections import defaultdict
from configs.settings import DATA_PATH, CURRENT_VOLUME_FILTER
from dbhelper import DBHelper
import services.price as price
from patterns.all_patterns import Patterns
import pandas_ta as ta

def compute_vcp_features(df):
    df['sma_150'] = df.ta.sma(df.close, length=int(150))
    df['sma_200'] = df.ta.sma(df.close, length=int(200))
    df['52w_low'] = df.close.rolling(250).min()
    return df

def find_patterns(df):
    patterns = defaultdict(list)
    temp_df = df.copy()
    temp_df = compute_vcp_features(temp_df)
    start_window_unit = 3
    for i in range(start_window_unit, len(df)+1):
        temp_sub_df = temp_df.iloc[i-start_window_unit:i]
        # print(temp_sub_df)
        if temp_sub_df.iloc[-1]['sma_200'] < temp_sub_df.iloc[-1]['sma_150'] \
          and temp_sub_df.iloc[-1]['sma_150'] < temp_sub_df.iloc[-1]['close'] \
          and temp_sub_df.iloc[-1]['52w_low'] * 1.25 < temp_sub_df.iloc[-1]['close'] \
          and temp_sub_df.iloc[-2]['volume'] < temp_sub_df.iloc[-1]['volume'] \
          and temp_sub_df.iloc[-2]['close'] < temp_sub_df.iloc[-1]['close']:
            patterns[Patterns.VCP.name].append(temp_sub_df.iloc[-1]['date'])
    return patterns