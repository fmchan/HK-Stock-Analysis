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

def compute_differential_features(df):
    df['true_low'] = df[['low', 'close_1d']].min(axis=1)
    df['true_high'] = df[['high', 'close_1d']].max(axis=1)
    df['buying_pressure'] = df.close - df['true_low']
    df['selling_pressure'] = df.close - df['true_high']
    return df

def find_differential_patterns(df):
    patterns = defaultdict(list)
    temp_df = df.copy()
    temp_df = compute_differential_features(temp_df)
    start_window_unit = 6
    for i in range(start_window_unit, len(df)+1):
        temp_sub_df = temp_df.iloc[i-start_window_unit:i]
        # print(temp_sub_df)
        if temp_sub_df.iloc[-1]['close'] < temp_sub_df.iloc[-2]['close'] and temp_sub_df.iloc[-2]['close'] < temp_sub_df.iloc[-3]['close'] and \
            temp_sub_df.iloc[-1]['true_low'] > temp_sub_df.iloc[-2]['true_low'] and temp_sub_df.iloc[-1]['true_high'] < temp_sub_df.iloc[-2]['true_high']:
            # patterns[Patterns.TD_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.iloc[-1]['date'])

        if temp_sub_df.iloc[-1]['close'] > temp_sub_df.iloc[-2]['close'] and temp_sub_df.iloc[-2]['close'] > temp_sub_df.iloc[-3]['close'] and \
            temp_sub_df.iloc[-1]['true_low'] < temp_sub_df.iloc[-2]['true_low'] and temp_sub_df.iloc[-1]['true_high'] > temp_sub_df.iloc[-2]['true_high']:
            # patterns[Patterns.TD_DIFFERENTIAL_EXIT.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_DIFFERENTIAL_EXIT.name].append(temp_sub_df.iloc[-1]['date'])

        if temp_sub_df.iloc[-1]['close'] < temp_sub_df.iloc[-2]['close'] and temp_sub_df.iloc[-2]['close'] < temp_sub_df.iloc[-3]['close'] and \
            temp_sub_df.iloc[-1]['true_low'] < temp_sub_df.iloc[-2]['true_low'] and temp_sub_df.iloc[-1]['true_high'] > temp_sub_df.iloc[-2]['true_high']:
            # patterns[Patterns.TD_REVERSE_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_REVERSE_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.iloc[-1]['date'])

        if temp_sub_df.iloc[-1]['close'] > temp_sub_df.iloc[-2]['close'] and temp_sub_df.iloc[-2]['close'] > temp_sub_df.iloc[-3]['close'] and \
            temp_sub_df.iloc[-1]['true_low'] > temp_sub_df.iloc[-2]['true_low'] and temp_sub_df.iloc[-1]['true_high'] < temp_sub_df.iloc[-2]['true_high']:
            # patterns[Patterns.TD_REVERSE_DIFFERENTIAL_EXIT.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_REVERSE_DIFFERENTIAL_EXIT.name].append(temp_sub_df.iloc[-1]['date'])

        if temp_sub_df.iloc[-1]['close'] < temp_sub_df.iloc[-2]['close'] and temp_sub_df.iloc[-2]['close'] > temp_sub_df.iloc[-3]['close'] and \
            temp_sub_df.iloc[-3]['close'] < temp_sub_df.iloc[-4]['close'] and temp_sub_df.iloc[-4]['close'] < temp_sub_df.iloc[-5]['close']:
            # patterns[Patterns.TD_ANTI_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_ANTI_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.iloc[-1]['date'])

        if temp_sub_df.iloc[-1]['close'] > temp_sub_df.iloc[-2]['close'] and temp_sub_df.iloc[-2]['close'] < temp_sub_df.iloc[-3]['close'] and \
            temp_sub_df.iloc[-3]['close'] > temp_sub_df.iloc[-4]['close'] and temp_sub_df.iloc[-4]['close'] > temp_sub_df.iloc[-5]['close']:
            # patterns[Patterns.TD_ANTI_DIFFERENTIAL_EXIT.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_ANTI_DIFFERENTIAL_EXIT.name].append(temp_sub_df.iloc[-1]['date'])
        # print(temp_sub_df.iloc[-1]['date])

    return patterns