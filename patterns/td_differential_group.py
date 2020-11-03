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

def compute_differential_features(df):
    df['true_low'] = df[['Low', 'close_1d']].min(axis=1)
    df['true_high'] = df[['High', 'close_1d']].max(axis=1)
    df['buying_pressure'] = df.Close - df['true_low']
    df['selling_pressure'] = df.Close - df['true_high']
    return df

def td_differential(sid, df):
    temp_df = df.copy()
    temp_df = compute_differential_features(temp_df)

    if temp_df.iloc[-1]['Close'] < temp_df.iloc[-2]['Close'] and temp_df.iloc[-2]['Close'] < temp_df.iloc[-3]['Close'] and \
        temp_df.iloc[-1]['true_low'] > temp_df.iloc[-2]['true_low'] and temp_df.iloc[-1]['true_high'] < temp_df.iloc[-2]['true_high']:
        print('{} has buy signal of td differential'.format(sid))

    if temp_df.iloc[-1]['Close'] > temp_df.iloc[-2]['Close'] and temp_df.iloc[-2]['Close'] > temp_df.iloc[-3]['Close'] and \
        temp_df.iloc[-1]['true_low'] < temp_df.iloc[-2]['true_low'] and temp_df.iloc[-1]['true_high'] > temp_df.iloc[-2]['true_high']:
        print('{} has sell signal of td differential'.format(sid))

def td_reverse_differential(sid, df):
    temp_df = df.copy()
    temp_df = compute_differential_features(temp_df)

    if temp_df.iloc[-1]['Close'] < temp_df.iloc[-2]['Close'] and temp_df.iloc[-2]['Close'] < temp_df.iloc[-3]['Close'] and \
        temp_df.iloc[-1]['true_low'] < temp_df.iloc[-2]['true_low'] and temp_df.iloc[-1]['true_high'] > temp_df.iloc[-2]['true_high']:
        print('{} has buy signal of td reverse differential'.format(sid))

    if temp_df.iloc[-1]['Close'] > temp_df.iloc[-2]['Close'] and temp_df.iloc[-2]['Close'] > temp_df.iloc[-3]['Close'] and \
        temp_df.iloc[-1]['true_low'] > temp_df.iloc[-2]['true_low'] and temp_df.iloc[-1]['true_high'] < temp_df.iloc[-2]['true_high']:
        print('{} has sell signal of td reverse differential'.format(sid))

def td_anti_differential(sid, df):
    temp_df = df.copy()
    temp_df = compute_differential_features(temp_df)

    if temp_df.iloc[-1]['Close'] < temp_df.iloc[-2]['Close'] and temp_df.iloc[-2]['Close'] > temp_df.iloc[-3]['Close'] and \
        temp_df.iloc[-3]['Close'] < temp_df.iloc[-4]['Close'] and temp_df.iloc[-4]['Close'] < temp_df.iloc[-5]['Close']:
        print('{} has buy signal of td anti differential'.format(sid))

    if temp_df.iloc[-1]['Close'] > temp_df.iloc[-2]['Close'] and temp_df.iloc[-2]['Close'] < temp_df.iloc[-3]['Close'] and \
        temp_df.iloc[-3]['Close'] > temp_df.iloc[-4]['Close'] and temp_df.iloc[-4]['Close'] > temp_df.iloc[-5]['Close']:
        print('{} has sell signal of td anti differential'.format(sid))