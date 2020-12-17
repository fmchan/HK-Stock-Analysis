import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import multiprocessing
from configs.settings import DATA_PATH, CURRENT_VOLUME_FILTER
import services.price as price
from configs.logger import Logger
import patterns.cup_handle as cup_handle
import patterns.td_differential_group as td_differential_group
import patterns.vcp as vcp
from dbhelper import DBHelper
from datautils import pattern_utils

def compute_features(df):
    df['Date'] = pd.to_datetime(df.index)
    df['sma_volume_5'] = df.Volume.rolling(window=5).mean()
    df['min_volume_5'] = df.Volume.rolling(window=5).min()
    df['sma_20'] = df.Close.rolling(window=20).mean()
    df['sma_10'] = df.Close.rolling(window=10).mean()
    df['std_20'] = df.Close.rolling(window=20).std() 
    df['upper_band'] = df['sma_20'] + (df['std_20'] * 2)
    df['lower_band'] = df['sma_20'] - (df['std_20'] * 2)
    df['sma10_over_sma20'] = (df['sma_10'] / df['sma_20'])
    df['min_20'] = df.Close.rolling(window=10).min()
    df['close_1d'] = df.Close.shift(1)
    df = df[20:] # Drop first row because NAN
    return df

if __name__ == '__main__':
    logger = Logger('MainLogger').setup_system_logger()
    # fail_stock_list = []
    # day_delta = 1_000
    # # price.insert_all_stocks(day_delta)
    # price.update_all_stocks('YAHOO', 'HK')
    # price.update_failed_stocks('YAHOO', 'HK')

    # # sid = '8083.HK'
    # # sid = '0670.HK'
    # # sid = '1055.HK'
    # # sid = '0753.HK'
    # # sid = '6865.HK'

    # sid = '3838.HK'

    db = DBHelper()
    stocks_df = db.query_stock_by_volume(5, CURRENT_VOLUME_FILTER)
    count = 0
    for index, stock_df in stocks_df.iterrows():
        sid = stock_df['sid']
        df = db.query_stock('YAHOO', 'HK', sid, start='2019-12-01', letter_case=False)
        if len(df) > 50 and df.iloc[-1]['close'] > 1:
            print("processing: {}".format(sid))
            # df = compute_features(df)
            # # Cup & Handle
            # cup_patterns = cup_handle.find_cup_patterns(df)
            # pattern_utils.show_pair_patterns(cup_patterns)

            # TD Differential Group
            # differential_patterns = td_differential_group.find_differential_patterns(df)
            # pattern_utils.show_single_patterns(differential_patterns)
            # td_differential_group.td_differential(sid, df)
            # td_differential_group.td_reverse_differential(sid, df)
            # td_differential_group.td_anti_differential(sid, df)

            # VCP
            vcp_patterns = vcp.find_patterns(df)
            pattern_utils.show_single_patterns(vcp_patterns)