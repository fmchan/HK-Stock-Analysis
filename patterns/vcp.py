import pandas as pd
import numpy as np
from collections import defaultdict
from patterns.all_patterns import Patterns
import pandas_ta as ta
from dbhelper import DBHelper
import logging
from datetime import datetime, timedelta
import os
import sys
from scipy.signal import argrelextrema, argrelmin, argrelmax, find_peaks, find_peaks_cwt

# possible dates found for 0005.HK as reference
# [('2020-01-13', '2020-01-17'), ('2020-05-27', '2020-06-08'), ('2020-06-17', '2020-07-06'), 
# ('2020-10-07', '2020-10-27'), ('2020-10-27', '2020-11-11'), ('2020-11-11', '2020-11-17'), ('2020-11-17', '2020-11-25'), ('2020-11-25', '2020-12-04'), 
# ('2021-01-21', '2021-02-19'), ('2021-02-25', '2021-03-09'), ('2021-04-08', '2021-04-30')]

smoothing = 0
window = 0
prominence = None
# prominence = 0.01 # 0.01 is the best after testing
# neighbour = 4 # when using find_peaks()
# neighbour = 3 # when using argrelextrema()
neighbour = 8

volume_rolling_period = 5

name = Patterns.VCP.name
logger = logging.getLogger('MainLogger')
db = DBHelper()

def get_max_min(df, smoothing, window_range, neighbour, prominence, debug=False):
    max_min = pd.DataFrame()
    try:
        if neighbour == 0: # min neighbour is 1
            neighbour = 1
        if smoothing == 0:
            smooth_prices = df["Close"]
        else:
            smooth_prices = df["Close"].rolling(window=smoothing).mean().dropna()
        local_max = argrelextrema(smooth_prices.values, np.greater, order=neighbour, mode="clip")[0]
        local_min = argrelextrema(smooth_prices.values, np.less, order=neighbour, mode="clip")[0]
        # local_max = find_peaks(smooth_prices.values, distance=neighbour, prominence=prominence)[0]
        # local_max = np.append(local_max, len(df)-1, axis=None)
        # local_min = find_peaks(1/smooth_prices.values, distance=neighbour, prominence=prominence)[0]
        # local_min = np.append(local_min, len(df)-1, axis=None)
        # local_max = find_peaks_cwt(smooth_prices.values, np.arange(1, 1))
        # local_min = find_peaks_cwt(1/smooth_prices.values, np.arange(1, 1))
        price_local_max_dt = []
        for i in local_max:
            if window_range == 0:
                price_local_max_dt.append(df.index.values[i])
            elif (i > window_range) and (i < len(df) - window_range):
                price_local_max_dt.append(df.iloc[i - window_range : i + window_range]["Close"].idxmax())
        price_local_min_dt = []
        for i in local_min:
            if window_range == 0:
                price_local_min_dt.append(df.index.values[i])
            elif (i > window_range) and (i < len(df) - window_range):
                price_local_min_dt.append(df.iloc[i - window_range : i + window_range]["Close"].idxmin())
        if len(price_local_max_dt) > 0 and len(price_local_min_dt) > 0:
            maxima = pd.DataFrame(df.loc[price_local_max_dt])
            maxima['type'] = 'max'
            minima = pd.DataFrame(df.loc[price_local_min_dt])
            minima['type'] = 'min'
            max_min = pd.concat([maxima, minima]).sort_index()
            max_min.index.name = "DateTime"
            max_min['Date'] = pd.to_datetime(max_min["Date"])
            max_min = max_min.reset_index()
            max_min = max_min[~max_min.DateTime.duplicated()]
            # p = df.reset_index()
            # max_min["day_num"] = p[p["Date"].isin(max_min.DateTime)].index.values
            # max_min = max_min.set_index("day_num")["Close"]
            if debug:
                logger.info(maxima[["Close"]])
                logger.info(minima[["Close"]])
        return max_min
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.exception(exc_type, fname, exc_tb.tb_lineno)
        logger.exception(e)
        return ""

def find_wave_patterns(sid, max_min, min_cycle=2, peak_bottom_buffer=1.05, debug=True):
    try:
        cycle_patterns = defaultdict(list)
        peak_patterns = defaultdict(list)
        if len(max_min) > 0:
            max_min = max_min[(max_min["type"].values == 'max').argmax():] # to ensure only start the first max as peak start

            # init_date = max_min.iloc[0]["Date"].date().strftime("%Y-%m-%d")
            init_date = max_min.iloc[0]["Date"]
            init_close = max_min.iloc[0]["Close"]
            prev_cycle_end_date = init_date
            cycle_counter = 0
            prev_peak_start_date = init_date
            prev_peak_bottom = init_close # to compare current bottom to previous bottom
            prev_close = init_close
            prev_volume = 0
            peak_start_date = init_date
            peak_start_close = init_close
            peak_end_date = init_date
            is_peak_bottom = False
            is_peak_bottom_sightly = False
            is_peak_start = False
            is_peak_end = False
            for i in range(1, len(max_min)):
                curr_close = max_min.iloc[i]["Close"]
                curr_volume = max_min.iloc[i]["Volume"]
                # curr_date = max_min.iloc[i]["Date"].date().strftime("%Y-%m-%d")
                curr_date = max_min.iloc[i]["Date"]
                if debug:
                    logger.info("*"*50)
                    logger.info(f"date {str(curr_date)}")
                    logger.info(f"curr_close {curr_close}")
                    logger.info(f"prev_cycle_end_date {prev_cycle_end_date}")
                    logger.info(f"cycle_counter {cycle_counter}")
                    logger.info(f"prev_peak_start_date {prev_peak_start_date}")
                    logger.info(f"prev_peak_bottom {prev_peak_bottom}")
                    logger.info(f"prev_close {prev_close}")
                    logger.info(f"peak_start_date {peak_start_date}")
                    logger.info(f"peak_start_close {peak_start_close}")
                    logger.info(f"is_peak_bottom {is_peak_bottom}")
                    logger.info(f"is_peak_bottom_sightly {is_peak_bottom_sightly}")
                    logger.info(f"is_peak_start {is_peak_start}")
                    logger.info(f"is_peak_end {is_peak_end}")

                if is_peak_start and is_peak_bottom and curr_close >= peak_start_close:
                    is_peak_end = True
                    if debug:
                        logger.info(f"{curr_date} is_peak_end")
                    peak_end_date = curr_date
                elif is_peak_start and curr_close > prev_peak_bottom and curr_close < prev_close and curr_volume < prev_volume:
                    is_peak_bottom = True
                    prev_peak_bottom = curr_close
                    if debug:
                        logger.info(f"{curr_date} is_peak_bottom")
                elif is_peak_start and curr_close > prev_peak_bottom and curr_close < prev_close * peak_bottom_buffer and curr_volume < prev_volume:
                    is_peak_bottom_sightly = True
                    if debug:
                        logger.info(f"{curr_date} is_peak_bottom slightly")
                else:
                    is_peak_start = True
                    if debug:
                        logger.info(f"{curr_date} is_peak_start")
                    is_peak_bottom = False # set back
                    is_peak_bottom_sightly = False # set back
                    peak_start_date = curr_date
                    peak_start_close = curr_close

                prev_close = curr_close
                prev_volume = curr_volume
                if is_peak_start and is_peak_end and cycle_counter == 0:
                    is_peak_start = True # set curr as begining
                    is_peak_bottom = False
                    is_peak_bottom_sightly = False
                    is_peak_end = False
                    prev_peak_start_date = peak_start_date # assign peak_start_date to prev_peak_start_date before reset peak_start_date
                    prev_cycle_end_date = peak_end_date # assign peak_end_date to prev_cycle_end_date before reset peak_end_date
                    peak_start_date = curr_date
                    peak_start_close = curr_close
                    cycle_counter = 1
                    if debug:
                        logger.info(f"first cycle {cycle_counter}")
                elif (i == len(max_min)-1 and cycle_counter >= min_cycle) or \
                (is_peak_start and not is_peak_end and cycle_counter >= min_cycle and prev_cycle_end_date != peak_start_date):
                    cycle_patterns[name].append((prev_peak_start_date.strftime("%Y-%m-%d"), prev_cycle_end_date.strftime("%Y-%m-%d")))
                    db.insert_pattern(prev_cycle_end_date, name, sid) # pattern_end_date is buying date
                    is_peak_start = True # set curr as begining
                    is_peak_bottom = False
                    is_peak_bottom_sightly = False
                    is_peak_end = False
                    prev_cycle_end_date = curr_date
                    peak_start_date = curr_date
                    peak_start_close = curr_close
                    cycle_counter = 0
                    prev_peak_start_date = peak_start_date
                    prev_peak_bottom = 0
                    if debug:
                        logger.info(f"finish cycle {cycle_counter}")
                elif is_peak_start and is_peak_end and cycle_counter > 0 and prev_cycle_end_date == peak_start_date:
                    is_peak_start = True # set curr as begining
                    is_peak_bottom = False
                    is_peak_bottom_sightly = False
                    is_peak_end = False
                    prev_cycle_end_date = peak_end_date # assign peak_end_date to prev_cycle_end_date before reset peak_end_date
                    peak_start_date = curr_date
                    peak_start_close = curr_close
                    if debug:
                        logger.info(f"cycle {cycle_counter} to be +=1 from {prev_peak_start_date} to {curr_date}")
                        peak_patterns[name].append((prev_peak_start_date.strftime("%Y-%m-%d"), curr_date.strftime("%Y-%m-%d")))
                        # # db.insert_pattern(curr_date, name, sid) # pattern_end_date is buying date
                    cycle_counter += 1
                elif is_peak_start and not is_peak_end and not is_peak_bottom and not is_peak_bottom_sightly:
                    cycle_counter = 0
                    prev_peak_start_date = peak_start_date
                    prev_peak_bottom = 0
                    if debug:
                        logger.info(f"transit cycle {cycle_counter}")
                else:
                    if debug:
                        logger.info(f"no action for cycle {cycle_counter}")  
                    pass                  

        logger.info("analysed vcp for : {}".format(sid))
        return cycle_patterns, peak_patterns
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.exception(exc_type, fname, exc_tb.tb_lineno)
        logger.exception(e)
        return ""

def find_vcp_patterns(df, sid, debug=False):
    max_min = get_max_min(df, smoothing, window, neighbour, prominence, debug=debug)
    if debug:
        logger.info(max_min[["Date", "Close", "Volume", "type"]].head(5))
        logger.info(max_min[["Date", "Close", "Volume", "type"]].tail(5))
    cycle_patterns, peak_patterns = find_wave_patterns(sid, max_min, min_cycle=2, peak_bottom_buffer=1.05, debug=debug)
    return cycle_patterns, peak_patterns

def compute_vcp_features(df):
    df["sma_200"] = df["Close"].rolling(200).mean()
    df["obv"] = df.ta.obv(close=df["Close"], volume=df["Volume"], append=False)
    df["rsi"] = df.ta.rsi(close=df["Close"], length=14, append=False)
    df["30w_avg"] = df["Close"].rolling('210D').mean()
    df["40w_avg"] = df["Close"].rolling('280D').mean()

    df["40d_sma"] = df["Close"].rolling(40).mean()
    df["50d_sma"] = df["Close"].rolling(50).mean()
    df["50w_sma"] = df["Close"].rolling(250).mean()
    df["40w_sma"] = df["Close"].rolling(200).mean()
    df["5d_low"] = df["Close"].rolling(5).min()
    df["50w_low"] = df["Close"].rolling(250).min()
    df["5d_high"] = df["Close"].rolling(5).max()
    df["50w_high"] = df["Close"].rolling(250).max()
    df["52w_high"] = df["Close"].rolling(260).max()
    df["13d_ema"] = df.ta.ema(close=df["Close"], length=13, append=False)
    df["26d_ema"] = df.ta.ema(close=df["Close"], length=26, append=False)
    df["13w_ema"] = df.ta.ema(close=df["Close"], length=65, append=False)
    df["26w_ema"] = df.ta.ema(close=df["Close"], length=130, append=False)
    df["5d_volume_sma"] = df["Volume"].rolling(5).mean()

    return df

def find_vcp_patterns_by_ta(df, sid, last_row_no=300):
    cycle_patterns = defaultdict(list)
    start_window_unit = 102
    temp_df = df.copy()
    temp_df = compute_vcp_features(temp_df)
    temp_df = temp_df.iloc[-last_row_no:]
    # logger.info(temp_df[["Close", "Volume", "obv", "rsi", "sma_200", "30w_avg", "40w_avg"]])
    for i in range(start_window_unit, len(temp_df)+1):
        temp_sub_df = temp_df.iloc[i-start_window_unit:i]
        if temp_sub_df.iloc[-2]["obv"] < temp_sub_df.iloc[-1]["obv"] \
        and temp_sub_df.iloc[-2]["rsi"] < temp_sub_df.iloc[-1]["rsi"] \
        and temp_sub_df.iloc[-1]["sma_200"] < temp_sub_df.iloc[-1]["Close"] \
        and temp_sub_df.iloc[-1]["30w_avg"] < temp_sub_df.iloc[-1]["Close"] \
        and temp_sub_df.iloc[-1]["40w_avg"] < temp_sub_df.iloc[-1]["Close"] \
        and temp_sub_df.iloc[-1]["40w_avg"] < temp_sub_df.iloc[-1]["30w_avg"] \
        and temp_sub_df.iloc[-2]["Volume"] < temp_sub_df.iloc[-1]["Volume"] \
        and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-1]["Close"]:
            start_date = temp_sub_df.iloc[-1]["Date"]
            name = "VCP_TA1"
            cycle_patterns[name].append(start_date)
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            db.insert_pattern(start_date, name, sid)

        # 13 week EMA > 26 week EMA and
        # 26 week EMA > 50 week SMA and
        # 40 week SMA > 40 day SMA 5 weeks ago and
        # Close >= 50 week low * 1.3 and
        # Close >= 50 week high * 0.75 and
        # 13 day EMA 20 days ago > 26 day EMA 20 weeks ago and
        # 40 day SMA 5 weeks ago > 40 day SMA 10 weeks ago and
        # Close > 50 day SMA
        if temp_sub_df.iloc[-1]["13w_ema"] > temp_sub_df.iloc[-1]["26w_ema"] \
        and temp_sub_df.iloc[-1]["26w_ema"] > temp_sub_df.iloc[-1]["50w_sma"] \
        and temp_sub_df.iloc[-1]["40w_sma"] > temp_sub_df.iloc[-26]["40w_sma"] \
        and temp_sub_df.iloc[-1]["Close"] >= temp_sub_df.iloc[-1]["50w_low"] *1.3 \
        and temp_sub_df.iloc[-1]["Close"] >= temp_sub_df.iloc[-1]["50w_high"] *0.75 \
        and temp_sub_df.iloc[-21]["13d_ema"] > temp_sub_df.iloc[-101]["26d_ema"] \
        and temp_sub_df.iloc[-26]["40d_sma"] > temp_sub_df.iloc[-51]["40d_sma"] \
        and temp_sub_df.iloc[-1]["Close"] > temp_sub_df.iloc[-1]["50d_sma"] \
        and temp_sub_df.iloc[-2]["Volume"] < temp_sub_df.iloc[-1]["Volume"] \
        and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-1]["Close"]:
            start_date = temp_sub_df.iloc[-1]["Date"]
            name = "VCP_TA2"
            cycle_patterns[name].append(start_date)
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            db.insert_pattern(start_date, name, sid)

        # Volume < 5 day SMA Volume for 5 days consequently
        # Close < 52 week high and Close > 52 week high * 0.6
        # (5 day high - 5 day low) / Close < 0.1
        # 5 day Close == 5 day high
        if temp_sub_df.iloc[-1]["Volume"] < temp_sub_df.iloc[-1]["5d_volume_sma"] \
        and temp_sub_df.iloc[-2]["Volume"] < temp_sub_df.iloc[-2]["5d_volume_sma"] \
        and temp_sub_df.iloc[-3]["Volume"] < temp_sub_df.iloc[-3]["5d_volume_sma"] \
        and temp_sub_df.iloc[-4]["Volume"] < temp_sub_df.iloc[-4]["5d_volume_sma"] \
        and temp_sub_df.iloc[-5]["Volume"] < temp_sub_df.iloc[-5]["5d_volume_sma"] \
        and temp_sub_df.iloc[-1]["Close"] < temp_sub_df.iloc[-1]["52w_high"] \
        and temp_sub_df.iloc[-1]["Close"] >= temp_sub_df.iloc[-1]["52w_high"] *0.6 \
        and ((temp_sub_df.iloc[-1]["5d_high"] - temp_sub_df.iloc[-1]["5d_low"]) /temp_sub_df.iloc[-1]["Close"]) < 0.1 \
        and temp_sub_df.iloc[-1]["5d_high"] == temp_sub_df.iloc[-5]["Close"]:
            start_date = temp_sub_df.iloc[-1]["Date"]
            name = "VCP_TA3"
            cycle_patterns[name].append(start_date)
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            db.insert_pattern(start_date, name, sid)

    logger.info("analysed VCP TA for : {}".format(sid))
    return cycle_patterns