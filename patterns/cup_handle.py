import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from scipy.signal import argrelextrema, argrelmin, argrelmax, find_peaks, find_peaks_cwt
from collections import defaultdict
from patterns.all_patterns import Patterns

smoothing = 0
window = 0
prominence = 0.01 # 0.01 is the best after testing
neighbour = 4 # when using find_peaks()
# neighbour = 3 # when using argrelextrema()

# min_period = 20
min_period = 27
max_radius_difference = 5
peak_depth = 1.05
step_depth = 1.01
start_end_price_ratio_difference = 0.07

def get_max_min(df, smoothing, window_range, neighbour, prominence, debug=False):
    try:
        if neighbour == 0: # min neighbour is 1
            neighbour = 1
        if smoothing == 0:
            smooth_prices = df["Close"]
        else:
            smooth_prices = df["Close"].rolling(window=smoothing).mean().dropna()
        # local_max = argrelextrema(smooth_prices.values, np.greater, order=neighbour, mode="clip")[0]
        # local_min = argrelextrema(smooth_prices.values, np.less, order=neighbour, mode="clip")[0]
        local_max = find_peaks(smooth_prices.values, distance=neighbour, prominence=prominence)[0]
        local_max = np.append(local_max, len(df)-1, axis=None)
        local_min = find_peaks(1/smooth_prices.values, distance=neighbour, prominence=prominence)[0]
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
        maxima = pd.DataFrame(df.loc[price_local_max_dt])
        minima = pd.DataFrame(df.loc[price_local_min_dt])
        max_min = pd.concat([maxima, minima]).sort_index()
        max_min.index.name = "DateTime"
        max_min = max_min.reset_index()
        max_min = max_min[~max_min.DateTime.duplicated()]
        # p = df.reset_index()
        # max_min["day_num"] = p[p["Date"].isin(max_min.DateTime)].index.values
        # max_min = max_min.set_index("day_num")["Close"]
        if debug:
            print(maxima[["Close"]])
            print(minima[["Close"]])
        return max_min
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        return ""

def find_patterns(max_min, min_period=27, peak_depth=1.05, start_end_price_ratio_difference=0.07, max_radius_difference=5, step_depth=1.01):
    try:
        patterns = defaultdict(list)
        # window_units = 6
        for window_units in range(3, 10):
            for i in range(window_units, len(max_min)+1):
                window = max_min.iloc[i - window_units:i]
                if window.index[-1] - window.index[0] > 100:
                    continue   

                closes = []
                volumes = []
                dates = []
                for j in range(0, window_units):
                    closes.append(window.iloc[j]["Close"])
                    volumes.append(window.iloc[j]["Volume"])
                    dates.append(window.iloc[j]["Date"])
                pattern_day_difference = (dates[window_units-1] - dates[0]).days
                start_end_price_ratio = closes[window_units-1] / closes[0] if closes[window_units-1] > closes[0] else closes[0] / closes[window_units-1]

                min_index = closes.index(min(closes))
                min_close = min(closes)
                entry_bottom_day_difference = (dates[min_index] - dates[0]).days
                bottom_end_day_difference = (dates[window_units-1] - dates[min_index]).days
                entry_bottom_price_ratio = closes[0] / min_close
                bottom_end_price_ratio = closes[window_units-1] / min_close
                criteria_1 = False
                criteria_2 = False
                for k in range(0, min_index):
                    if closes[k] > closes[k+1] and closes[k] / closes[k+1] > step_depth:
                        criteria_1 = True
                        continue
                    else:
                        criteria_1 = False
                        break
                if min_index >= 1 and window_units > 3:
                    for l in range(min_index, window_units-1):
                        if closes[l] < closes[l+1] and closes[l+1] / closes[l] > step_depth:
                            criteria_2 = True
                            continue
                        else:
                            criteria_2 = False
                            break
                elif closes[1] < closes[2] and closes[2] / closes[1] > step_depth:
                    criteria_2 = True
                else:
                    criteria_2 = False

                if criteria_1 and criteria_2 \
                    and volumes[min_index] < volumes[0] and volumes[min_index] < volumes[window_units-1] \
                    and entry_bottom_price_ratio > peak_depth and bottom_end_price_ratio > peak_depth \
                    and 1 - start_end_price_ratio_difference < start_end_price_ratio < 1 + start_end_price_ratio_difference \
                    and pattern_day_difference >= min_period \
                    and abs(entry_bottom_day_difference - bottom_end_day_difference) <= max_radius_difference:
                        patterns[Patterns.CUP_HANDLE.name].append((window.iloc[0]["Date"], window.iloc[-1]["Date"]))

        return patterns
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        return ""

def find_cup_patterns(df):
    minmax = get_max_min(df, smoothing, window, neighbour, prominence, debug=False)
    # print(minmax[["Date", "Close", "Volume"]].tail(20))
    patterns = find_patterns(minmax, min_period, peak_depth, start_end_price_ratio_difference, max_radius_difference, step_depth)
    return patterns