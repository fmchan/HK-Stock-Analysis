import pandas as pd
import numpy as np
from pandas.plotting import register_matplotlib_converters
from datetime import datetime, timedelta
from collections import defaultdict
from patterns.all_patterns import Patterns
from dbhelper import DBHelper
from datetime import datetime

def compute_differential_features(df):
    df["true_low"] = df[["Low", "close_1d"]].min(axis=1)
    df["true_high"] = df[["High", "close_1d"]].max(axis=1)
    df["buying_pressure"] = df["Close"] - df["true_low"]
    df["selling_pressure"] = df["Close"] - df["true_high"]
    return df

def find_differential_patterns(df, sid):
    patterns = defaultdict(list)
    temp_df = df.copy()
    temp_df = compute_differential_features(temp_df)
    start_window_unit = 6
    db = DBHelpder()
    for i in range(start_window_unit, len(temp_df)+1):
        temp_sub_df = temp_df.iloc[i-start_window_unit:i]
        # print(temp_sub_df)
        if temp_sub_df.iloc[-1]["Close"] < temp_sub_df.iloc[-2]["Close"] and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-3]["Close"] and \
            temp_sub_df.iloc[-1]["true_low"] > temp_sub_df.iloc[-2]["true_low"] and temp_sub_df.iloc[-1]["true_high"] < temp_sub_df.iloc[-2]["true_high"]:
            # patterns[Patterns.TD_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.iloc[-1]["Date"])

        if temp_sub_df.iloc[-1]["Close"] > temp_sub_df.iloc[-2]["Close"] and temp_sub_df.iloc[-2]["Close"] > temp_sub_df.iloc[-3]["Close"] and \
            temp_sub_df.iloc[-1]["true_low"] < temp_sub_df.iloc[-2]["true_low"] and temp_sub_df.iloc[-1]["true_high"] > temp_sub_df.iloc[-2]["true_high"]:
            # patterns[Patterns.TD_DIFFERENTIAL_EXIT.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_DIFFERENTIAL_EXIT.name].append(temp_sub_df.iloc[-1]["Date"])

        if temp_sub_df.iloc[-1]["Close"] < temp_sub_df.iloc[-2]["Close"] and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-3]["Close"] and \
            temp_sub_df.iloc[-1]["true_low"] < temp_sub_df.iloc[-2]["true_low"] and temp_sub_df.iloc[-1]["true_high"] > temp_sub_df.iloc[-2]["true_high"]:
            # patterns[Patterns.TD_REVERSE_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_REVERSE_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.iloc[-1]["Date"])

        if temp_sub_df.iloc[-1]["Close"] > temp_sub_df.iloc[-2]["Close"] and temp_sub_df.iloc[-2]["Close"] > temp_sub_df.iloc[-3]["Close"] and \
            temp_sub_df.iloc[-1]["true_low"] > temp_sub_df.iloc[-2]["true_low"] and temp_sub_df.iloc[-1]["true_high"] < temp_sub_df.iloc[-2]["true_high"]:
            # patterns[Patterns.TD_REVERSE_DIFFERENTIAL_EXIT.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_REVERSE_DIFFERENTIAL_EXIT.name].append(temp_sub_df.iloc[-1]["Date"])

        if temp_sub_df.iloc[-1]["Close"] < temp_sub_df.iloc[-2]["Close"] and temp_sub_df.iloc[-2]["Close"] > temp_sub_df.iloc[-3]["Close"] and \
            temp_sub_df.iloc[-3]["Close"] < temp_sub_df.iloc[-4]["Close"] and temp_sub_df.iloc[-4]["Close"] < temp_sub_df.iloc[-5]["Close"]:
            # patterns[Patterns.TD_ANTI_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_ANTI_DIFFERENTIAL_ENTRY.name].append(temp_sub_df.iloc[-1]["Date"])

        if temp_sub_df.iloc[-1]["Close"] > temp_sub_df.iloc[-2]["Close"] and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-3]["Close"] and \
            temp_sub_df.iloc[-3]["Close"] > temp_sub_df.iloc[-4]["Close"] and temp_sub_df.iloc[-4]["Close"] > temp_sub_df.iloc[-5]["Close"]:
            # patterns[Patterns.TD_ANTI_DIFFERENTIAL_EXIT.name].append(temp_sub_df.index[-1])
            patterns[Patterns.TD_ANTI_DIFFERENTIAL_EXIT.name].append(temp_sub_df.iloc[-1]["Date"])
        # print(temp_sub_df.iloc[-1]["Date])

    return patterns