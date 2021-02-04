import time
import logging
import patterns.cup_handle as cup_handle
import patterns.td_differential_group as td_differential_group
import patterns.trendline as trendline
import patterns.vcp as vcp
from dbhelper import DBHelper
from datautils import pattern_utils
import pandas as pd
from configs.settings import DATA_PATH, CURRENT_VOLUME_FILTER, DB_QUERY_START_DATE
from datetime import datetime

def compute_patterns():
    logger = logging.getLogger('MainLogger')
    db = DBHelper()
    stocks_df = db.query_stock_by_volume(5, CURRENT_VOLUME_FILTER)
    logger.info(len(stocks_df))
    for index, stock_df in stocks_df.iterrows():
        try:
            sid = stock_df["sid"]
            logger.info("processing analysis: {}".format(sid))
            df = db.query_stock("YAHOO", "HK", sid, start=DB_QUERY_START_DATE, letter_case=True)
            if isinstance(df, pd.DataFrame) and len(df) > 260 and df.iloc[-1]["Close"] > 1: # 260 = 52*5
                # df = compute_features(df)

                # # Cup & Handle
                cup_patterns = cup_handle.find_cup_patterns(df, sid)
                # pattern_utils.show_pair_patterns(sid, cup_patterns)

                # TD Differential Group
                # differential_patterns = td_differential_group.find_differential_patterns(df, sid)
                # pattern_utils.show_single_patterns(sid, differential_patterns)
                # td_differential_group.td_differential(sid, df)
                # td_differential_group.td_reverse_differential(sid, df)
                # td_differential_group.td_anti_differential(sid, df)

                # VCP
                vcp_patterns = vcp.find_patterns(df, sid, last_row_no=3) # only compute last 3 rows
                # pattern_utils.show_single_patterns(sid, vcp_patterns)

                # Trend Lines
                # flat_base_patterns = trendline.find_flat_base_patterns(df, sid)
                # pattern_utils.show_pair_patterns(sid, flat_base_patterns)
        except Exception as e:
            message = "Exception in update_patterns_job for %s: %s" % sid, e
            logger.exception(message + str(e))