import time
import logging
import patterns.cup_handle as cup_handle
import patterns.td_differential_group as td_differential_group
import patterns.trendline as trendline
import patterns.vcp as vcp
from dbhelper import DBHelper
from datautils import pattern_utils
import pandas as pd
from configs.settings import DATA_PATH, CURRENT_VOLUME_FILTER, DB_QUERY_START_DATE, CPU_COUNT
from datetime import datetime
import multiprocessing as mp
from configs.logger import Logger

logger = Logger("MainLogger").setup_system_logger() # setup pool.apply_async logging
# logger = logging.getLogger('MainLogger') 
db = DBHelper()

def compute_patterns(last_row_no=3):
    stocks_df = db.query_stock_by_volume(5, CURRENT_VOLUME_FILTER)
    logger.info(len(stocks_df))

    # for index, stock_df in stocks_df.iterrows():
    #     _compute_patterns_core(stock_df["sid"], last_row_no)

    with mp.Pool(processes=CPU_COUNT) as pool:
        results = [pool.apply_async(_compute_patterns_core, args=[stock_df["sid"], last_row_no]) for index, stock_df in stocks_df.iterrows()]
        for result in results:
            logger.info(result.get())

def _compute_patterns_core(sid, last_row_no):
    try:
        logger.info("processing analysis: {}".format(sid))
        df = db.query_stock("YAHOO", "HK", sid, start=DB_QUERY_START_DATE, letter_case=True)
        if isinstance(df, pd.DataFrame) and len(df) > 260: # 260 = 52*5
            # Cup & Handle
            cup_patterns = cup_handle.find_cup_patterns(df, sid)
            pattern_utils.show_pair_patterns(sid, cup_patterns)

            # TD Differential Group
            # differential_patterns = td_differential_group.find_differential_patterns(df, sid)
            # pattern_utils.show_single_patterns(sid, differential_patterns)
            # td_differential_group.td_differential(sid, df)
            # td_differential_group.td_reverse_differential(sid, df)
            # td_differential_group.td_anti_differential(sid, df)

            # SEPA
            vcp_patterns = vcp.find_patterns(df, sid, last_row_no=last_row_no) # only compute last 3 rows
            pattern_utils.show_single_patterns(sid, vcp_patterns)

            # Trend Lines
            # flat_base_patterns = trendline.find_flat_base_patterns(df, sid)
            # pattern_utils.show_pair_patterns(sid, flat_base_patterns)
        return sid + " successes"
    except Exception as e:
        message = "Exception in update_patterns_job for %s: %s" % sid, e
        logger.exception(message + str(e))
        return sid + " fails"