import schedule
import time
from configs.logger import Logger
import services.price as price
from apscheduler.schedulers.background import BackgroundScheduler
from configs.logger import Logger
import patterns.cup_handle as cup_handle
import patterns.td_differential_group as td_differential_group
import patterns.trendline as trendline
import patterns.vcp as vcp
from dbhelper import DBHelper
from datautils import pattern_utils
import pandas as pd
from configs.settings import DATA_PATH, CURRENT_VOLUME_FILTER, DB_QUERY_START_DATE

def update_stocks_job():
    logger.info("update_stocks_job scheduler start")
    price.update_all_stocks("YAHOO", "HK", lower_limit=1, expire_mins=60)
    logger.info("scheduler task end")

def update_patterns_job():
    logger.info("update_patterns_job scheduler start")
    db = DBHelper()
    stocks_df = db.query_stock_by_volume(5, CURRENT_VOLUME_FILTER)
    for index, stock_df in stocks_df.iterrows():
        sid = stock_df["sid"]
        df = db.query_stock("YAHOO", "HK", sid, start=DB_QUERY_START_DATE, letter_case=True)
        if len(df) > 260 and df.iloc[-1]["Close"] > 1: # 260 = 52*5
            # print("processing: {}".format(sid))
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
            vcp_patterns = vcp.find_patterns(df, sid)
            # pattern_utils.show_single_patterns(sid, vcp_patterns)

            # Trend Lines
            # flat_base_patterns = trendline.find_flat_base_patterns(df, sid)
            # pattern_utils.show_pair_patterns(sid, flat_base_patterns)
    logger.info("scheduler task end")

if __name__ == "__main__":
    logger = Logger("MainLogger").setup_system_logger()
    logger.info("stocks price scheduler just restarted")
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_stocks_job, "cron", hour="0", minute="0")
    scheduler.add_job(update_patterns_job, "cron", hour="4", minute="0")
    scheduler.start()
    while True:
        schedule.run_pending()
        time.sleep(1)
