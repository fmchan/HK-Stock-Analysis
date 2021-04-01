from configs.settings import DATA_PATH, CURRENT_VOLUME_FILTER, DB_QUERY_START_DATE
from configs.logger import Logger
import time
from services import analysis
from services import price

if __name__ == "__main__":
    # logger = Logger("MainLogger").setup_system_logger()
    start_time = time.time()
    price.insert_all_stocks()
    # price.update_all_stocks("YAHOO", "HK")
    # price.update_failed_stocks("YAHOO", "HK")
    analysis.compute_patterns(last_row_no=100)
    print("program takes %s seconds", time.time() - start_time)