import schedule
import time
from configs.logger import Logger
import services.price as price
from apscheduler.schedulers.background import BackgroundScheduler
from services import analysis
from services import aa_fundamentals

def update_stocks_job():
    logger.info("update_stocks_job scheduler start")
    price.update_all_stocks("YAHOO", "HK", lower_limit=1, expire_mins=10)
    logger.info("scheduler task end")

def update_patterns_job():
    logger.info("update_patterns_job scheduler start")
    analysis.compute_patterns()
    logger.info("scheduler task end")

def update_and_process():
    update_stocks_job()
    update_patterns_job()

def update_income_summary():
    aa_fundamentals.update_all_stocks("YAHOO", "HK")
    aa_fundamentals.export_adjust_fundamentals()

if __name__ == "__main__":
    logger = Logger("MainLogger").setup_system_logger()
    logger.info("stocks price scheduler just restarted")
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_and_process, "cron", hour="18,21,0", minute="0")
    scheduler.add_job(update_income_summary, "cron", year="*", month="*", day="1")
    scheduler.start()
    while True:
        schedule.run_pending()
        time.sleep(1)
