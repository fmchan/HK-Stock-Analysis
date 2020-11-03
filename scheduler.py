import schedule
import time
from configs.logger import Logger
import services.price as price
from apscheduler.schedulers.background import BackgroundScheduler

def update_stocks_job():
    logger.info('update_stocks_job scheduler start')
    price.update_all_stocks('YAHOO', 'HK', lower_limit=1, expire_mins=60)
    logger.info('scheduler task end')

if __name__ == "__main__":
    logger = Logger('MainLogger').setup_system_logger()
    logger.info('stocks price scheduler just restarted')
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_stocks_job, 'cron', hour='06', minute='30')
    scheduler.start()
    while True:
        schedule.run_pending()
        time.sleep(1)
