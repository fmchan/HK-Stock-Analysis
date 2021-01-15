"""The file contains name settings."""
# from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.options import Options
import platform

# output settings
ALLOW_OVERWRITE = False # set False to read the data file instead of re-scraping

MIN_TEXT_NUM = 100
SCROLL_NUM = 20

# time setting
SECOND_TIME_OUT = 20
SCROLL_PAUSE_TIME = 1
SCRAP_PAUSE_TIME = 5

# TA settings
CURRENT_VOLUME_FILTER = 1_000_000

# flask config
HOST = "192.168.232.96"
PORT = 5000

# web driver config
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--start-maximized")
# options.add_argument("window-size=2560,1440")
# prefs = {"profile.default_content_setting_values.notifications" : 2}
# prefs = {"profile.managed_default_content_settings.images" : 2} # disable loading image
# options.add_experimental_option("prefs", prefs) # for chrome only
options.add_experimental_option("prefs", {
    "profile.default_content_setting_values.notifications" : 2,
    "profile.managed_default_content_settings.images" : 2
})

# config by platform
if platform.system() in ["Darwin", "Windows"]:
    LOG_PATH = "D://temp//Stock-Patterns-Master//log/"
    DATA_PATH = "data/"
    WEBDRIVER_PATH = "exe/chromedriver.exe"
    DB_PATH = "stocks.db"
    DB_ENGINE = "sqlite:///stocks.db"
else:
    LOG_PATH = "/var/local/apps/Stock-Patterns-Master/log/"
    DATA_PATH = "/usr/local/apps/Stock-Patterns-Master/data/"
    WEBDRIVER_PATH = "/usr/local/bin/chromedriver"
    DB_PATH = "/root/stocks.db"
    DB_ENGINE = "sqlite:////root/stocks.db"
