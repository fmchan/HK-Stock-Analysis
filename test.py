import pandas as pd
import pandas_ta as ta
from dbhelper import DBHelper

db = DBHelper()
df = db.query_stock('YAHOO', 'HK', '0700.HK', letter_case=False)
df.ta.log_return(cumulative=True, append=True)
df.ta.percent_return(cumulative=True, append=True)
df.ta.sma(length=10, append=True)
df.ta.macd(fast=12, slow=26, signal=9, append=True)
print(df.tail(5))