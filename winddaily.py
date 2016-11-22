from WindPy import *
print w.start()
from datetime import date
from datetime import timedelta
import
import pandas as pd
#import pd.dateframe as df

# culist = w.wsd("CU1612.SHF", "close,volume", "2016-10-23", "2016-11-21", "TradingCalendar=SHFE")
# print culist
#
start_date = date(2016, 11,21)
end_date = date(2016, 11, 21)

# #get all trading days
# #decide wheater it is a trading days
#
# #W9400055
#by exchange
#basic_metal = [CU, AL, ZN, PB, NI, SN]
#precious_metal = [AU, AG]
SHFE_EXCH = ["CU", "AL", "ZN", "PB", "NI", "SN", "AU", "AG", "RB"]
#DCE = ["M", "Y", "A", "P", "C", "CS", "JD", "L", "V", "PP", "J", "JM", "I"]
#ZCE = ["SR", "CF", "ZC", "FG", "TA", "MA", "O", "RM", "SF", "SM"]
for product in SHFE_EXCH:
    exch = ".SHF"
    for i in range((end_date - start_date).days + 1):
        date_counter = start_date + timedelta(days = i)

        for month_count in range(6):
            symbolname = product + (date_counter + timedelta(days = month_count * 28)).strftime('%y%m') + ".SHF"
            fmtdate = date_counter.strftime("%Y-%m-%d")
            list = w.wsd(symbolname, "close,volume", fmtdate, fmtdate, "TradingCalendar=SHFE")
            if len(list.Data) == 1:
                print "it's not a trading day"
                continue
            print list.Data
            print symbolname