from WindPy import *
print w.start()
from datetime import date
from datetime import timedelta
import pandas as pd
from pandas import Series, DataFrame
import numpy as np

#import pd.dateframe as df

# culist = w.wsd("CU1612.SHF", "close,volume", "2016-10-23", "2016-11-21", "TradingCalendar=SHFE")
# print culist
#
start_date = date(2015, 11,23)
end_date = date(2016, 5, 21)
#todo:get all data and deal with data with dataframe
#todo list to dataframe
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

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


for product in SHFE_EXCH:
    exch = ".SHF"
    result_sheet = []
    fmtstart_date = start_date.strftime("%Y-%m-%d")
    fmtend_date = end_date.strftime("%Y-%m-%d")
    result_data = []
    days_list = []
    for single_date in daterange(start_date, end_date):
        days_list.append(single_date.strftime("%Y-%m-%d"))
    priceframe = pd.DataFrame(index=days_list)
    for month_count in range(1, (end_date - start_date).days % 30 + 6):
        symbolname = product + (start_date + timedelta(days=month_count * 28)).strftime('%y%m') + exch
        result_data = w.wsd(symbolname, "close,volume", fmtstart_date, fmtend_date, "TradingCalendar=SHFE", "Days=AllDays")
        # clost_pirce = []
        # traded_volume = []
        # clost_pirce.append(result_data.Data[0])
        # traded_volume.append(result_data.Data[1])
        # for c, v in clost_pirce, traded_volume:
        #     if len(result_data.Data) == 1:
        #         print "it's not a trading day"
        #         continue

        #price_list = {symbolname: result_data.Data[0]}
        price_series = Series(result_data.Data[0], index=priceframe.index)
        #quantiy_list = {symbolname: result_data.Data[1]}
        # aframe = DataFrame(price_list)
        #df01 = pd.DataFrame(price_list, columns=[symbolname], index=days_list)
        priceframe[symbolname] = price_series
        #priceframe = DataFrame(price_list, columns=[symbolname], index=days_list)
        #quantiyframe = DataFrame(quantiy_list, columns=[symbolname], index=days_list)

    print "hold on"
    print "hello list"
