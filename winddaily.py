#coding=GB18030
################################################
### Author: patrick Yang
### Create Time: 2016/11/21
### function: 从wind获取数据期货的数据，设成交量最大的品种为活跃交易品种，并获取对应的价格作为代表价格，
###           生成价格序列，计算不同的品种的相关性
### input：    品种，开始日期，结束日期
### out：      价格序列的csv，相关性的matrix的csv

from WindPy import *
print w.start()
from datetime import date
from datetime import timedelta
import pandas as pd
from pandas import Series
import numpy as np

###input
###
EXCH = ["CU", "AL", "ZN", "PB", "NI", "SN", "AU", "AG", "RB"]
#DCE = ["M", "Y", "A", "P", "C", "CS", "JD", "L", "V", "PP", "J", "JM", "I"]
#ZCE = ["SR", "CF", "ZC", "FG", "TA", "MA", "O", "RM", "SF", "SM"]
start_date = date(2015, 11,23)
end_date = date(2016, 6, 21)


# #get all trading days
# #decide wheater it is a trading days
#
# #W9400055
#by exchange
#basic_metal = [CU, AL, ZN, PB, NI, SN]
#precious_metal = [AU, AG]


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def dropweekends(df):
    for dateindex, row in df.iterrows():
        if datetime.strptime(dateindex, '%Y-%m-%d').isoweekday() >= 6:
            df.drop(dateindex, inplace=True)

def get_col_name(df, row):
    b = (df.ix[row.name] == row['maxquantity'])
    print b
    print "type of b is " + str(type(b))
    print b.argmax()
    return b.argmax()
    # print b.index[b.argmax()]
    # return b.index[b.argmax()]


days_list = []
for single_date in daterange(start_date, end_date):
    days_list.append(single_date.strftime("%Y-%m-%d"))
productdataframe = pd.DataFrame(index=days_list)

for product in EXCH:
    exch = ".SHF"
    result_sheet = []
    fmtstart_date = start_date.strftime("%Y-%m-%d")
    fmtend_date = end_date.strftime("%Y-%m-%d")
    result_data = []

    pricequantityframe = pd.DataFrame(index=days_list)
    quantityframe = pd.DataFrame(index=days_list)
    for month_count in range(1, (end_date - start_date).days / 30 + 7):
        symbolname = product + (start_date + timedelta(days=month_count * 28)).strftime('%y%m') + exch
        result_data = w.wsd(symbolname, "close,volume", fmtstart_date, fmtend_date, "TradingCalendar=SHFE", "Days=AllDays")
        #drop days if it is holiday
        #need to maintain a chinese holiday table

        # clost_pirce = []
        # traded_volume = []
        # clost_pirce.append(result_data.Data[0])
        # traded_volume.append(result_data.Data[1])
        # for c, v in clost_pirce, traded_volume:
        #     if len(result_data.Data) == 1:
        #         print "it's not a trading day"
        #         continue
        price_series = Series(result_data.Data[0], index=pricequantityframe.index)
        quantity_series = Series(result_data.Data[1], index=pricequantityframe.index)
        pricequantityframe[symbolname + "_price"] = price_series
        pricequantityframe[symbolname + "_quantity"] = quantity_series
        quantityframe[symbolname] = quantity_series
    dropweekends(pricequantityframe)
    dropweekends(quantityframe)
    #pricequantityframe.to_csv(product + ".csv")
    tquantityframe = quantityframe.transpose()
    quantitymax = tquantityframe.max()

    pricequantityframe["maxquantity"] = Series(quantitymax, index=pricequantityframe.index)

    pricequantityframe = pricequantityframe[np.isfinite(pricequantityframe['maxquantity'])]
    pricequantityframe.to_csv(product + "strip.csv")
    qdf = pd.DataFrame(pricequantityframe["maxquantity"])
    pricequantityframe["theprice"] = np.nan
    thepricelist = []
    for r in range(len(pricequantityframe.index)):
        row = qdf.irow(r)
        print row.name
        thecolumn = get_col_name(quantityframe, row)
        # pricequantityframe("theprice", index=[row.name]) = pricequantityframe(thecolumn + "_price", index=[row.name])
        pricequantityframe["theprice"][row.name] = pricequantityframe[thecolumn + "_price"][row.name]
    # pricequantityframe.to_csv(product + "_theprice.csv")
        # row["maxquantity"]
    # qdf.apply(get_col_name, axis=1)
    # for row in qdf.iterrows():
    #     print row
    #     print row['maxquantity']
    #     # print row
    #     # row
    #     # if value == row["maxquantity"]:


    # pricequantityframe.ix[]
    # xxx = qdf.apply(get_col_name, axis = 1)
    # print xxx
    print "hold on"
    print "hello list"
    productdataframe[product] = Series(pricequantityframe["theprice"], index=productdataframe.index)
productdataframe.to_csv("all_product.csv")
cor = productdataframe.corr()
cor.to_csv("correlation.csv")