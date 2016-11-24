#coding=GB18030
################################################
### Author: patrick Yang
### Create Time: 2016/11/21
### function: 从wind获取数据期货的数据，设成交量最大的品种为活跃交易品种，并获取对应的价格作为代表价格，
###           生成价格序列，计算不同的品种的相关性
### input：    品种，开始日期，结束日期
### out：      价格序列的csv，相关性的matrix的csv
### other:     用cmd运行脚本或启动wind的时候需要管理员权限启动
### environment:  使用了 numpy，pandas与wind的插件，跑之前需要安装
### todo:      加强错误处理与提示部分或者把数据存入数据库
from WindPy import *
print w.start()
from datetime import date
from datetime import timedelta
import pandas as pd
from pandas import Series
import numpy as np

###input
###
#EXCH = ["AU"]
EXCH = ["CU", "AL", "ZN", "PB", "NI", "SN", "AU", "AG", "RB"]
#DCE = ["M", "Y", "A", "P", "C", "CS", "JD", "L", "V", "PP", "J", "JM", "I"]
#ZCE = ["SR", "CF", "ZC", "FG", "TA", "MA", "O", "RM", "SF", "SM"]
start_date = date(2016, 8,23)
end_date = date(2016, 12, 21)

def daterange(start_date, end_date):
    '''
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 日期形式
    '''
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def dropweekends(df):
    '''
    去除掉生成的datarame中包含周末数据的行
    :param df: 原始的dataframe
    :return: 修改后的dataframe
    '''
    for dateindex, row in df.iterrows():
        if datetime.strptime(dateindex, '%Y-%m-%d').isoweekday() >= 6:
            df.drop(dateindex, inplace=True)

def get_col_name(df, row):
    '''
    寻找当前列成交量做大的值所在对应的月份的column
    :param df:
    :param row:
    :return:
    '''
    b = (df.ix[row.name] == row['maxquantity'])
    # print b
    # print "type of b is " + str(type(b))
    # print b.argmax()
    return b.argmax()


days_list = []
for single_date in daterange(start_date, end_date):
    days_list.append(single_date.strftime("%Y-%m-%d"))
productdataframe = pd.DataFrame()
percentage_change_dataframe = pd.DataFrame()

for product in EXCH:
    print "start deal with product " + product
    exch = ".SHF"
    result_sheet = []
    fmtstart_date = start_date.strftime("%Y-%m-%d")
    fmtend_date = end_date.strftime("%Y-%m-%d")
    result_data = []
    pricequantityframe = pd.DataFrame(index=days_list)
    quantityframe = pd.DataFrame(index=days_list)
    #假设离当前月份的最活跃的月份是从今天往后数7个月内的某个合约
    for month_count in range(1, (end_date - start_date).days / 30 + 7):
        symbolname = product + (start_date + timedelta(days=month_count * 28)).strftime('%y%m') + exch
        result_data = w.wsd(symbolname, "close,volume", fmtstart_date, fmtend_date, "TradingCalendar=SHFE", "Days=AllDays")
        price_series = Series(result_data.Data[0], index=pricequantityframe.index)
        quantity_series = Series(result_data.Data[1], index=pricequantityframe.index)
        # if type(price_series[0]) == type(None):
        #当从wind取到的价格为None的时候drop掉
        if price_series[0] is None:
            continue
        pricequantityframe[symbolname + "_price"] = price_series
        # print price_series
        percentage_change_series = price_series.pct_change(1)
        pricequantityframe[symbolname + "_percentage_change"] = percentage_change_series
        pricequantityframe[symbolname + "_quantity"] = quantity_series
        quantityframe[symbolname] = quantity_series
    dropweekends(pricequantityframe)
    dropweekends(quantityframe)
    #分品种保存数据
    pricequantityframe.to_csv(product + ".csv")
    tquantityframe = quantityframe.transpose()
    quantitymax = tquantityframe.max()
    pricequantityframe["maxquantity"] = Series(quantitymax, index=pricequantityframe.index)
    pricequantityframe = pricequantityframe[np.isfinite(pricequantityframe['maxquantity'])]
    pricequantityframe.to_csv(product + "strip.csv")
    qdf = pd.DataFrame(pricequantityframe["maxquantity"])
    pricequantityframe["theprice"] = np.nan
    pricequantityframe["thepercentage_change"] = np.nan
    thepricelist = []
    for r in range(len(pricequantityframe.index)):
        row = qdf.irow(r)
        thecolumn = get_col_name(quantityframe, row)
        pricequantityframe["theprice"][row.name] = pricequantityframe[thecolumn + "_price"][row.name]
        pricequantityframe["thepercentage_change"][row.name] = pricequantityframe[thecolumn + "_percentage_change"][row.name]
    productdataframe[product] = Series(pricequantityframe["theprice"], index=pricequantityframe.index)
    percentage_change_dataframe[product + "_percentage_change"] = Series(pricequantityframe["thepercentage_change"], index=pricequantityframe.index)
    print "finish deal with product " + product + "\r\n"
#保存所有数据
productdataframe.to_csv("all_product.csv")
percentage_change_dataframe.to_csv("all_product_percentage_change.csv")
#计算相关性矩阵，todo：不过是否需要换算法
cor = percentage_change_dataframe.corr()
#保存相关性矩阵
cor.to_csv("correlation.csv")