#coding=GB18030
################################################
### Author: patrick Yang
### Create Time: 2016/11/21
### function: ��wind��ȡ�����ڻ������ݣ���ɽ�������Ʒ��Ϊ��Ծ����Ʒ�֣�����ȡ��Ӧ�ļ۸���Ϊ����۸�
###           ���ɼ۸����У����㲻ͬ��Ʒ�ֵ������
### input��    Ʒ�֣���ʼ���ڣ���������
### out��      �۸����е�csv������Ե�matrix��csv
### other:     ��cmd���нű�������wind��ʱ����Ҫ����ԱȨ������
### environment:  ʹ���� numpy��pandas��wind�Ĳ������֮ǰ��Ҫ��װ
### todo:      ��ǿ����������ʾ���ֻ��߰����ݴ������ݿ�
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
    :param start_date: ��ʼ����
    :param end_date: ��������
    :return: ������ʽ
    '''
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def dropweekends(df):
    '''
    ȥ�������ɵ�datarame�а�����ĩ���ݵ���
    :param df: ԭʼ��dataframe
    :return: �޸ĺ��dataframe
    '''
    for dateindex, row in df.iterrows():
        if datetime.strptime(dateindex, '%Y-%m-%d').isoweekday() >= 6:
            df.drop(dateindex, inplace=True)

def get_col_name(df, row):
    '''
    Ѱ�ҵ�ǰ�гɽ��������ֵ���ڶ�Ӧ���·ݵ�column
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
    #�����뵱ǰ�·ݵ����Ծ���·��Ǵӽ���������7�����ڵ�ĳ����Լ
    for month_count in range(1, (end_date - start_date).days / 30 + 7):
        symbolname = product + (start_date + timedelta(days=month_count * 28)).strftime('%y%m') + exch
        result_data = w.wsd(symbolname, "close,volume", fmtstart_date, fmtend_date, "TradingCalendar=SHFE", "Days=AllDays")
        price_series = Series(result_data.Data[0], index=pricequantityframe.index)
        quantity_series = Series(result_data.Data[1], index=pricequantityframe.index)
        # if type(price_series[0]) == type(None):
        #����windȡ���ļ۸�ΪNone��ʱ��drop��
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
    #��Ʒ�ֱ�������
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
#������������
productdataframe.to_csv("all_product.csv")
percentage_change_dataframe.to_csv("all_product_percentage_change.csv")
#��������Ծ���todo�������Ƿ���Ҫ���㷨
cor = percentage_change_dataframe.corr()
#��������Ծ���
cor.to_csv("correlation.csv")