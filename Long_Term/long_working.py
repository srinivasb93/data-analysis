import pandas as pd
import numpy as np
from math import floor
import matplotlib.pyplot as plt
import sqlalchemy as sa
import datetime as dt
import pyodbc
import urllib
import os
import statsmodels.api as sm

# Working good gor HDFCBank, Infy, Reliance only ..Can be considered better
pd.options.mode.chained_assignment = None
"""
period: data period to download (Either Use period parameter or use start and end) Valid periods are: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
interval: data interval (intraday data cannot extend last 60 days) Valid intervals are: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
start: If not using period - Download start date string (YYYY-MM-DD) or datetime.
end: If not using period - Download end date string (YYYY-MM-DD) or datetime.
prepost: Include Pre and Post market data in results? (Default is False)
auto_adjust: Adjust all OHLC automatically? (Default is True)
actions: Download stock dividends and stock splits events? (Default is True)
"""

#Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=DESKTOP-MAK81E6\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "Trusted_Connection=yes")

'''
#Use this for SQL server authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=dagger;"
                                 "DATABASE=test;"
                                 "UID=user;"
                                 "PWD=password")
'''

#Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
conn=engine.connect()

#prices=pd.read_csv('Stock_Symbol1.csv')
#prices=pd.read_csv('Nifty_Midcap_50.csv')
#prices=pd.read_csv('Nifty_Next_50.csv')
#stocks = prices['Symbol'].tolist()
stocks = ['KOTAKBANK','HDFCBANK','ITC','RELIANCE','INFY','TATAMOTORS','HINDUNILVR','SONATSOFTW','TITAN','RELAXO','ASIANPAINT','NESTLEIND','BAJFINANCE','MOTHERSUMI','PIDILITIND','BRITANNIA','ICICIBANK']
#stocks = input("Enter the Stock symbol as in NSE : ")
# ndays = input("Enter the no. of days for Data Analysis : ")
fast = int(input("Enter the no.of days for Fast MA : "))
slow = int(input("Enter the no.of days for Slow MA : "))
df4 = pd.DataFrame()
df5 = pd.DataFrame()
df6 = pd.DataFrame()
start_date = dt.date(2020,1,1)
end_date = dt.date.today()- dt.timedelta(1)

def slope(ser, n):
    "function to calculate the slope of regression line for n consecutive points on a plot"
    #     ser = (ser - ser.min())/(ser.max() - ser.min())
    x = np.array(range(len(ser)))
    #     x = (x - x.min())/(x.max() - x.min())
    slopes = [i * 0 for i in range(n - 1)]
    reg_prices = [i * 0 for i in range(n - 1)]
    for i in range(n, len(ser) + 1):
        y_scaled = ser[i - n:i]
        x_scaled = x[i - n:i]
        x_scaled = sm.add_constant(x_scaled)
        model = sm.OLS(y_scaled, x_scaled)
        results = model.fit()
        results1 = model.predict(results.params)
        slopes.append(results.params[-1])
        reg_prices.append(results1[-1])
    slope_angle = (np.rad2deg(np.arctan(np.array(slopes))))
    return slope_angle, reg_prices

for stock in stocks:
    print(stock)
    if stock == 'BAJAJ-AUTO':
        stock = 'BAJAJAUTO'
    if stock == 'M&M':
        stock = 'MM'
    if stock == 'MCDOWELL-N':
        stock = 'MCDOWELL'
    if stock == 'L&TFH':
        stock = 'LTFH'
    if stock == 'M&MFIN':
        stock = 'MMFIN'
    query = "SELECT * FROM dbo." + stock + " WHERE DATE >='2014-01-01 00:00:00.000'"
    df = pd.read_sql(query, con=conn, parse_dates=True)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Close_change'] = df['Close'].pct_change()
    act_ndays = len(df.index)

    #high_52_week = df['High'].max()
    #low_52_week = df['Low'].min()
    #latest_close = df['Close'].tail(1).values[0]
    #chg_52_high = ((latest_close - high_52_week) / high_52_week) * 100
    #chg_52_low = ((latest_close - low_52_week) / low_52_week) * 100

    #dfmin = df['Close'].min()
    #dfmax = df['Close'].max()
    #dfmean = round(df['Close'].mean(), 1)
    # dfmean_fast = np.round(df['Close'].rolling(fast).mean(), 2)
    # dfmean_slow = np.round(df['Close'].rolling(slow).mean(), 2)
    dfmean_fast_C = np.round(df['Close'].ewm(span=fast).mean(), 2)
    dfmean_fast_H = np.round(df['High'].ewm(span=fast).mean(), 2)
    dfmean_fast_L = np.round(df['Low'].ewm(span=fast).mean(), 2)
    dfmean_slow_C = np.round(df['Close'].ewm(span=slow).mean(), 2)
    dfmean_slow_H = np.round(df['High'].ewm(span=slow).mean(), 2)
    dfmean_slow_L = np.round(df['Low'].ewm(span=slow).mean(), 2)
    EMA_50 = np.round(df['Close'].ewm(span=50).mean(), 2)
    EMA_11 = np.round(df['Close'].ewm(span=11).mean(), 2)
    EMA_20 = np.round(df['Close'].rolling(20).mean(), 2)
    BB_Std=df.loc[:,'Close'].rolling(20).std()
    BB2_Upper = EMA_20+(BB_Std*2)
    BB1_Upper = EMA_20+(BB_Std*1)
    BB2_Lower = EMA_20-(BB_Std*2)
    BB1_Lower = EMA_20-(BB_Std*1)
    # dfmean_low = np.round(df['Low'].ewm(span=fast).mean(), 2)
    df['Range'] = df['High'] - df['Low']
    df['Close_Low'] = df['Close'] - df['Low']
    df['High_Close'] = df['High'] - df['Close']
    df['Range_chg'] = df['Range'].pct_change()
    df['Avg_Range'] = round(df['Range'].rolling(10).mean(), 1)
    MACD_mean_fast = np.round(df['Close'].ewm(span=11).mean(), 2)
    MACD_mean_slow = np.round(df['Close'].ewm(span=26).mean(), 2)
    # MACD = dfmean_fast_C - dfmean_slow_C
    MACD = MACD_mean_fast - MACD_mean_slow
    MACD_Signal = round(MACD.ewm(span=9).mean(), 2)
    MACD_hist = MACD - MACD_Signal

    # Calculating Support zones
    s1_1 = df.loc[int(act_ndays*2/3):(act_ndays-10),'Low'].min()
    s1_2 = df.loc[int(act_ndays*2/3):(act_ndays-10),'Close'].min()
    r1_1 = df.loc[int(act_ndays*2/3):(act_ndays-10),'Close'].max()
    r1_2 = df.loc[int(act_ndays*2/3):(act_ndays-10),'High'].max()
    #r2_1 = df.loc[int(act_ndays/3):int(act_ndays*2/3),'Close'].max()
    #r2_2 = df.loc[int(act_ndays/3):int(act_ndays*2/3),'High'].max()
    #r3_1 = df.loc[0:int(act_ndays/3),'Close'].max()
    #r3_2 = df.loc[0:int(act_ndays/3),'High'].max()

    data1 = {
        'Symbol':stock,
        'Date': df['Date'],
        'Open': df['Open'],
        'High': df['High'],
        'Low': df['Low'],
        'Close': df['Close'],
        'Close_Chg': round(df['Close_change'] * 100, 1),
        'EMA_L_C': dfmean_fast_C - dfmean_fast_L,
        'EMA_H_C': dfmean_fast_H - dfmean_fast_C,
        'EMA_H_L': '',
        '3EMA_H_L': '',
        'Slow_EMA_L_C': dfmean_slow_C - dfmean_slow_L,
        'Slow_EMA_H_C': dfmean_slow_H - dfmean_slow_C,
        'Slow_EMA_H_L': '',
        'Slow_3EMA_H_L': '',
        'Range': df['Range'],
        'Close_Low':df['Close_Low'],
        'High_Close':df['High_Close'],
        'Range_Chg': round(df['Range_chg'] * 100, 1),
        'Avg_Range': df['Avg_Range'],
        'Slow_Mean_Chg': '',
        'EMA_fast': dfmean_fast_C,
        'EMA_slow': dfmean_slow_C,
        'EMA_50': EMA_50,
        'EMA_20':EMA_20,
        'BB_Std':BB_Std,
        'BB1_Upper':BB1_Upper,
        'BB2_Upper': BB2_Upper,
        'BB1_Lower': BB1_Lower,
        'BB2_Lower': BB2_Lower,
        # 'EMA_11_low': dfmean_low,
        'Volume': df['Volume'],
        'MACD': MACD,
        'MACD_Chg': '',
        'MACD_Signal': MACD_Signal,
        'MACD_Hist': MACD_hist,
        'EMA_Signal': '',
        'EMA_3_Signal': '',
        'EMA_HL_Signal':'',
        'Slow_EMA_Signal': '',
        'Slow_EMA_3_Signal': '',
        'Slow_EMA_HL_Signal':'',
        'EMA_Crossover': '',
        'MACD_Crossover': '',
        'My_Signal_1': '',
        'My_Signal_2': '',
        'Range_Signal': '',
        'My_Signal_4': '',
        'My_Signal_5':'',
        'S_R':'',
        'MACD_Hist_Signal':'',
        'High_Low':'',
        'H_L_20days':'',
        'BB_width':'',
        'BB_Signal':'',
        'Volume_S':'',
        'Range_S':'',
        'Open_Status':'',
        'MACD_Signal1':''
    }

    df3 = pd.DataFrame(data1)
    df3['MACD_Chg'] = round(df3['MACD'].pct_change() * 100, 1)
    # df3.loc[50:,'Slow_Mean_Chg']=round(((df3.loc[50:,'Close']-df3.loc[50:,'EMA_slow'])/df3.loc[50:,'EMA_slow'])*100,1)
    df3['Slow_Mean_Chg'] = round(((df3['Close'] - df3['EMA_slow']) / df3['EMA_slow']) * 100, 1)
    df3['Mean_50_Chg'] = round(((df3['Close'] - df3['EMA_50']) / df3['EMA_50']) * 100, 1)
    df3['Volume_10'] = round(df3['Volume'].rolling(10, min_periods=1).mean(), 1)
    df3['Volume_10_max'] = df3['Volume'].rolling(10, min_periods=1).max()
    df3['Volume_10_min'] = df3['Volume'].rolling(10, min_periods=1).min()

    df3['Range_10_max'] = df3['Range'].rolling(10, min_periods=1).max()
    df3['Range_10_min'] = df3['Range'].rolling(10, min_periods=1).min()
 
    df3['High_Chg'] = round(df3['High'].pct_change() * 100, 1)
    df3['Low_Chg'] = round(df3['Low'].pct_change() * 100, 1)
    df3['High_20day'] = df3['High'].rolling(20).max()
    df3['Low_20day'] = df3['Low'].rolling(20).min()

    len_di = len(df3.index)
    buy_qty = 1
    total_qty = 0
    buy_value = 0
    latest_value = 0
    buy_price = 0

    #print('Starting Portfolio Value : {}'.format(start_value))
    for row_index, row_data in df3.iterrows():
        k = row_index + 5

        if (k < len_di):
            #print('Date: {0[0]}, Open: {0[1]} ,High: {0[2]}, Low: {0[3]}, Close: {0[4]}'.format(df3.loc[k, 1:6].tolist()))
            #print('EMA_50: {0[0]}, EMA_11: {0[1]} ,MACD: {0[2]}, MACD_Signal: {0[3]}'.format(df3.loc[k, 11:15].tolist()))
            if df3.loc[k,'High_Chg'] >0 :
                base_price = df3.loc[k,'High']
                buy_price = base_price - .05*base_price

            if df3.loc[k,'Low'] <buy_price :             
                total_qty += buy_qty
                buy_value += buy_qty * buy_price
                buy_price = buy_price - .05*buy_price

                """
                print('BUY EXECUTED , Qty: %d, Total Buy Cost :%.2f, Available Cash: %.2f' % (buy_qty, buy_cost, available_cash))
                print('Date: {0[0]}, Open: {0[1]} ,High: {0[2]}, Low: {0[3]}, Close: {0[4]}'.format(df3.loc[k, 1:6].tolist()))
                print('Buy Price: {0:.2f}, Stop Price: {1:.2f}, Target Price: {2:.2f}'.format(buy_price, stop_price,target_price))
                """
            latest_value = total_qty*df3.loc[k, 'Close']
            #total_qty = total_qty if df3.loc[k,'Stock_Split']==0 else total_qty*df3.loc[k,'Stock_Split']
            df3.loc[k,'Total_Qty'] = total_qty 
            df3.loc[k,'Buy_Value'] = round(buy_value,2)
            df3.loc[k,'Latest_Value'] = round(latest_value,2)


            #df3.loc[k,'Year'] =
        # df4 = df3[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Close_Chg','Score','Avg_Range', 'MACD', 'MACD_Signal', 'MACD_Hist',
        #           'EMA_Crossover','EMA_Crossover1','EMA_Crossover2', 'Signal_50_EMA','EMA_fast','EMA_slow','EMA_50','EMA_20']]

    df3['Profit'] = df3['Latest_Value'] - df3['Buy_Value']
    df4 = df3[['Symbol', 'Total_Qty', 'Buy_Value', 'Latest_Value','Profit']].tail(1)
    df6 = df6.append(df4)

df6['Avg_Buy_Price'] = round(df6['Buy_Value']/df6['Total_Qty'],2)
df6['Profit_Perc'] = round((df6['Latest_Value'] - df6['Buy_Value'])/df6['Buy_Value']*100,2)
df6.sort_values(by='Profit_Perc',ascending=False).to_csv("Results_Long_Overall.csv",index=False)
