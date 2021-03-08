'''
Created on May 20, 2020
Individual Stock Analysis where there is one entry and multiple exits for entire traded qty
@author: Srinivasulu_B
'''
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

pd.options.mode.chained_assignment = None
plt.style.use('ggplot')
start_time = dt.datetime.now()

prices = pd.read_csv('../Stock_Symbol.csv')
# prices = pd.read_csv('Stock_Symbol_Nxt50.csv')
# prices=pd.read_csv('Nifty_Midcap_50.csv')
# prices=pd.read_csv('Nifty_Next_50.csv')
stocks = prices['Symbol'].tolist()
#stocks = ['MOTHERSUMI','BAJFINANCE','HDFCBANK','ASIANPAINT','RELAXO','SONATSOFTW','ICICIBANK','KOTAKBANK','RELIANCE','TITAN','HINDUNILVR','PIDILITIND','ITC','NESTLEIND','INFY','TCS']
#stocks = ['MOTHERSUMI']
# stocks = input("Enter the Stock symbol as in NSE : ")
# ndays = input("Enter the no. of days for Data Analysis : ")
fast = int(input("Enter the no.of days for Fast MA : "))
slow = int(input("Enter the no.of days for Slow MA : "))
start_date = pd.Timestamp(dt.date(2014, 1, 1))
end_date = pd.Timestamp(dt.date.today() - dt.timedelta(1))

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

df4 = pd.DataFrame()
df5 = pd.DataFrame()
df6 = pd.DataFrame()
df7 = pd.DataFrame()
df8 = pd.DataFrame()

def wwma(values, n):
    return values.ewm(alpha=1 / n, adjust=False).mean()

def atr(df, n=14):
    data = df.copy()
    high = data['High']
    low = data['Low']
    close = data['Close']
    data['tr0'] = abs(high - low)
    data['tr1'] = abs(high - close.shift())
    data['tr2'] = abs(low - close.shift())
    tr = data[['tr0', 'tr1', 'tr2']].max(axis=1)
    atr = wwma(tr, n)
    # atr = tr.rolling(n).mean()
    return round(atr,2)

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

strategy = 'Reg_Latest_Long_Large'
os.mkdir(os.path.join(os.getcwd(), strategy))
new_path = os.path.join(os.getcwd(), strategy)

# query = "SELECT NAME FROM SYS.TABLES"
# stocks_data = conn.execute(query)
# stocks = stocks_data.fetchall()
# excp_list = ['BAJAJ-AUTO','STOCKSPLIT','NIFTY50','NIFTYNEXT50','NIFTYMID50']

for stock in stocks:
    st_time = dt.datetime.now()
    # stock = stock[0]
    # if stock in excp_list:
    #     continue
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
    df = pd.read_sql(query,con=conn,parse_dates=True)
    df['Date'] = pd.to_datetime(df['Date'])
    # df = df[(start_date <= df['Date']) & (df['Date']<= end_date)]
    df['Close_change'] = df['Close'].pct_change()
    act_ndays = len(df.index)

    """ Calculate fast and slow EMA """
    # dfmean_fast_C = np.round(df['Close'].ewm(span=fast).mean(), 2)
    # dfmean_fast_H = np.round(df['High'].ewm(span=fast).mean(), 2)
    # dfmean_fast_L = np.round(df['Low'].ewm(span=fast).mean(), 2)
    # dfmean_slow_C = np.round(df['Close'].ewm(span=slow).mean(), 2)
    # dfmean_slow_H = np.round(df['High'].ewm(span=slow).mean(), 2)
    # dfmean_slow_L = np.round(df['Low'].ewm(span=slow).mean(), 2)
    """ Calculate 11 and 50 EMA , 20 SMA"""
    # EMA_3 = np.round(df['Close'].ewm(span=3).mean(), 2)
    # EMA_5 = np.round(df['Close'].ewm(span=5).mean(), 2)
    # EMA_50 = np.round(df['Close'].ewm(span=50).mean(), 2)
    EMA_10 = np.round(df['Close'].ewm(span=10).mean(), 2)
    # SMA_20 = np.round(df['Close'].rolling(20).mean(), 2)
    EMA_20 = np.round(df['Close'].ewm(span=20).mean(), 2)
    EMA_200 = np.round(df['Close'].ewm(span=200).mean(), 2)
    EMA_100 = np.round(df['Close'].ewm(span=100).mean(), 2)
    EMA_50 = np.round(df['Close'].ewm(span=50).mean(), 2)
    """ Calculate Bollinger Bands for 20 day SMA """
    # BB_Std = df.loc[:, 'Close'].rolling(20).std()
    # BB2_Upper = SMA_20 + (BB_Std * 2)
    # BB1_Upper = SMA_20 + (BB_Std * 1)
    # BB2_Lower = SMA_20 - (BB_Std * 2)
    # BB1_Lower = SMA_20 - (BB_Std * 1)
    """Calculate Range & 10 days Average Range """
    df['Range'] = df['High'] - df['Low']
    # df['Range_chg'] = df['Range'].pct_change()
    df['Avg_Range'] = round(df['Range'].rolling(10).mean(), 1)
    df['Close_Low'] = df['Close'] - df['Low']
    df['High_Close'] = df['High'] - df['Close']
    """ Calculate MACD Standard 11,26,9 """
    # MACD_mean_fast = np.round(df['Close'].ewm(span=11).mean(), 2)
    # MACD_mean_slow = np.round(df['Close'].ewm(span=26).mean(), 2)
    # MACD = MACD_mean_fast - MACD_mean_slow
    # MACD_Signal = round(MACD.ewm(span=9).mean(), 2)
    # MACD_hist = MACD - MACD_Signal
    """Calculating Support and Resistance zones"""
    # s1_1 = df.loc[int(act_ndays * 2 / 3):(act_ndays - 10), 'Low'].min()
    # s1_2 = df.loc[int(act_ndays * 2 / 3):(act_ndays - 10), 'Close'].min()
    # r1_1 = df.loc[int(act_ndays * 2 / 3):(act_ndays - 10), 'Close'].max()
    # r1_2 = df.loc[int(act_ndays * 2 / 3):(act_ndays - 10), 'High'].max()

    """Creating a dictionary with the required data"""
    data1 = {
        'Symbol': stock,
        'Date': df['Date'],
        'Open': df['Open'],
        'High': df['High'],
        'Low': df['Low'],
        'Close': df['Close'],
        'Close_Chg': round(df['Close_change'] * 100, 1),
        # 'EMA_L_C': dfmean_fast_C - dfmean_fast_L,
        # 'EMA_H_C': dfmean_fast_H - dfmean_fast_C,
        # 'EMA_H_L': '',
        # '3EMA_H_L': '',
        # 'Slow_EMA_L_C': dfmean_slow_C - dfmean_slow_L,
        # 'Slow_EMA_H_C': dfmean_slow_H - dfmean_slow_C,
        # 'Slow_EMA_H_L': '',
        # 'Slow_3EMA_H_L': '',
        'Range': df['Range'],
        'Close_Low': df['Close_Low'],
        'High_Close': df['High_Close'],
        'Avg_Range': df['Avg_Range'],
        # 'Slow_Mean_Chg': '',
        # 'EMA_fast': dfmean_fast_C,
        # 'EMA_slow': dfmean_slow_C,
        # 'EMA_3': EMA_3,
        'EMA_50': EMA_50,
        'EMA_100': EMA_100,
        'EMA_200': EMA_200,
        'EMA_20': EMA_20,
        'EMA_10': EMA_10,
        # 'BB_Std': BB_Std,
        # 'BB1_Upper': BB1_Upper,
        # 'BB2_Upper': BB2_Upper,
        # 'BB1_Lower': BB1_Lower,
        # 'BB2_Lower': BB2_Lower,
        'Volume': df['Volume'],
        # 'BB_width': '',
        # 'MACD': MACD,
        # 'MACD_Signal': MACD_Signal,
        # 'MACD_Hist': MACD_hist,
        'Signal_B': 0,
        'Signal_SL': 0,
        'Signal_SP': 0,
        'PnL': 0,
        'Present_Value': 0
    }

    """Create a DataFrame with dictionary data"""

    df3 = pd.DataFrame(data1)

    # df3['MACD_Chg'] = round(df3['MACD'].pct_change() * 100, 1)
    # df3['MACD_Sig_Chg'] = round(df3['MACD_Signal'].pct_change() * 100, 1)
    # df3['Slow_Mean_Chg'] = round(df3['EMA_slow'].pct_change() * 100, 1)
    # df3['Fast_Mean_Chg'] = round(df3['EMA_fast'].pct_change() * 100, 1)
    # df3['Mean_50_Chg'] = round(df3['EMA_50'].pct_change() * 100, 1)
    df3['Volume_10'] = round(df3['Volume'].rolling(10, min_periods=1).mean(), 1)
    df3['Volume_10_max'] = df3['Volume'].rolling(10, min_periods=1).max()
    df3['Volume_10_min'] = df3['Volume'].rolling(10, min_periods=1).min()
    df3['Range_10_max'] = df3['Range'].rolling(10, min_periods=1).max()
    df3['Range_10_min'] = df3['Range'].rolling(10, min_periods=1).min()

    df3['HH'] = df3['High'] - df3['High'].shift()
    df3['LL'] = df3['Low'] - df3['Low'].shift()
    df3['High_10'] = df3['High'].rolling(10).max()
    df3['High_20'] = df3['High'].rolling(20).max()
    df3['Low_10'] = df3['Low'].rolling(10).min()
    df3['Low_20'] = df3['Low'].rolling(20).min()
    df3['ATR'] = atr(df3[['High', 'Low', 'Close']], 14)
    df3['Range_ATR'] = round(df3['Range'] / df3['ATR'], 1)
    #     df3['Slow_Mean_Chg'] = pd.to_numeric(df3['Slow_Mean_Chg'])
    # df3['EMA_H_L'] = df3['EMA_H_C'] - df3['EMA_L_C']
    # df3['3EMA_H_L'] = round(df3['EMA_H_L'].ewm(span=3).mean(), 2)
    # df3['Slow_EMA_H_L'] = df3['Slow_EMA_H_C'] - df3['Slow_EMA_L_C']
    # df3['Slow_3EMA_H_L'] = round(df3['Slow_EMA_H_L'].ewm(span=3).mean(), 2)

    """ High/Low Change to identify HH,HL,LH,LL and 20 day High/Low calculation """
    df3['High_Chg'] = round(df3['High'].pct_change() * 100, 1)
    df3['Low_Chg'] = round(df3['Low'].pct_change() * 100, 1)
    df3['High_20day'] = df3['High'].rolling(20).max()
    df3['Low_20day'] = df3['Low'].rolling(20).min()
    df3['C_High_20day'] = df3['Close'].rolling(20).max()
    df3['C_Low_20day'] = df3['Close'].rolling(20).min()
    # df3[['ATR_UP', 'ATR_Down', 'ATR_TSL']] = atr(df3[['High', 'Low', 'Close']])
    df3['Slope_5'], df3['Reg_5'] = slope(df3['Close'], 5)
    df3['Slope_10'], df3['Reg_10'] = slope(df3['Close'], 10)
    df3['Reg_5_Chg'] = df3['Reg_5'] - df3['Reg_5'].shift()
    df3['Reg_Cross'] = df3['Reg_5'] - df3['Reg_10']

    df3['Diff_EM10_20'] = df3['EMA_10'] - df3['EMA_20']
    df3['Diff_Reg5_EM20'] = df3['Reg_5'] - df3['EMA_20']
    df3['EM_Diff_By_ATR'] = round(df3['Diff_EM10_20'] / df3['ATR'], 2)
    df3['Close_EM10'] = df3['Close'] - df3['EMA_10']
    df3['Close_EM20'] = df3['Close'] - df3['EMA_20']

    df3['Cls_Abv_EMA20'] = df3['Close'] - df3['EMA_20']
    df3['Cls_Abv_EMA10'] = df3['Close'] - df3['EMA_10']
    df3['EM_Diff_Status'] =''
    curr_support = prev_support = curr_res = prev_res = 0.0
    df3['Support']=df3['Resistance']= df3['Curr_Supp'] = df3['Prev_Supp'] = df3['Curr_Res'] = df3['Prev_Res'] = df3['Trail_Price'] = 0.0

    """ Bollinger Band width calculation """
    # df3['BB1_width'] = df3['BB1_Upper'] - df3['BB1_Lower']
    # df3['BB2_width'] = df3['BB2_Upper'] - df3['BB2_Lower']
    # df3['BB1_30days'] = df3['BB1_width'].rolling(30).min()
    # df3['BB2_30days'] = df3['BB2_width'].rolling(30).min()

    """ Variables required to track the cash flow of the strategy """
    len_di = len(df3.index)
    cnt_trades = 0
    profit_trades = 0
    loss_trades = 0
    position = False
    start_value = 100000
    available_cash = 0
    latest_value = 0
    risk = .02
    trail = .02
    #     print('Starting Portfolio Value : {}'.format(start_value))
    for row_index, row_data in df3.iterrows():
        k = row_index + 5
        if (k < len_di):
            df3.loc[k, 'Signal_SL'] = np.NaN
            df3.loc[k, 'Signal_SP'] = np.NaN
            df3.loc[k, 'Signal_B'] = np.NaN

            df3.loc[k, 'EMA20_Sig'] = 'Close_GT_20EMA' if df3.loc[k, 'Cls_Abv_EMA20'] >= 0 else 'Close_LT_20EMA'
            if df3.loc[k, 'Cls_Abv_EMA20'] >= 0 and df3.loc[k - 1, 'Cls_Abv_EMA20'] < 0:
                df3.loc[k, 'EMA20_Sig'] = 'Cross_Abv_20EMA'
            elif df3.loc[k, 'Cls_Abv_EMA20'] < 0 and df3.loc[k - 1, 'Cls_Abv_EMA20'] >= 0:
                df3.loc[k, 'EMA20_Sig'] = 'Cross_Blw_20EMA'

            df3.loc[k, 'EMA10_Sig'] = 'Close_GT_10EMA' if df3.loc[k, 'Cls_Abv_EMA10'] >= 0 else 'Close_LT_10EMA'
            if df3.loc[k, 'Cls_Abv_EMA10'] >= 0 and df3.loc[k - 1, 'Cls_Abv_EMA10'] < 0:
                df3.loc[k, 'EMA10_Sig'] = 'Cross_Abv_10EMA'
            elif df3.loc[k, 'Cls_Abv_EMA10'] < 0 and df3.loc[k - 1, 'Cls_Abv_EMA10'] >= 0:
                df3.loc[k, 'EMA10_Sig'] = 'Cross_Blw_10EMA'

            if df3.loc[k,'Reg_Cross'] >=0 and df3.loc[k-1,'Reg_Cross'] <0:
                df3.loc[k,'Reg_Cross_Sig'] = 'Cross_Up'
            elif df3.loc[k,'Reg_Cross'] <0 and df3.loc[k-1,'Reg_Cross'] >=0:
                df3.loc[k,'Reg_Cross_Sig'] = 'Cross_Down'
            elif df3.loc[k,'Reg_Cross'] >=0 and df3.loc[k-1,'Reg_Cross'] >=0:
                df3.loc[k, 'Reg_Cross_Sig'] = 'Reg5_Abv_Reg10'
            elif df3.loc[k,'Reg_Cross'] < 0 and df3.loc[k-1,'Reg_Cross'] <0:
                df3.loc[k, 'Reg_Cross_Sig'] = 'Reg5_Blw_Reg10'

            supp_cond = df3.loc[k,'Reg_5_Chg'] >= 0 and df3.loc[k-1,'Reg_5_Chg']<1 and df3.loc[k-2,'Reg_5_Chg']<1
            if supp_cond:
                prev_support = curr_support
                curr_support = df3.loc[k-1,'Reg_5']
                df3.loc[k,'Prev_Supp'] = prev_support
                df3.loc[k,'Curr_Supp'] = curr_support
                if curr_support > prev_support and  curr_res > prev_res and df3.loc[k,'Low'] > curr_support:
                    df3.loc[k,'Support'] = 'Price_Abv_Supp'
                elif curr_support > prev_support and curr_res < prev_res and df3.loc[k, 'Low'] > curr_res:
                    df3.loc[k, 'Support'] = 'Price_Abv_Cur_Res'
                elif curr_support < prev_support and curr_res < prev_res and (df3.loc[k, 'Low'] > prev_support or df3.loc[k, 'Low'] > curr_res) :
                    df3.loc[k, 'Support'] = 'Price_Abv_Prev_Sup'

            res_cond = df3.loc[k,'Reg_5_Chg'] < 0 and df3.loc[k-1,'Reg_5_Chg']>= 0 and df3.loc[k-2,'Reg_5_Chg']>= 0
            if res_cond:
                prev_res = curr_res
                curr_res = df3.loc[k-1,'Reg_5']
                df3.loc[k,'Prev_Res'] = prev_res
                df3.loc[k,'Curr_Res'] = curr_res
                if curr_res < prev_res and df3.loc[k, 'High'] < curr_res:
                    df3.loc[k,'Resistance'] = 'Price_Blw_Res'

            if curr_support > prev_support and  curr_res > prev_res and  df3.loc[k,'Low'] < curr_support and df3.loc[k,'Close'] >= curr_support:
                df3.loc[k,'Support'] = 'Price_Crs_Abv_Supp'
            elif curr_support > prev_support and  curr_res < prev_res and  df3.loc[k,'Low'] < curr_res and df3.loc[k,'Close'] >= curr_res:
                df3.loc[k,'Support'] = 'Price_Crs_Abv_Cur_Res'
            elif curr_support < prev_support and curr_res < prev_res and  (df3.loc[k,'Low'] < prev_support or df3.loc[k,'Low'] < curr_res) and (df3.loc[k,'Close'] >= curr_res or df3.loc[k,'Close'] >= prev_support ):
                df3.loc[k,'Support'] = 'Price_Crs_Abv_Prev_Sup'

            if curr_res < prev_res and  df3.loc[k,'High'] > curr_res and df3.loc[k,'Close'] < curr_res:
                df3.loc[k,'Resistance'] = 'Price_Crs_Blw_Res'
            elif curr_res > prev_res and  df3.loc[k,'High'] > prev_res and df3.loc[k, 'Close'] < prev_res:
                df3.loc[k, 'Resistance'] = 'Price_Crs_Blw_Prev_Res'

            if df3.loc[k,'Curr_Res'] > df3.loc[k,'Prev_Res']:
                diff_price = df3.loc[k,'High_10'] - df3.loc[k,'Low_20']
                fib382 = df3.loc[k, 'Fib_382'] = df3.loc[k,'High_10'] - diff_price*.382
                fib50 =  df3.loc[k, 'Fib_50'] = df3.loc[k,'High_10'] - diff_price*.5
                fib618 = df3.loc[k, 'Fib_618'] = df3.loc[k,'High_10'] - diff_price*.618

            if curr_res > prev_res:
                if df3.loc[k,'Low'] < fib382 and df3.loc[k,'Close'] > fib382:
                    df3.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F382'
                if df3.loc[k,'Low'] < fib50 and df3.loc[k,'Close'] > fib50:
                    df3.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F50'
                if df3.loc[k,'Low'] < fib618 and df3.loc[k,'Close'] > fib618:
                    df3.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F618'

            if df3.loc[k,'EM_Diff_By_ATR'] >= df3.loc[k-1,'EM_Diff_By_ATR'] and df3.loc[k-1,'EM_Diff_By_ATR'] >= df3.loc[k-2,'EM_Diff_By_ATR'] \
                and df3.loc[k-2,'EM_Diff_By_ATR'] < df3.loc[k-3,'EM_Diff_By_ATR'] and -1 <= df3.loc[k,'EM_Diff_By_ATR'] <= 1 :
                df3.loc[k,'EM_Diff_Status'] = 'EM_Diff_Signal'

            if not position:

                EMA_20_signal = df3.loc[k,'EMA20_Sig'] in ['Close_GT_20EMA','Cross_Abv_20EMA']
                EMA_10_signal = df3.loc[k,'EMA10_Sig'] in ['Close_GT_10EMA','Cross_Abv_10EMA']
                # Cls_Abv_EMA = df3.loc[k,'Close'] >= df3.loc[k,'EMA_50']
                Reg_5_cond = df3.loc[k,'Reg_Cross_Sig'] in ['Cross_Up','Reg5_Abv_Reg10']
                Reg_5_chg_cond = df3.loc[k,'Reg_5_Chg'] >0
                supp_cond_list = ['Price_Abv_Supp','Price_Abv_Cur_Res','Price_Abv_Prev_Sup','Price_Crs_Abv_Supp','Price_Crs_Abv_Prev_Sup','Price_Crs_Abv_Cur_Res']
                supp_cond_pr = df3.loc[k,'Support'] in supp_cond_list
                supp_cond_prv1 = df3.loc[k-1, 'Support'] in supp_cond_list
                supp_cond_prv2 = df3.loc[k-2, 'Support'] in supp_cond_list

                # if EMA_10_signal and EMA_20_signal and (supp_cond_pr or supp_cond_prv1) and Reg_5_chg_cond:


                if  df3.loc[k,'Diff_Reg5_EM20'] >=0 and df3.loc[k-1,'Diff_Reg5_EM20'] < 0:
                    """ Calculate the buy qty for available cash if strategy condition is satisfied """
                    buy_qty = floor(start_value / df3.loc[k, 'Close'])
                    """ Calculate the parameters if a Buy is executed """
                    cnt_trades += 1
                    buy_price = df3.loc[k, 'Close']

                    buy_cost = buy_qty * df3.loc[k, 'Close']
                    position = True
                    df3.loc[k, 'Signal_B'] = buy_price
                    print('Buy Date: {0[0]}, Open: {0[1]} ,High: {0[2]}, Low: {0[3]}, Close: {0[4]}'.format(df3.iloc[k, 1:6].tolist()))
            #                     print('BUY EXECUTED , Qty: %d, Total Buy Cost :%.2f, Available Cash: %.2f' % (buy_qty, buy_cost, available_cash))
            #                     print('Buy Price: {0:.2f}, Stop Price: {1:.2f}'.format(buy_price, stop_price))
            #                     print('=====================================================================')
            else:

                """ Sell below the STOP LOSS in case if GAP DOWN in Open Price """
                if df3.loc[k,'Diff_Reg5_EM20'] <=0 and df3.loc[k-1,'Diff_Reg5_EM20'] > 0:

                    sell_price = df3.loc[k, 'Close']
                    pnl = (sell_price - buy_price)*buy_qty
                    print('Sell Date: {0[0]}, Open: {0[1]} ,High: {0[2]}, Low: {0[3]}, Close: {0[4]}'.format(df3.iloc[k, 1:6].tolist()))
                    start_value = start_value + pnl  # Brokerage Cost is considered too
                    if pnl >0 :
                        profit_trades +=1
                    else:
                        loss_trades += 1
                    position = False
                    df3.loc[k, 'Signal_SL'] = sell_price
                    df3.loc[k, 'PnL'] = pnl
                    df3.loc[k, 'Present_Value'] = start_value

        #                         print('Trail Set to {0}'.format(trail_price))
        df3.loc[k, 'Trade_Count'] = cnt_trades
        df3.loc[k, 'Profit_Trades'] = profit_trades
        df3.loc[k, 'Loss_Trades'] = loss_trades
        df3.loc[k, 'Portfolio_Value'] = round(start_value, 2)

    df7 = df3[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_Chg', 'Reg_5', 'Reg_10', 'EMA_20','EM_Diff_By_ATR','Curr_Supp','Prev_Supp','Curr_Res','Prev_Res','Signal_B',
               'Signal_SL','Trail_Price','Signal_SP', 'PnL', 'Present_Value','Support','EMA20_Sig','Reg_Cross_Sig','Fib_Status']]

    df7.to_csv(os.path.join(new_path, "Results_" + stock + ".csv"), index=False)
    print("Time taken for {0} : {1}" .format(stock,dt.datetime.now()-st_time))

    df8 = df3[['Symbol', 'Trade_Count', 'Profit_Trades', 'Loss_Trades', 'Portfolio_Value']].tail(1)
    df8.loc[:,'Symbol'] = stock
    no_of_trades = df8['Trade_Count'].values[0] if df8['Trade_Count'].values[0] > 0 else 1000
    df8['Win_Loss_Perc'] = round(df8['Profit_Trades'].values[0] / no_of_trades, 2) * 100
    df8['P/L_Perc'] = round((df8['Portfolio_Value'].values[0] - 100000) / 100000, 2) * 100
    df6 = df6.append(df8)

df6.sort_values(by='Symbol').to_csv(os.path.join(new_path, "Results_Overall_" + strategy + ".csv"), index=False)
print('Total Time Taken : {} ' .format(dt.datetime.now() - start_time))