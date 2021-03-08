import pandas as pd
import numpy as np
import datetime as dt
import yfinance as yf
import sqlalchemy as sa
import urllib.parse
import statsmodels.api as sm
import math
import os
from nsetools import Nse

dt_today = dt.date.today().strftime('%d%m%Y')

df5 = pd.DataFrame()
df6 = pd.DataFrame()
nse=Nse()
#Connect to SQL SERVER
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=DESKTOP-MAK81E6\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "Trusted_Connection=yes")
dbEngine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
conn = dbEngine.connect()

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

prices=pd.read_csv('../Stock_Symbol.csv')
stock_idx = 'NIFTY50'
stock_list = prices['Symbol'].tolist()

# prices=pd.read_csv('Stock_Symbol_Nxt50.csv')
# stock_idx = 'NEXT50'
# stock_list = prices['Symbol'].tolist()

# prices=pd.read_csv('Stock_Symbol_Mid50.csv')
# stock_idx = 'MID50'
# stock_list = prices['Symbol'].tolist()

#stock_list = ['TATAMOTORS','MARUTI','ZEEL','BRITANNIA','SBIN','BAJAJFINSV','BAJFINANCE','HDFCBANK','ASIANPAINT','ICICIBANK','KOTAKBANK','RELIANCE','TITAN','HINDUNILVR','INDUSINDBK','ITC','NESTLEIND','INFY','TCS']
# stock_idx = 'MYSTOCKS'

# query = "SELECT NAME FROM SYS.TABLES"
# stocks_data = conn.execute(query)
# stock_list = stocks_data.fetchall()
# excp_list = ['BAJAJ-AUTO','STOCKSPLIT','NIFTY50','NIFTYNEXT50','NIFTYMID50']
missing_stocks = []
# stock_list = ['INFY']
for stock in stock_list:
    # stock = stock[0]
    # if stock in excp_list:
    #     continue
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
    print(stock)
    # query = "SELECT * FROM dbo." + stock + " WHERE DATE >='2020-10-20 00:00:00.000' AND DATE <'2021-01-05 00:00:00.000' ORDER BY DATE ASC "
    query = "SELECT * FROM dbo." + stock + " WHERE DATE >='2020-11-25 00:00:00.000' ORDER BY DATE ASC "
    data = pd.read_sql_query(query, con=conn, parse_dates=True)
    try:
        if stock == 'BAJAJAUTO':
            stock = 'BAJAJ-AUTO'
        if stock == 'MM':
            stock = 'M&M'
        if stock == 'MCDOWELL':
            stock = 'MCDOWELL-N'
        if stock == 'LTFH':
            stock = 'L&TFH'
        if stock == 'MMFIN':
            stock = 'M&MFIN'
        quote1 = nse.get_quote(stock, as_json=False)
        df_latest = pd.DataFrame([quote1])
        df_today = df_latest.loc[:, ['open', 'dayHigh', 'dayLow', 'lastPrice', 'totalTradedVolume']]
        df_today['Date'] = dt.date.today()
        df_today.rename(columns={'open': 'Open', 'dayHigh': 'High', 'dayLow': 'Low', 'lastPrice': 'Close',
                     'totalTradedVolume': 'Volume'}, inplace=True)
        data = data.append(df_today, ignore_index=True, sort=True)
    except:
        missing_stocks.append(stock)
        continue
    data['Symbol'] = stock
    data = data[['Symbol','Date','Open','High','Low','Close','Volume']]
    data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%m/%d/%Y')
    data['Close_Chg'] = round(data['Close'].pct_change()*100,2)
    data['Range'] = round(data['High'] - data['Low'],2)
    data['HH'] = data['High'] - data['High'].shift()
    data['LL'] = data['Low'] - data['Low'].shift()
    data['High_10'] = data['High'].rolling(10,min_periods=10).max()
    data['High_20'] = data['High'].rolling(20,min_periods=20).max()
    data['Low_10'] = data['Low'].rolling(10,min_periods=10).min()
    data['Low_20'] = data['Low'].rolling(20,min_periods=20).min()
    data['ATR'] = atr(data[['High','Low','Close']],14)
    data['Range_ATR'] = round(data['Range']/data['ATR'],1)
    data['Range_Cls_Chg'] = round(data['Range']/data['Close_Chg'],1)
    data['Vol_Avg10'] = round(data['Volume'].rolling(10,min_periods=10).mean(),0)
    data['Vol_Avg20'] = round(data['Volume'].rolling(20,min_periods=20).mean(), 0)
    data['EMA_200'] = round(data['Close'].ewm(span=200).mean(), 2)
    data['EMA_20'] = round(data['Close'].ewm(span=20,min_periods=20).mean(), 2)
    data['EMA_20_Chg'] = data['EMA_20'].pct_change(periods=10)
    data['EMA_10'] = round(data['Close'].ewm(span=10,min_periods=10).mean(), 2)

    dx_10 = np.diff(range(0, len(data.index)))
    dy_10 = np.diff(data['EMA_10'])
    slope_em10 = dy_10 / dx_10
    slope_rads_em10 = (np.rad2deg(np.arctan(slope_em10)))
    data.loc[1:, 'Slope_EM10'] = slope_rads_em10

    dx_20 = np.diff(range(0, len(data.index)))
    dy_20 = np.diff(data['EMA_20'])
    slope_em20 = dy_20 / dx_20
    slope_rads_em20 = (np.rad2deg(np.arctan(slope_em20)))
    data.loc[1:, 'Slope_EM20'] = slope_rads_em20

    data['Reg_5_Slope'],data['Reg_5'] = slope(data['Close'],5)
    data['Reg_10_Slope'],data['Reg_10'] = slope(data['Close'], 10)
    data['Diff_EM10_20'] = data['EMA_10'] - data['EMA_20']
    data['EM_Diff_By_ATR'] = round(data['Diff_EM10_20']/data['ATR'],2)
    data['Close_EM10'] = data['Close'] - data['EMA_10']
    data['Close_EM20'] = data['Close'] - data['EMA_20']
    data['Reg_5_Chg'] = data['Reg_5'] - data['Reg_5'].shift()
    data['Reg_Cross'] = data['Reg_5'] - data['Reg_10']
    data['Vol_Abv_Avg10'] = round(data['Volume']/ data['Vol_Avg10'],2)
    # data['Vol_Abv_Avg20'] = round(data['Volume']/data['Vol_Avg20'],2)
    data['Cls_Abv_EMA20'] = data['Close'] - data['EMA_20']
    data['Cls_Abv_EMA10'] = data['Close'] - data['EMA_10']
    data['Cls_Abv_Reg5'] = data['Close'] - data['Reg_5']
    ema_cnt_chk= curr_support=prev_support=curr_res =prev_res = fib382 = fib50 = fib618=0.0
    data['Curr_Supp']=data['Prev_Supp']=data['Curr_Res']=data['Prev_Res']=0.0

    data['Fib_Status'] = ''
    for rdata in data.itertuples():
        k = rdata.Index+2
        poc_bl = poc_br = 0
        if k < len(data.index):

            data.loc[k,'EMA20_Sig'] = 'Close_GT_20EMA' if data.loc[k,'Cls_Abv_EMA20'] >=0 else 'Close_LT_20EMA'
            if data.loc[k,'Cls_Abv_EMA20'] >=0 and data.loc[k-1,'Cls_Abv_EMA20'] < 0:
                data.loc[k, 'EMA20_Sig'] = 'Cross_Abv_20EMA'
            elif data.loc[k,'Cls_Abv_EMA20'] < 0 and data.loc[k-1,'Cls_Abv_EMA20'] >= 0:
                data.loc[k, 'EMA20_Sig'] = 'Cross_Blw_20EMA'

            if data.loc[k,'EMA20_Sig'] in ['Cross_Abv_20EMA','Close_GT_20EMA']:
                ema_cnt_chk +=1
                poc_bl += 1
                data.loc[k,'EMA20_Cnt'] = ema_cnt_chk
            if data.loc[k,'EMA20_Sig'] in ['Cross_Blw_20EMA','Close_LT_20EMA']:
                ema_cnt_chk =0
                poc_br += 1
                data.loc[k, 'EMA20_Cnt'] = ema_cnt_chk

            data.loc[k, 'EMA10_Sig'] = 'Close_GT_10EMA' if data.loc[k, 'Cls_Abv_EMA10'] >= 0 else 'Close_LT_10EMA'
            if data.loc[k, 'Cls_Abv_EMA10'] >= 0 and data.loc[k - 1, 'Cls_Abv_EMA10'] < 0:
                data.loc[k, 'EMA10_Sig'] = 'Cross_Abv_10EMA'
            elif data.loc[k, 'Cls_Abv_EMA10'] < 0 and data.loc[k - 1, 'Cls_Abv_EMA10'] >= 0:
                data.loc[k, 'EMA10_Sig'] = 'Cross_Blw_10EMA'

            data.loc[k,'Reg5_Sig'] = 'Close_GT_Reg5' if data.loc[k, 'Cls_Abv_Reg5'] >= 0 else 'Close_LT_Reg5'
            if data.loc[k,'Low'] <data.loc[k,'Reg_5'] and data.loc[k,'Close'] >= data.loc[k,'Reg_5']:
                data.loc[k, 'Reg5_Sig'] = 'Close_CrossUp_Reg5'
            elif data.loc[k,'High'] > data.loc[k,'Reg_5'] and data.loc[k,'Close'] <= data.loc[k,'Reg_5']:
                data.loc[k, 'Reg5_Sig'] = 'Close_CrossDn_Reg5'

            if 1 <= data.loc[k, 'Vol_Abv_Avg10'] <=1.5 :
                data.loc[k,'Vol10_Sig'] = 'Above Avg Volume'
            elif data.loc[k, 'Vol_Abv_Avg10'] >1.5:
                data.loc[k, 'Vol10_Sig'] = 'Super Volume'
            else:
                data.loc[k, 'Vol10_Sig'] = 'Normal Volume'

            if data.loc[k,'Reg_Cross'] >=0 and data.loc[k-1,'Reg_Cross'] <0:
                data.loc[k,'Reg_Cross_Sig'] = 'Cross_Up'
            elif data.loc[k,'Reg_Cross'] <0 and data.loc[k-1,'Reg_Cross'] >=0:
                data.loc[k,'Reg_Cross_Sig'] = 'Cross_Down'
            elif data.loc[k,'Reg_Cross'] >=0 and data.loc[k-1,'Reg_Cross'] >=0:
                data.loc[k, 'Reg_Cross_Sig'] = 'Reg5_Abv_Reg10'
            elif data.loc[k,'Reg_Cross'] < 0 and data.loc[k-1,'Reg_Cross'] <0:
                data.loc[k, 'Reg_Cross_Sig'] = 'Reg5_Blw_Reg10'

            if data.loc[k,'HH'] >=0 and data.loc[k,'LL'] >=0:
                data.loc[k,'High_Low'] = 'HH_HL'
                poc_bl += 1
            elif data.loc[k,'HH'] >=0 and data.loc[k,'LL'] < 0:
                data.loc[k,'High_Low'] = 'HH_LL'
                if data.loc[k,'Close'] > data.loc[k-1,'Close']:
                    poc_bl += 1
                elif data.loc[k,'Close'] < data.loc[k-1,'Close']:
                    poc_br += 1
            elif data.loc[k,'HH'] < 0 and data.loc[k,'LL'] < 0:
                data.loc[k,'High_Low'] = 'LH_LL'
                poc_br += 1
            elif data.loc[k,'HH'] < 0 and data.loc[k,'LL'] >= 0:
                data.loc[k,'High_Low'] = 'LH_HL'
                poc_bl += .5

            supp_cond = data.loc[k,'Reg_5_Chg'] > 0 and data.loc[k - 1,'Reg_5_Chg'] <= 0 and data.loc[k - 2,'Reg_5_Chg'] <= 0
            if supp_cond:
                prev_support = curr_support
                curr_support = data.loc[k - 1, 'Reg_5']
                data.loc[k, 'Prev_Supp'] = prev_support
                data.loc[k, 'Curr_Supp'] = curr_support
                # Condition for recent HH and HL
                if curr_support > prev_support and curr_res > prev_res and data.loc[k, 'Low'] > curr_support:
                    data.loc[k, 'Support'] = 'Price_Abv_Supp'
                    poc_bl += 1
                # Condition for recent LH and HL where cadle is above current LH
                elif curr_support > prev_support and curr_res < prev_res and data.loc[k, 'Low'] > curr_res:
                    data.loc[k, 'Support'] = 'Price_Abv_Cur_Res'
                    poc_bl += 1
                elif curr_support < prev_support and curr_res < prev_res and (data.loc[k, 'Low'] > prev_support or data.loc[k, 'Low'] > curr_res) and data.loc[k-1,'Close'] < prev_support:
                    data.loc[k, 'Support'] = 'Price_Abv_Prev_Sup'
                    poc_bl += .5

            res_cond = data.loc[k, 'Reg_5_Chg'] < 0 and data.loc[k - 1, 'Reg_5_Chg'] >= 0 and data.loc[k - 2, 'Reg_5_Chg'] >= 0
            if res_cond:
                prev_res = curr_res
                curr_res = data.loc[k - 1, 'Reg_5']
                data.loc[k, 'Prev_Res'] = prev_res
                data.loc[k, 'Curr_Res'] = curr_res
                if curr_res < prev_res and curr_support < prev_support and  data.loc[k, 'High'] < curr_res and data.loc[k-1,'Close'] > curr_res:
                    data.loc[k, 'Resistance'] = 'Price_Blw_Res'
                    poc_br += 1
                elif curr_res < prev_res and curr_support < prev_support and  data.loc[k, 'High'] < curr_support and  data.loc[k-1,'Close'] >= curr_support:
                    data.loc[k, 'Resistance'] = 'Price_Blw_Cur_Sup'
                    poc_br += 1

            if curr_support < prev_support and curr_res < prev_res and (data.loc[k, 'Low'] < prev_support or data.loc[k, 'Low'] < curr_res) and \
                    (data.loc[k, 'Close'] >= curr_res or data.loc[k, 'Close'] >= prev_support) and data.loc[k-1, 'Close'] < prev_support:
                data.loc[k, 'Support'] = 'Price_Crs_Abv_Prev_Sup'
                poc_bl += .5
            elif curr_support > prev_support and curr_res > prev_res and data.loc[k, 'Low'] < curr_support and data.loc[k, 'Close'] >= curr_support:
                data.loc[k, 'Support'] = 'Price_Crs_Abv_Supp'
                poc_bl += 1
            elif curr_support > prev_support and curr_res < prev_res and data.loc[k, 'Low'] < curr_res and data.loc[k,'Close'] >= curr_res and data.loc[k-1,'Close'] < curr_res:
                data.loc[k, 'Support'] = 'Price_Crs_Abv_Cur_Res'
                poc_bl += 1

            if curr_res < prev_res and data.loc[k, 'High'] > curr_res and data.loc[k, 'Close'] < curr_res and data.loc[k-1, 'Close'] > curr_res:
                data.loc[k, 'Resistance'] = 'Price_Crs_Blw_Res'
                poc_br += 1
            elif curr_res > prev_res and data.loc[k, 'High'] > prev_res and data.loc[k, 'Close'] < prev_res and data.loc[k-1, 'Close'] > prev_res:
                data.loc[k, 'Resistance'] = 'Price_Crs_Blw_Prev_Res'
                poc_br += .5

            if data.loc[k-1,'Close'] < data.loc[k-1,'High_10'] and data.loc[k,'Close'] > data.loc[k-1,'High_10'] and data.loc[k,'Reg_5']> data.loc[k,'Reg_10'] :
                data.loc[k,'Brakeout_10'] = 'Breakout_10_Up'
                poc_bl += 1
            elif data.loc[k-1,'Close'] > data.loc[k-1,'Low_10'] and data.loc[k,'Close'] < data.loc[k-1,'Low_10'] and data.loc[k,'Reg_5']< data.loc[k,'Reg_10']:
                data.loc[k,'Brakeout_10'] = 'Breakout_10_Down'
                poc_br += 1
            if data.loc[k-1,'Close'] < data.loc[k-1,'High_20'] and data.loc[k,'Close'] > data.loc[k-1,'High_20'] and data.loc[k,'Reg_5']> data.loc[k,'Reg_10']:
                data.loc[k,'Brakeout_20'] = 'Breakout_20_Up'
                poc_bl += 1
            elif data.loc[k-1,'Close'] > data.loc[k-1,'Low_20'] and data.loc[k,'Close'] < data.loc[k-1,'Low_20'] and data.loc[k,'Reg_5']< data.loc[k,'Reg_10']:
                data.loc[k,'Brakeout_20'] = 'Breakout_20_Down'
                poc_br += 1

            res_break_chk = data.loc[k,'Low'] < curr_res and data.loc[k-1,'Close'] < curr_res and data.loc[k-2,'Close'] < curr_res and data.loc[k,'Close'] >= curr_res
            pres_break_chk = data.loc[k,'Low'] < prev_res and data.loc[k-1,'Close'] < prev_res and data.loc[k-2,'Close'] < prev_res and data.loc[k,'Close'] >= prev_res
            if res_break_chk:
                data.loc[k,'Break_Sup_Res'] = 'Cur_Res_Broken_Up'
                poc_bl += 1
            elif pres_break_chk:
                data.loc[k,'Break_Sup_Res'] = 'Prev_Res_Broken_Up'
                poc_bl += 1
            elif res_break_chk and pres_break_chk:
                data.loc[k, 'Break_Sup_Res'] = 'Both_Res_Broken_Up'
                poc_bl += 1

            sup_break_chk = data.loc[k,'High'] > curr_support and data.loc[k-1,'Close'] > curr_support and data.loc[k-2,'Close'] > curr_support and  data.loc[k,'Close'] < curr_support
            psup_break_chk = data.loc[k,'High'] > prev_support and data.loc[k-1,'Close'] > prev_support and data.loc[k-2,'Close'] > prev_support and  data.loc[k,'Close'] < prev_support
            if sup_break_chk:
                data.loc[k,'Break_Sup_Res'] = 'Cur_Sup_Broken_Down'
                poc_br += 1
            elif psup_break_chk:
                data.loc[k,'Break_Sup_Res'] = 'Prev_Sup_Broken_Down'
                poc_br += 1
            elif sup_break_chk and psup_break_chk:
                data.loc[k, 'Break_Sup_Res'] = 'Both_Sup_Broken_Down'
                poc_br += 1

            if curr_res > prev_res and curr_support>prev_support:
                diff_price = data.loc[k,'High_10'] - data.loc[k,'Low_20']
                fib382 = data.loc[k, 'Fib_382'] = data.loc[k,'High_10'] - diff_price*.382
                fib50 =  data.loc[k, 'Fib_50'] = data.loc[k,'High_10'] - diff_price*.5
                fib618 = data.loc[k, 'Fib_618'] = data.loc[k,'High_10'] - diff_price*.618

            if curr_res > prev_res:
                if data.loc[k,'Low'] < fib382 and data.loc[k,'Close'] > fib382:
                    data.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F382'
                    poc_bl +=1
                if data.loc[k,'Low'] < fib50 and data.loc[k,'Close'] > fib50:
                    data.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F50'
                    poc_bl += 1
                if data.loc[k,'Low'] < fib618 and data.loc[k,'Close'] > fib618:
                    data.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F618'
                    poc_bl += 1
            data.loc[k, 'Bull_Signal'] = poc_bl
            data.loc[k, 'Bear_Signal'] = poc_br

    # data.to_csv('data_'+stock+'.csv',index=False)

    out_cols = ['Symbol','Date','Open','High','Low','Close','Close_Chg','ATR','Vol10_Sig','Range_ATR','Fib_Status','EMA20_Sig','EMA20_Cnt','High_Low','Support','Break_Sup_Res','Brakeout_10','Resistance',
                'Brakeout_20','Bull_Signal','Bear_Signal']
    df5 = df5.append(data.tail(1))
    df5 = df5[out_cols]
    df6 = df6.append(data.tail(50))
    df6 = df6[out_cols]
    # data.to_csv(stock + "_" + str(dt_today) + ".csv", index=False)
    # data[out_cols].to_csv(stock + "_" + str(dt_today) + ".csv", index=False)

df5.to_csv("Results_" + stock_idx+"_"+ str(dt_today) + ".csv", index=False)
df6.to_csv("Results50_" + stock_idx+"_"+ str(dt_today) + ".csv", index=False)

ema_20_abv = ['Close_GT_20EMA','Cross_Abv_20EMA']
ema_20_blw = ['Close_LT_20EMA','Cross_Blw_20EMA']

print("List of Bullish Stocks")
print(df5[(df5['Bull_Signal']>2) & (df5['Bear_Signal']<=1)& (df5['EMA20_Sig'].isin(ema_20_abv))])
print("========================")
print("List of Bearish Stocks")
print(df5[(df5['Bull_Signal']<=1) & (df5['Bear_Signal']>2) & (df5['EMA20_Sig'].isin(ema_20_blw))])
print("========================")

# df4.to_csv("Results_Ind_Stock.csv",index=False)
# engine = create_engine("mssql+pyodbc://scott:tiger@myhost:port/TRAINING?driver=SQL+Server+Native+Client+10.0")
# df5.to_sql('STOCK_DATA',engine)
print('Report is generated successfully')

# df5.reset_index(inplace=True)

fib_stocks = []
ema_20_stocks = []
supp_crs_stocks = []
reg_crs_stocks = []

fib_stocks = df5['Symbol'][df5['Fib_Status'] =='Cls_Crs_Abv_F50'].to_list()


ema_20_stocks = df5['Symbol'][df5['EMA20_Sig'].isin(ema_20_abv)].to_list()
# ema_10_chk = ['Close_GT_10EMA','Cross_Abv_10EMA']
# ema_10_stocks = df5['Symbol'][df5['EMA10_Sig'].isin(ema_10_chk)].to_list()
vol_check_list = df5['Symbol'][df5['Vol10_Sig'] =='Vol_GT_Avg10'].to_list()
supp_cond_list = ['Price_Abv_Supp','Price_Abv_Cur_Res','Price_Abv_Prev_Sup','Price_Crs_Abv_Supp','Price_Crs_Abv_Prev_Sup','Price_Crs_Abv_Cur_Res']
supp_crs_stocks = df5['Symbol'][df5['Support'].isin(supp_cond_list)].to_list()

print('Stocks which are at Support Levels')
print(supp_crs_stocks)
print('************************************')
print('Stocks above 20 day EMA')
print(ema_20_stocks)
print('************************************')
print('Stocks with greater Volume')
print(vol_check_list)
print('************************************')
print('Stocks at Important Fibanocci levels')
print(fib_stocks)
print('************************************')
print("Stocks with missing data")
print(missing_stocks)
#
# strgy_df = pd.DataFrame(stock_list, columns=['Symbol', 'Strategy'])
#
# df5['Close'] = round(df5['Close'], 2)
# df5['Qty'] = 50000 / df5['Close']
# df5['Qty'] = df5['Qty'].apply(lambda x: math.floor(x))
# df5['Status'] = ''
# df5['Buy Price'] = 0
# df5['Sell Price'] = 0
# df5['PnL'] = df5['Qty'] * (df5['Sell Price'] - df5['Buy Price'])
# df5['SL1'] = round(df5['Close'].apply(lambda x: (x - .02 * x)), 2)
# df5['SL2'] = round(df5['Close'].apply(lambda x: (x - .01 * x)), 2)
# df5['Tgt_1'] = round(df5['Close'].apply(lambda x: (x + .01 * x)), 2)
# df5['Tgt_2'] = round(df5['Close'].apply(lambda x: (x + .02 * x)), 2)
# df5['Tgt_3'] = round(df5['Close'].apply(lambda x: (x + .03 * x)), 2)
# df5['Tgt_3.5'] = round(df5['Close'].apply(lambda x: (x + .035 * x)), 2)
# df5['Tgt_4'] = round(df5['Close'].apply(lambda x: (x + .04 * x)), 2)
# df5['Risk'] = round(df5['Qty'] * (df5['Close'] - df5['SL1']), 2)
# df5['Result'] = ''
#
# result = pd.merge(df5, strgy_df, on='Symbol')
#
# result_disp_cols = ['Date', 'Symbol', 'Strategy', 'Qty', 'Close', 'SL1', 'Tgt_4', 'Long_Price']
# result_cols = ['Date', 'Symbol', 'Strategy', 'Status', 'Qty', 'Close', 'Buy Price', 'SL1', 'SL2', 'Tgt_1', 'Tgt_2',
#                'Tgt_3', 'Tgt_3.5', 'Tgt_4', 'Sell Price', 'PnL', 'Risk', 'Result']
#
# result_display = result.loc[:, result_disp_cols]
#
# result_store = result.loc[:, result_cols]
# result_store = result_store.append(tradebook, ignore_index=True, sort=False)
# result_store.drop_duplicates(subset=['Date', 'Symbol', 'Strategy'], keep='last', inplace=True)
# result_store.to_csv('Tradebook' + dt_today + '.csv', index=False)
#
# print(result_display[['Date', 'Symbol', 'Strategy', 'Qty', 'Close', 'SL1', 'Tgt_4']] \
#           [(result_display['Strategy'] != 'Long_Entry') | (result_display['Strategy'] != 'Long_Exit')])
#
# print(result_display[['Date', 'Symbol', 'Strategy', 'Long_Price']][
#           (result_display['Strategy'] == 'Long_Entry') | (result_display['Strategy'] == 'Long_Exit')])
# # print(result_store)

