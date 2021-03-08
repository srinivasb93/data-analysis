import pandas as pd
import numpy as np
import datetime as dt
import yfinance as yf
import sqlalchemy as sa
import urllib.parse
import statsmodels.api as sm

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

#stock_list = ['MOTHERSUMI','BAJFINANCE','HDFCBANK','ASIANPAINT','RELAXO','SONATSOFTW','ICICIBANK','KOTAKBANK','RELIANCE','TITAN','HINDUNILVR','PIDILITIND','ITC','NESTLEIND','INFY','TCS']

stock_list = ['HDFCBANK']
for stock in stock_list:
    print(stock)
    query = "SELECT * FROM dbo." + stock + " WHERE DATE >='2014-01-01 00:00:00.000' ORDER BY DATE ASC "
    data = pd.read_sql(query, con=conn, parse_dates=True)

    data['Close_Chg'] = round(data['Close'].pct_change()*100,2)
    data['Range'] = round(data['High'] - data['Low'],2)
    data['HH'] = data['High'] - data['High'].shift()
    data['LL'] = data['Low'] - data['Low'].shift()
    data['High_10'] = data['High'].rolling(10).max()
    data['High_20'] = data['High'].rolling(20).max()
    data['Low_10'] = data['Low'].rolling(10).min()
    data['Low_20'] = data['Low'].rolling(20).min()
    data['ATR'] = atr(data[['High','Low','Close']],14)
    data['Range_ATR'] = round(data['Range']/data['ATR'],1)
    data['Range_Cls_Chg'] = round(data['Range']/data['Close_Chg'],1)
    data['Vol_Avg10'] = round(data['Volume'].rolling(10,min_periods=1).mean(),0)
    data['Vol_Avg20'] = round(data['Volume'].rolling(20,min_periods=1).mean(), 0)
    data['EMA_200'] = round(data['Close'].ewm(span=200).mean(), 2)
    data['EMA_20'] = round(data['Close'].ewm(span=20).mean(), 2)
    data['EMA_10'] = round(data['Close'].ewm(span=10).mean(), 2)

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
    curr_support=prev_support=curr_res =prev_res = 0.0
    data['Curr_Supp']=data['Prev_Supp']=data['Curr_Res']=data['Prev_Res']=0.0
    for rdata in data.itertuples():
        k = rdata.Index+2
        if k < len(data.index):

            data.loc[k,'EMA20_Sig'] = 'Close_GT_20EMA' if data.loc[k,'Cls_Abv_EMA20'] >=0 else 'Close_LT_20EMA'
            if data.loc[k,'Cls_Abv_EMA20'] >=0 and data.loc[k-1,'Cls_Abv_EMA20'] < 0:
                data.loc[k, 'EMA20_Sig'] = 'Cross_Abv_20EMA'
            elif data.loc[k,'Cls_Abv_EMA20'] < 0 and data.loc[k-1,'Cls_Abv_EMA20'] >= 0:
                data.loc[k, 'EMA20_Sig'] = 'Cross_Blw_20EMA'

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

            data.loc[k,'Vol10_Sig'] = 'Vol_GT_Avg10' if data.loc[k, 'Vol_Abv_Avg10'] > 1 else 'Vol_LT_Avg10'

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
            elif data.loc[k,'HH'] >=0 and data.loc[k,'LL'] < 0:
                data.loc[k,'High_Low'] = 'HH_LL'
            elif data.loc[k,'HH'] < 0 and data.loc[k,'LL'] < 0:
                data.loc[k,'High_Low'] = 'LH_LL'
            elif data.loc[k,'HH'] < 0 and data.loc[k,'LL'] >= 0:
                data.loc[k,'High_Low'] = 'LH_HL'

            supp_cond = data.loc[k,'Reg_5_Chg'] >= 0 and data.loc[k - 1,'Reg_5_Chg'] < 1 and data.loc[k - 2,'Reg_5_Chg'] < 1
            if supp_cond:
                prev_support = curr_support
                curr_support = data.loc[k - 1, 'Reg_5']
                data.loc[k, 'Prev_Supp'] = prev_support
                data.loc[k, 'Curr_Supp'] = curr_support
                if curr_support > prev_support and curr_res > prev_res and data.loc[k, 'Low'] > curr_support:
                    data.loc[k, 'Support'] = 'Price_Abv_Supp'
                elif curr_support > prev_support and curr_res < prev_res and data.loc[k, 'Low'] > curr_res:
                    data.loc[k, 'Support'] = 'Price_Abv_Cur_Res'
                elif curr_support < prev_support and curr_res < prev_res and (data.loc[k, 'Low'] > prev_support or data.loc[k, 'Low'] > curr_res):
                    data.loc[k, 'Support'] = 'Price_Abv_Prev_Sup'

            res_cond = data.loc[k, 'Reg_5_Chg'] < 0 and data.loc[k - 1, 'Reg_5_Chg'] >= 0 and data.loc[k - 2, 'Reg_5_Chg'] >= 0
            if res_cond:
                prev_res = curr_res
                curr_res = data.loc[k - 1, 'Reg_5']
                data.loc[k, 'Prev_Res'] = prev_res
                data.loc[k, 'Curr_Res'] = curr_res
                if curr_res < prev_res and curr_support < prev_support and  data.loc[k, 'High'] < curr_res:
                    data.loc[k, 'Resistance'] = 'Price_Blw_Res'
                elif curr_res < prev_res and curr_support < prev_support and  data.loc[k, 'High'] < curr_support:
                    data.loc[k, 'Resistance'] = 'Price_Blw_Cur_Sup'
                elif curr_support > prev_support and curr_res > prev_res and (data.loc[k, 'High'] < prev_support or data.loc[k, 'High'] < prev_support):
                    data.loc[k, 'Support'] = 'Price_Abv_Prev_Sup'

            if curr_support > prev_support and curr_res > prev_res and data.loc[k, 'Low'] < curr_support and data.loc[k, 'Close'] >= curr_support:
                data.loc[k, 'Support'] = 'Price_Crs_Abv_Supp'
            elif curr_support > prev_support and curr_res < prev_res and data.loc[k, 'Low'] < curr_res and data.loc[k, 'Close'] >= curr_res:
                data.loc[k, 'Support'] = 'Price_Crs_Abv_Cur_Res'
            elif curr_support < prev_support and curr_res < prev_res and (data.loc[k, 'Low'] < prev_support or data.loc[k, 'Low'] < curr_res) and \
                    (data.loc[k, 'Close'] >= curr_res or data.loc[k, 'Close'] >= prev_support):
                data.loc[k, 'Support'] = 'Price_Crs_Abv_Prev_Sup'

            if curr_res < prev_res and data.loc[k, 'High'] > curr_res and data.loc[k, 'Close'] < curr_res:
                data.loc[k, 'Resistance'] = 'Price_Crs_Blw_Res'
            elif curr_res > prev_res and data.loc[k, 'High'] > prev_res and data.loc[k, 'Close'] < prev_res:
                data.loc[k, 'Resistance'] = 'Price_Crs_Blw_Prev_Res'

            if data.loc[k-1,'Close'] < data.loc[k-1,'High_10'] and data.loc[k,'Close'] > data.loc[k-1,'High_10'] and data.loc[k,'Reg_5']> data.loc[k,'Reg_10'] :
                data.loc[k,'Brakeout_10'] = 'Breakout_10_Up'
            elif data.loc[k-1,'Close'] > data.loc[k-1,'Low_10'] and data.loc[k,'Close'] < data.loc[k-1,'Low_10'] and data.loc[k,'Reg_5']< data.loc[k,'Reg_10']:
                data.loc[k,'Brakeout_10'] = 'Breakout_10_Down'
            if data.loc[k-1,'Close'] < data.loc[k-1,'High_20'] and data.loc[k,'Close'] > data.loc[k-1,'High_20'] and data.loc[k,'Reg_5']> data.loc[k,'Reg_10']:
                data.loc[k,'Brakeout_20'] = 'Breakout_20_Up'
            elif data.loc[k-1,'Close'] > data.loc[k-1,'Low_20'] and data.loc[k,'Close'] < data.loc[k-1,'Low_20'] and data.loc[k,'Reg_5']< data.loc[k,'Reg_10']:
                data.loc[k,'Brakeout_20'] = 'Breakout_20_Down'

            res_break_chk = data.loc[k,'Low'] < curr_res and data.loc[k-1,'Close'] < curr_res and data.loc[k-2,'Close'] < curr_res and data.loc[k,'Close'] >= curr_res
            pres_break_chk = data.loc[k,'Low'] < prev_res and data.loc[k-1,'Close'] < prev_res and data.loc[k-2,'Close'] < prev_res and data.loc[k,'Close'] >= prev_res
            if res_break_chk:
                data.loc[k,'Break_Sup_Res'] = 'Cur_Res_Broken_Up'
            elif pres_break_chk:
                data.loc[k,'Break_Sup_Res'] = 'Prev_Res_Broken_Up'
            elif res_break_chk and pres_break_chk:
                data.loc[k, 'Break_Sup_Res'] = 'Both_Res_Broken_Up'

            sup_break_chk = data.loc[k,'High'] > curr_support and data.loc[k-1,'Close'] > curr_support and data.loc[k-2,'Close'] > curr_support and  data.loc[k,'Close'] < curr_support
            psup_break_chk = data.loc[k,'High'] > prev_support and data.loc[k-1,'Close'] > prev_support and data.loc[k-2,'Close'] > prev_support and  data.loc[k,'Close'] < prev_support
            if sup_break_chk:
                data.loc[k,'Break_Sup_Res'] = 'Cur_Sup_Broken_Down'
            elif psup_break_chk:
                data.loc[k,'Break_Sup_Res'] = 'Prev_Sup_Broken_Down'
            elif sup_break_chk and psup_break_chk:
                data.loc[k, 'Break_Sup_Res'] = 'Both_Sup_Broken_Down'

            if data.loc[k,'Curr_Res'] > data.loc[k,'Prev_Res']:
                diff_price = data.loc[k,'High_10'] - data.loc[k,'Low_20']
                fib382 = data.loc[k, 'Fib_382'] = data.loc[k,'High_10'] - diff_price*.382
                fib50 =  data.loc[k, 'Fib_50'] = data.loc[k,'High_10'] - diff_price*.5
                fib618 = data.loc[k, 'Fib_618'] = data.loc[k,'High_10'] - diff_price*.618

            if curr_res > prev_res:
                if data.loc[k,'Low'] < fib382 and data.loc[k,'Close'] > fib382:
                    data.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F382'
                if data.loc[k,'Low'] < fib50 and data.loc[k,'Close'] > fib50:
                    data.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F50'
                if data.loc[k,'Low'] < fib618 and data.loc[k,'Close'] > fib618:
                    data.loc[k,'Fib_Status'] = 'Cls_Crs_Abv_F618'


    data.to_csv('data_'+stock+'.csv',index=False)

