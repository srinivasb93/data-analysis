import pandas as pd
import sqlalchemy as sa
import urllib.parse

#Connect to SQL SERVER
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=DESKTOP-MAK81E6\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "Trusted_Connection=yes")
dbEngine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
conn = dbEngine.connect()

#stock_list = ['TATAMOTORS','MARUTI','ZEEL','BRITANNIA','SBIN','BAJAJFINSV','BAJFINANCE','HDFCBANK','ASIANPAINT','ICICIBANK','KOTAKBANK','RELIANCE','TITAN','HINDUNILVR','INDUSINDBK','ITC','NESTLEIND','INFY','TCS']
#stock_list = ['TATAMOTORS']

query = "SELECT NAME FROM dbo.STOCKS UNION SELECT NAME FROM dbo.STOCK_INDICES UNION SELECT NAME FROM dbo.STOCK_SECTORS"
stocks_data = conn.execute(query)
stocks = stocks_data.fetchall()

def find_candle(df):
    for rdata in df.itertuples():
        k = rdata.Index + 1
        if k < len(df.index):
            upp_shadow = (df.loc[k,'High'] - df.loc[k,'Close']) if df.loc[k,'Close'] > df.loc[k,'Open'] else (df.loc[k,'High'] - df.loc[k,'Open'])
            low_shadow = (df.loc[k,'Open'] - df.loc[k,'Low']) if df.loc[k,'Close'] > df.loc[k,'Open'] else (df.loc[k,'Close'] - df.loc[k,'Low'])
            mid_body = (df.loc[k,'Close'] - df.loc[k,'Open']) if df.loc[k,'Close'] > df.loc[k,'Open'] else (df.loc[k,'Open'] - df.loc[k,'Close'])

            df.loc[k, 'Upp_Shd'] = upp_shadow
            df.loc[k, 'Low_Shd'] = low_shadow
            if low_shadow !=0 and upp_shadow!=0 and (low_shadow // upp_shadow) >= 3 :
                df.loc[k, 'Candle'] = 'Bullish Hammer'
            if low_shadow !=0 and upp_shadow!=0 and (upp_shadow // low_shadow) >= 3 :
                df.loc[k, 'Candle'] = 'Inverted Hammer'


# stocks = ['GRASIM','MANAPPURAM','MM','NATIONALUM','RAMCOCEM','SHREECEM','TATACONSUM','TECHM','TORNTPHARM']

for stock in stocks:
    stock = stock[0]
    print(stock)
    # query = "SELECT * FROM dbo." + stock + " WHERE DATE >='2020-10-20 00:00:00.000' AND DATE <'2021-01-05 00:00:00.000' ORDER BY DATE ASC "
    # query = "SELECT * FROM dbo." + stock + " WHERE DATE >='2020-01-25 00:00:00.000' ORDER BY DATE ASC "
    query = "SELECT * FROM dbo." + stock + " ORDER BY DATE ASC "
    data = pd.read_sql_query(query, con=conn, parse_dates=True)
    #data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%m/%d/%Y')
    data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date',inplace=True)
    data.sort_index(inplace=True)

    logic = {"Open":'first',
             "High":'max',
             "Low":'min',
             'Close':'last',
             'Volume':'sum'
             }

    wk_data = data.resample('M').agg(logic)
    wk_data.reset_index(inplace=True)
    wk_data['Mth_Cls_Chg'] = round(wk_data['Close'].pct_change()*100,2)
    wk_data['Mth_Range'] = wk_data['High'] - wk_data['Low']
    wk_data['Mth_EMA_20'] = round(wk_data['Close'].ewm(span=20).mean(), 2)
    find_candle(wk_data)
    wk_data['Bi_Mth_Cls_Chg'] = round(wk_data['Close'].pct_change(periods=2)*100,2)
    wk_data['High_12_Mth'] = wk_data['High'].rolling(12).max()
    wk_data['Low_12_Mth'] = wk_data['Low'].rolling(12).min()
    wk_data['Max_Mth_Chg'] = wk_data['Mth_Cls_Chg'].expanding().max()
    wk_data['Max_Mth_Vol'] = wk_data['Volume'].expanding().max()
    wk_data['Max_Mth_Range'] = (wk_data['High'] - wk_data['Low']).expanding().max()

    print("Start Data Load for the stock : {}".format(stock))
    # Write data read from .csv to SQL Server table
    wk_data.to_sql(name=stock+'_M', con=conn, if_exists='replace', index=False)
    # stock.to_sql(name=stock + '_W', con=conn, if_exists='append', index=False)
    print("Data Load done for the stock : {}".format(stock))

print('Monthly Data Load successful for all the stocks')
