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

    wk_data = data.resample('W').agg(logic)
    wk_data.index = wk_data.index - pd.DateOffset(days=6)
    wk_data.reset_index(inplace=True)
    wk_data['Wk_Cls_Chg'] = round(wk_data['Close'].pct_change()*100,2)
    wk_data['Bi_Wk_Cls_Chg'] = round(wk_data['Close'].pct_change(periods=2)*100,2)
    wk_data['High_52_Wk'] = wk_data['High'].rolling(52).max()
    wk_data['Low_52_Wk'] = wk_data['Low'].rolling(52).min()
    wk_data['Wk_EMA_20'] = round(wk_data['Close'].ewm(span=20).mean(),2)
    wk_data['ATH'] = wk_data['High'].expanding().max()
    wk_data['ATL'] = wk_data['Low'].expanding().min()
    wk_data['Max_Wk_Chg'] = wk_data['Wk_Cls_Chg'].expanding().max()
    wk_data['Max_Wk_Vol'] = wk_data['Volume'].expanding().max()
    wk_data['Max_Wk_Range'] = (wk_data['High'] - wk_data['Low']).expanding().max()
    print("Start Data Load for the stock : {}".format(stock))
    # Write data read from .csv to SQL Server table
    wk_data.to_sql(name=stock+'_W', con=conn, if_exists='replace', index=False)
    # stock.to_sql(name=stock + '_W', con=conn, if_exists='append', index=False)
    print("Data Load done for the stock : {}".format(stock))

print('Weekly Data Load successful for all the stocks')
