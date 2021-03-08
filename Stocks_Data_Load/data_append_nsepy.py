import pandas as pd
import sqlalchemy as sa
import datetime as dt
import pyodbc
import urllib
import quandl
import yfinance as yf
from nsepy import get_history
import numpy as np

# startdate = dt.date(2010,1,1)
enddate = dt.date.today()

# split_df = pd.read_csv(r"C:\Users\admin\PycharmProjects\My Projects\PyTrain\StockSplit_Data1.csv")
# prices=pd.read_csv('Stock_Symbol.csv')
#prices=pd.read_csv('Nifty_Midcap_50.csv')
#prices=pd.read_csv('Nifty_Next_50.csv')
# stocks = ['EICHERMOT']

indices = ['NIFTY 50', 'NIFTY NEXT 50', 'NIFTY BANK', 'INDIA VIX', 'NIFTY 100', 'NIFTY 500', 'NIFTY MIDCAP 100',
           'NIFTY MIDCAP 50', 'NIFTY INFRA', 'NIFTY REALTY', 'NIFTY ENERGY', 'NIFTY FMCG', 'NIFTY MNC',
           'NIFTY PHARMA', 'NIFTY PSE', 'NIFTY PSU BANK', 'NIFTY SERV SECTOR', 'NIFTY IT', 'NIFTY SMLCAP 100',
           'NIFTY 200', 'NIFTY AUTO', 'NIFTY MEDIA', 'NIFTY METAL', 'NIFTY COMMODITIES', 'NIFTY CONSUMPTION',
           'NIFTY CPSE', 'NIFTY FIN SERVICE', 'NIFTY SMLCAP 50', 'NIFTY SMLCAP 250']

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

query = "SELECT NAME FROM SYS.TABLES"
stocks_data = conn.execute(query)
stocks = stocks_data.fetchall()

for stock in stocks:
    stock = stock[0]
    try:
        query = "SELECT max(DATE) FROM dbo."+stock
        print("Extracting Data from NSEPY for the stock : {}".format(stock))
        startdate = conn.execute(query)
        start_dt = startdate.fetchall()
        start_dt = start_dt[0][0] + dt.timedelta(1)
        start_dt = pd.to_datetime(start_dt).date()
    except:
        print('No data for the stock {}' .format(stock))
        continue
    # date_diff = enddate - start_dt
    #
    # if date_diff.days <=1 :
    #     print('skipped load for stock {}'.format(stock))
    #     continue

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
    if stock == 'FNO_LOT_SIZE':
        continue
    if " ".join(stock.split("_"))  in indices:
        data = get_history(symbol=" ".join(stock.split("_")), start=start_dt, end=enddate,index=True)
    else:
        data = get_history(symbol=stock, start=start_dt, end=enddate)
    data.reset_index(inplace=True)
    data['Date'] = pd.to_datetime(data['Date'], format='%Y-%m-%d')
    stock_data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()

    print("Start Data Load for the stock : {}".format(stock))
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
    #Write data read from .csv to SQL Server table
    stock_data.to_sql(name=stock,con=conn,if_exists='append',index=False)
    print("Data Load done for the stock : {}".format(stock))
#Read data from a SQL server table
# df1=pd.read_sql_table('STOCK_DATA',con=conn)
# print(df1.head())

'''
# Condition to not insert duplicate values into sql server table

for i in range(len(df)):
    try:
        df.iloc[i:i+1].to_sql(name="Table_Name",if_exists='append',con = Engine)
    except IntegrityError:
        pass #or any other action

'''
