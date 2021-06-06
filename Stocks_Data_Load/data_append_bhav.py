import pandas as pd
import sqlalchemy as sa
import datetime as dt
import pyodbc
import urllib
import quandl
import yfinance as yf
from nsepy import get_history
import numpy as np

bhavdate = dt.date(2021,3,5)


# split_df = pd.read_csv(r"C:\Users\admin\PycharmProjects\My Projects\PyTrain\StockSplit_Data1.csv")
# prices=pd.read_csv('Stock_Symbol.csv')
#prices=pd.read_csv('Nifty_Midcap_50.csv')
#prices=pd.read_csv('Nifty_Next_50.csv')
# stocks = ['EICHERMOT']

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

query = "SELECT NAME FROM dbo.STOCKS"
stocks_data = conn.execute(query)
stocks = stocks_data.fetchall()


#stocks = ['MM']

bhav_query = "SELECT * FROM dbo.BHAVCOPY"
data = pd.read_sql_query(bhav_query, con=conn, parse_dates=True)

print(data.head())

df_today = data.loc[:, ['SYMBOL','TIMESTAMP','OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']]
print(df_today.head())
exit()
df_today.rename(columns={'TIMESTAMP':'Date','OPEN': 'Open', 'HIGH': 'High', 'LOW': 'Low', 'CLOSE': 'Close',
                                 'TOTTRDQTY': 'Volume'}, inplace=True)
df_today.set_index('SYMBOL',inplace=True)

for stock in stocks:
    stock = stock[0]
    try:
        query = "SELECT max(DATE) FROM dbo."+stock
        print("Extracting Data from SQL for the stock : {}".format(stock))
        startdate = conn.execute(query)
        start_dt = startdate.fetchall()
        start_dt = start_dt[0][0]
        start_dt = pd.to_datetime(start_dt).date()
        day_is = bhavdate.strftime('%A')
    except:
        print('No data for the stock {}' .format(stock))
        continue
    date_diff = bhavdate - start_dt

    if date_diff.days ==0:
        print('skipped load for stock {}'.format(stock))
        continue
    # elif date_diff.days >1 and day_is != 'Monday':
    #     proceed = input('Bhav load is pending for more than one day..Do you want to Proceed.? Y or N')
    #     if proceed == 'N':
    #         break

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

    data_to_add = df_today[df_today.index==stock]

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
    data_to_add.to_sql(name=stock,con=conn,if_exists='append',index=False)
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
