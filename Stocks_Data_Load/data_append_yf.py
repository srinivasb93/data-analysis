import pandas as pd
import sqlalchemy as sa
import datetime as dt
import pyodbc
import urllib
import quandl
import yfinance as yf



# startdate = dt.date(2010,1,1)
enddate = dt.date.today()+ dt.timedelta(1)

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

query = "SELECT NAME FROM SYS.TABLES"
stocks_data = conn.execute(query)
stocks = stocks_data.fetchall()

for stock in stocks:
    stock = stock[0]

    try:
        query = "SELECT max(DATE) FROM dbo." + stock
        print("Extracting Data from YAHOO for the stock : {}".format(stock))
        startdate = conn.execute(query)
        start_dt = startdate.fetchall()
        start_dt = start_dt[0][0] + dt.timedelta(1)
        start_dt = pd.to_datetime(start_dt).date()
    except:
        print('No data for the stock {}'.format(stock))
        continue
    date_diff = enddate - start_dt

    if date_diff.days <= 2:
        print('skipped load for stock {}'.format(stock))
        continue

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

    get_stocks = yf.Ticker(stock+'.NS')
    df_yf = get_stocks.history(start=start_dt, end=enddate, interval="1d")
    df_yf.reset_index(inplace=True)
    df_yf['Date'] = pd.to_datetime(df_yf['Date'],format='%Y-%m-%d')
    df_yf = df_yf[['Date','Open','High','Low','Close','Volume']]

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
    df_yf.to_sql(name=stock,con=conn,if_exists='append',index=False)
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
