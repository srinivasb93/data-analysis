import pandas as pd
import sqlalchemy as sa
import datetime as dt
import pyodbc
import urllib
import quandl
import yfinance as yf
from nsepy import get_history,get_index_pe_history as get_pe


enddate = dt.date.today()

indices = ['NIFTY 50', 'NIFTY NEXT 50', 'NIFTY BANK', 'INDIA VIX', 'NIFTY 100', 'NIFTY 500', 'NIFTY MIDCAP 100', 'NIFTY MIDCAP 50', 'NIFTY INFRA', 'NIFTY REALTY', 'NIFTY ENERGY', 'NIFTY FMCG', 'NIFTY MNC', 'NIFTY PHARMA', 'NIFTY PSE', 'NIFTY PSU BANK', 'NIFTY SERV SECTOR', 'NIFTY IT', 'NIFTY SMLCAP 100', 'NIFTY 200', 'NIFTY AUTO', 'NIFTY MEDIA', 'NIFTY METAL', 'NIFTY COMMODITIES', 'NIFTY CONSUMPTION', 'NIFTY CPSE', 'NIFTY FIN SERVICE', 'NIFTY SMLCAP 50', 'NIFTY SMLCAP 250']

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

for stk_index in indices:
    stk_index_sql = "_".join(stk_index.split(" "))
    query = 'select max(date) from '+ stk_index_sql+'_PE'
    startdate = conn.execute(query)
    startdate = startdate.fetchall()
    start_dt = startdate[0][0]
    start_dt = pd.to_datetime(start_dt).date()
    print("Extracting Data from NSE for the index : {}" .format(stk_index))
    data = get_pe(symbol=stk_index, start=start_dt, end=enddate)
    data.reset_index(inplace=True)

    #Read data from .csv file
    # df=pd.read_csv('Results2.csv')
    # df.fillna('',inplace=True)
    print("Start Data Load for the index : {}".format(stk_index))
    #Write data read from .csv to SQL Server table
    data.to_sql(name=stk_index_sql+'_PE',con=conn,if_exists='append',index=False)
    print("Data Load done for the index : {}".format(stk_index))
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