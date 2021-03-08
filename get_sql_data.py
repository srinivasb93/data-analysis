
import pandas as pd
import sqlalchemy as sa
import datetime as dt
import pyodbc
import urllib
import quandl
import yfinance as yf
from nsepy import get_history

stock = 'MOTHERSUMI'


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

query = "SELECT * FROM dbo." + stock + " ORDER BY DATE ASC "
data = pd.read_sql_query(query, con=conn, parse_dates=True)
data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%m/%d/%Y')
data['Symbol'] = stock

print(data.tail())