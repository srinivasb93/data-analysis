import pandas as pd
import sqlalchemy as sa
import datetime as dt
import pyodbc
import urllib
import quandl
import yfinance as yf
from nsepy import get_history
from nsetools import Nse

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

nse = Nse()

stock = nse.get_fno_lot_sizes()
print(stock)
fno_data = pd.DataFrame.from_dict(stock,orient='index')
fno_data.reset_index(inplace=True)
fno_data.columns = ['ENTITY','LOT_SIZE']
print(fno_data)
#Read data from .csv file
# df=pd.read_csv('Results2.csv')
# df.fillna('',inplace=True)
print("Start Data Load")
#Write data read from .csv to SQL Server table

fno_data.to_sql(name='FNO_LOT_SIZE',con=conn,if_exists='replace',index=False)
print("Data Load done")
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