import nsepy
from nsepy import get_history
from datetime import date
today_dt = date(2021,3,5)
import sqlalchemy as sa
import urllib.parse

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

bcopy = nsepy.history.get_price_list(dt=today_dt)
# bcopy.to_csv('bhavcopy_'+today_dt.strftime('%d%m%Y')+'.csv',index=False)
bcopy.to_sql(name='BHAVCOPY',con=conn,if_exists='replace',index=False)

bcopy_indices = nsepy.history.get_indices_price_list(dt=today_dt)
# bcopy.to_csv('bhavcopy_'+today_dt.strftime('%d%m%Y')+'.csv',index=False)
bcopy_indices.to_sql(name='BHAVCOPY_INDICES',con=conn,if_exists='replace',index=False)

print('Bhav Copy extraction and data load is complete')