import pandas as pd
import sqlalchemy as sa
import pyodbc
import numpy as np

connection = pyodbc.connect(
                            'Driver={SQL Server};'
                            'Server=DESKTOP-MAK81E6\SQLEXPRESS;'
                            'Database=StockAnalysis;'
                            'Trusted_Connection=yes;'
                           )

query = "SELECT * FROM DBO.LONGTERM_COMBINED"
data = pd.read_sql(query,con=connection,parse_dates=True)
data = data.rename(columns= {'index':'Date'})
data['Month'] = data['Date'].dt.month
data['Year'] = data['Date'].dt.year

group_data = data.groupby(['Month','Year'])

report_data = pd.DataFrame()
report_data['Monthly_Inv_Amt'] = group_data['Invested_on_day'].agg(np.sum)
report_data['Max_Daily_Inv_Amt'] = group_data['Invested_on_day'].agg(np.max)
report_data.reset_index(inplace=True)
report_data.sort_values(by=['Year','Month'], inplace=True)
print(report_data)
report_data = pd.merge(report_data, data, left_on='Max_Daily_Inv_Amt',right_on='Invested_on_day',how='inner')

print(report_data)



# for group,data in group_data:
#     print(group,data)
#     exit()
#
# print(data.head())

