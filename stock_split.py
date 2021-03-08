import pandas as pd
import datetime as dt
import yfinance as yf

prices=pd.read_csv("Stock_Symbol_NXT50.csv")
stocks = prices['Symbol'].tolist()
start_date = dt.date(2020,5,1)
end_date = dt.date.today()
stk_split_data = pd.DataFrame()

for stock in stocks:
    print(stock)
    get_stocks = yf.Ticker(stock+'.NS')
    df_yahoo = get_stocks.history(start=start_date,end=end_date,interval='1d')
    df_yahoo['Stock'] = stock
    df_yahoo.reset_index(inplace=True)
    split_data = df_yahoo[['Date','Stock Splits','Stock']][df_yahoo['Stock Splits']>0]
    stk_split_data = stk_split_data.append(split_data)
    print(split_data)

stk_split_data.to_csv('Stock_Split2020_NXT50.csv',index=False)