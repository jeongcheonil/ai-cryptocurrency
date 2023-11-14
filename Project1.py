import time
import requests
import pandas as pd
import datetime

start_time=datetime.datetime.now()
year=start_time.year
month=start_time.month
day=start_time.day  #### 시작한 날짜 파일 


while(1):



    book = {}
    response = requests.get ('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=5')
    book = response.json()


    data = book['data']

    bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric,errors='ignore')
    bids.sort_values('price', ascending=False, inplace=True)

    bids = bids.reset_index(); del bids['index']
    bids['type'] = 0

    asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric,errors='ignore')
    asks.sort_values('price', ascending=True, inplace=True)

    asks['type'] = 1
    print("###########################")
    print (bids)
    print ("")
    print (asks)
    print("###########################")
    print("")

    df = bids.append(asks)

    timestamp = datetime.datetime.now()
    req_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    df['quantity'] = df['quantity'].apply(lambda x: f'{x:.5f}')
    df['timestamp'] = req_timestamp
    path= f'./{year}-{month}-{day}.csv'
    df.to_csv ( path,  sep='|', index=False, header=False, mode ='a') #sep: 구분자를 | 로 대체 
    time.sleep(4.9)    # 약 5초마다 실행