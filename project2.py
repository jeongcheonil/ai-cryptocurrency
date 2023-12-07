import timeit
import requests
import pandas as pd
import datetime
import os
import itertools

def get_sim_df_order (fn): # csv���� �ҷ����� fn�� ���ϸ� ��ġ����? �ƴ� Ȯ����� ���� �뵵���� ���� "2023-10-27-bithumb-BTC' ���� -book.csv�� ����
    
    print ('loading... %s' % fn)
    book_fn=fn+'-book.csv'  
    df = pd.read_csv(book_fn, delimiter='|', header=None, names=['price', 'quantity', 'type', 'timestamp']).apply(pd.to_numeric,errors='ignore') # ���ڷ� ��ȯ�� ����
    
    #print df.to_string();print '------'
    
    group_order = df.groupby(['timestamp']) # timestamp�� �������� �ϳ��� �׷츸��.
    print("complete")
    return group_order

def write_csv(fn, df):
    book_fn=fn+'-book.csv' #������ �� ���ϰ� �Ϸ��� 
    new_fn = fn+'-feature.csv'
    should_write_header = os.path.exists(new_fn)
    if should_write_header == False:
        df.to_csv(new_fn, index=False, header=True, mode = 'a')
    else:
        df.to_csv(new_fn, index=False, header=False, mode = 'a')


def faster_calc_indicators(fn):
    
    start_time = timeit.default_timer()

    # FROM CSV FILES (DAILY)
    #group_o = get_sim_df_order(raw_book_csv(raw_fn, ('%s-%s-%s' % (_tag, exchange, currency))))
    group_order=get_sim_df_order(fn)  #=group_order�� ��Ÿ���� 

    #group_t = get_sim_df_trade(raw_trade_csv(raw_fn, ('%s-%s-%s' % (_tag, exchange, currency)))) #fix for book-1 regression
    
    delay = timeit.default_timer() - start_time
    print ('df loading delay: %.2fs' % delay)
     
    level_1 = 2 
    level_2 = 5

    print ('param levels' ,level_1, level_2)  #, exchange, currency)

    #(ratio, level, interval seconds )   
    #book_imbalance_params = [(0.2,level_1,1),(0.2,level_2,1)]  # �����ϰ� �Ű����� �����ϱ� ����
    seq = 0
    print ('total groups:', len(group_order.size().index))
    
    #main part
    #employee_df={}
    employee_df=pd.DataFrame({'book-imbalnace-0.2-5-1':pd.Series([]),'mid_price_top':pd.Series([]),'mid_price_wt':pd.Series([]),'mid_price_wt_bias':pd.Series([]),'mid_price_mkt':pd.Series([]),
                  'mid_price_del':pd.Series([]),'mid_price_dom':pd.Series([]),'timestamp':pd.Series([])})
    new_employee={'book-imbalnace-0.2-5-1':pd.Series([]),'mid_price_top':pd.Series([]),'mid_price_wt':pd.Series([]),'mid_price_wt_bias':pd.Series([]),'mid_price_mkt':pd.Series([]),
                  'mid_price_del':pd.Series([]),'mid_price_dom':pd.Series([]),'timestamp':pd.Series([])}
    #for gr_order in itertools.izip (group_order):
    keys=group_order.groups.keys()
    for i in keys:
        gr_order=group_order.get_group(i)
        if gr_order is None :
            print ('Warning: group is empty')
            continue

         #timestamp = (gr_order[1].iloc[0])['timestamp']
        gr_bid_level = gr_order[(gr_order.type == 0)]
        gr_ask_level = gr_order[(gr_order.type == 1)]
        ratio=0.2 ; level=5 ; interval=1 
        mid_price_top=(gr_bid_level.iloc[0].price + gr_ask_level.iloc[0].price) * 0.5  
        mid_price_wt=((gr_bid_level.head(level))['price'].mean() + (gr_ask_level.head(level))['price'].mean()) * 0.5## ���ǻ���: csv�� �̹� ������������ ���ĵǾ� �־�� �Ѵ�.
        print(mid_price_wt)
        mid_price_mkt= round((gr_bid_level.iloc[0].price*gr_ask_level.iloc[0].quantity + gr_ask_level.iloc[0].price*gr_bid_level.iloc[0].quantity)
                             /(gr_bid_level.iloc[0].price+gr_ask_level.iloc[0].quantity),1)
        mid_price_wt_bias=((gr_bid_level.head(level))['price'].mean() - (gr_ask_level.head(level))['price'].mean()) * 0.5 # price_wt�� bid�� ������� ask�� ������� (������ ask, ����� bid�� ����)
        #mid_price_vwap = (group_t['total'].sum())/(group_t['units_traded'].sum())
        mid_price_del=((gr_bid_level.head(level))['price'][1:].mean() + (gr_ask_level.head(level))['price'][1:].mean()) * 0.5

        mid_price_dom=((gr_bid_level.head(5))['quantity'].sum() + (gr_ask_level.head(5))['quantity'].sum()) * 0.5   # Deepths Of Market
        
        #gr_bid_level.head(5)['quantity'].sum() = bid of dom
        #gr_ask_level.head(5)['quantity'].sum() = ask of dom
        #(gr_bid_level.iloc[0].price + gr_ask_level.iloc[0].price) * 0.5 = mid price in given hint.py
        
        
        quant_v_bid = gr_bid_level.quantity**ratio
        price_v_bid = gr_bid_level.price * quant_v_bid
        quant_v_ask = gr_ask_level.quantity**ratio
        price_v_ask = gr_ask_level.price * quant_v_ask

        askQty = quant_v_ask.values.sum()
        bidPx = price_v_bid.values.sum()
        bidQty = quant_v_bid.values.sum()
        askPx = price_v_ask.values.sum()
        bid_ask_spread = interval

        book_price = (((askQty*bidPx)/bidQty) + ((bidQty*askPx)/askQty)) / (bidQty+askQty)
        
        book_imbalance=(book_price-mid_price_top)/bid_ask_spread

        new_employee = pd.DataFrame({'book-imbalnace-0.2-5-1': [book_imbalance],'mid_price_top': [mid_price_top],'mid_price_wt': [mid_price_wt]
                           ,'mid_price_wt_bias':[mid_price_wt_bias],'mid_price_mkt': [mid_price_mkt], 'mid_price_del': [mid_price_del], 'mid_price_dom':[mid_price_dom],'timestamp': [i]})
  
        employee_df = employee_df.append(new_employee, ignore_index=True)
        seq += 1
        if seq%500==0 : # ����� Ȯ���Ϸ��� 
            print(seq)

    write_csv(fn, employee_df)
faster_calc_indicators('./2023-10-27-bithumb-BTC') # �츮�� ���� './2023-10-27-bithumb-BTC-book.csv'   


