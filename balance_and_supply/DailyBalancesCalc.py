import pandas as pd
import numpy as np
from datetime import datetime
import pickle
import glob
from concurrent import futures
import os

TOKENS = ['FRAX','EURS','XCHF','XAUt','FEI','GUSD','BUSD','sUSD']
DECIMALS = [18,2,18,6,18,2,18,18]
FIRST_BLOCKS = [11465581,5835474,6622987,9339031,12168368,6302486,8523552,5767935]
LAST_BLOCK = 14497033

# creates a data frame with the balance of every address at the last block per day
def balancesCalc(file):
    ts = pd.read_csv('block_timestamps_complete.csv', parse_dates=['timestamp'])
    ts = ts.query('FIRST_BLOCK <= block_number <= LAST_BLOCK')
    # ts = ts.loc[(ts.block_number >= FIRST_BLOCK) & (ts.block_number <= LAST_BLOCK)]
    last = pd.DataFrame(ts[['timestamp','block_number']].groupby(pd.Grouper(key='timestamp', axis=0, freq='D')).last()).reset_index()
    blocks = last['block_number'].to_list()
    addresses = pd.read_pickle(file)
    addresses = list(addresses.keys())
    print('{}_initialized'.format(file))
    b = {}
    for address in addresses:
        balance = []
        for block in blocks: # take all transactions
            balance.append(round(getBalance(block, address)/10**DECIMAL,8))
        b[address] = balance  
    f = open('balances_per_day_{}'.format(file),'wb')
    pickle.dump(b,f)
    f.close()
        
for i in range(len(TOKENS)):
    TOKEN = TOKENS[i]
    FIRST_BLOCK = FIRST_BLOCKS[i]
    DECIMAL = DECIMALS[i]
    
    balances = pd.read_pickle('{}_balances.pkl'.format(TOKEN)) 
    
    with futures.ProcessPoolExecutor(max_workers=40) as ex:
        for slice in glob.glob('sliced_accounts_{}_*.pkl'.format(TOKEN)):
            ex.submit(balancesCalc, slice) 
            
    ts = pd.read_csv('block_timestamps_complete.csv', parse_dates=['timestamp'])
    ts = ts.loc[(ts.block_number >= FIRST_BLOCK) & (ts.block_number <= LAST_BLOCK)]
    last = pd.DataFrame(ts[['timestamp','block_number']].groupby(pd.Grouper(key='timestamp', axis=0, freq='D')).last()).reset_index()
    timestamps = last['timestamp'].to_list()
    
    d0 = pd.read_pickle('balances_per_day_sliced_accounts_{}_1.pkl'.format(TOKEN))
    for file in glob.glob('balances_per_day_sliced_accounts_{}_*.pkl'.format(TOKEN)):
        d1 = pd.read_pickle(file)
        d0.update(d1) 
    df = pd.DataFrame.from_dict(d0, orient='index').T
    df['timestamp'] = timestamps
    df.to_csv('{}_balances_daily.csv'.format(TOKEN),index=False)

    for file in glob.glob('sliced_accounts_{}_*.pkl'.format(TOKEN)):
        os.remove(file)
    for file in glob.glob('balances_per_day_sliced_accounts_{}_*'.format(TOKEN)):
         os.remove(file)