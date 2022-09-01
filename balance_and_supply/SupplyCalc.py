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

# slices the number of blocks to calculate the supply into 40 files for multiprocessing
for i in range(len(TOKENS)):
    TOKEN = TOKENS[i]
    FIRST_BLOCK = FIRST_BLOCKS[i]
    slices = round((LAST_BLOCK - FIRST_BLOCK)/40)+1
    count = 1
    for s in range(FIRST_BLOCK,LAST_BLOCK+1,slices):
        blocks = [i for i in range(s,s+slices,1)]
        blockList = pd.DataFrame({'block_number':blocks})
        blockList.to_csv('block_list_{}_{}.csv'.format(TOKEN,count),index=False)
        count += 1

# get balance for given address at a given block_number
def getBalance(block, address):
    d = balances[address]
    valid_keys = [key for key in d if key <= block]
    if valid_keys == []:
        return 0
    else:
        balance = d[block] if block in d else d[min(valid_keys, key=lambda k: abs(k-block))] 
        return int(balance)
    
# calculates the supply at every block
def supplyCalc(file):
    b = pd.read_csv(file)
    blocks = b['block_number'].to_list()
    addresses = list(balances.keys())     
    df = pd.DataFrame({'block_number':blocks})
    supply = []
    for block in blocks:
        balance = 0
        for address in addresses:
            b = getBalance(block,address)
            if b > 0:
                balance += b
        supply.append(str(balance))
        count += 1
    df['supply'] = supply
    df.to_csv('balances_{}_{}'.format(TOKEN,file))

for i in range(len(TOKENS)):
    TOKEN = TOKENS[i]
    FIRST_BLOCK = FIRST_BLOCKS[i]
    balances = pd.read_pickle('{}_balances.pkl'.format(TOKEN)) 
    
    with futures.ProcessPoolExecutor(max_workers=40) as ex:
        for slice in glob.glob('block_list_{}_*.csv'.format(TOKEN)):
            ex.submit(supplyCalc, slice) 

    joined_files = os.path.join("balances_{}_*.csv".format(TOKEN))
    joined_list = glob.glob(joined_files)
    df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True)
    ts = pd.read_csv('block_timestamps_complete.csv')
    ts = ts.query('FIRST_BLOCK <= block_number <= LAST_BLOCK')
    # ts = ts.loc[(ts['block_number'] >= FIRST_BLOCK) & (ts['block_number'] <= LAST_BLOCK)]
    merged_df = ts.merge(df, on=['block_number'], how='left')
    del merged_df['Unnamed: 0']
    merged_df.to_csv('{}_supply.csv'.format(TOKEN), index=False)  
    print('{}_supply.csv: sucess'.format(TOKEN)) 

    # check for all partial files
    # if exist -> deletes it 
    for file in glob.glob('block_list_{}_{}.csv'.format(TOKEN,count)):
        if(os.path.exists(file) and os.path.isfile(file)):
            os.remove(file)
        else:
            print("file not found")
    for file in glob.glob('balances_{}_*.pkl'.format(TOKEN)):
        if(os.path.exists(file) and os.path.isfile(file)):
            os.remove(file)
        else:
            print("file not found")