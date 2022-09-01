import pandas as pd
import numpy as np
import pickle
import glob
from concurrent import futures
import os

TOKENS = ['FRAX','EURS','XCHF','XAUt','FEI','GUSD','BUSD','sUSD']
DECIMALS = [18,2,18,6,18,2,18,18]
FIRST_BLOCKS = [11465581,5835474,6622987,9339031,12168368,6302486,8523552,5767935]
LAST_BLOCK = 14497033

# split data into different files for multiprocessing
for TOKEN in TOKENS:
    dfRaw = pd.read_csv('{}_token_transfers.csv'.format(TOKEN))
    df = dfRaw[['from_address', 'to_address', 'value', 'block_number','log_index']].sort_values(by=['block_number', 'log_index']).reset_index(drop=True)
    addresses = list(set(df.from_address.to_list()) & set(df.to_address.to_list()))
    num = [i for i in range(len(addresses))]
    addressesDict = dict(zip(addresses,num))
    c = 1
    for a in np.array_split(addresses, 40):
        a_subset = {key: value for key, value in addressesDict.items() if key in a}
        f = open("sliced_accounts_{}_{}.pkl".format(TOKEN,c),"wb")
        pickle.dump(a_subset,f)
        f.close() 
        c += 1                 

# calculates balances for each address at each block where the address made a transaction and safes it in a dictionary
def balanceCalc(file):
    
    dfRaw = pd.read_csv('{}_token_transfers.csv'.format(TOKEN))
    df = dfRaw[['from_address', 'to_address', 'value', 'block_number','log_index']].sort_values(by=['block_number', 'log_index']).reset_index(drop=True)
    addresses = pd.read_pickle(file)
    addresses = list(addresses.keys())
    
    if DECIMAL < 18:
        df = pd.DataFrame(df.loc[(df['value'] != 0) & (df['from_address'] != df['to_address'])])
        tokenBalances = {}
        for address in addresses:   
            df2 = pd.DataFrame(df.loc[(df.from_address == address) | (df.to_address == address)])
            df2['balance'] = (np.where(df2['to_address'] == address, 1, -1) * df2['value']).cumsum()
            df2 = df2.groupby(by='block_number').last().reset_index()
            d = dict(zip(df2.block_number,df2.balance))
            tokenBalances[address] = d 
    else:
        df = pd.DataFrame(df.loc[(df['value'] != '0') & (df['from_address'] != df['to_address'])])
        tokenBalances = {}
        for address in addresses:   
            df2 = pd.DataFrame(df.loc[(df.from_address == address) | (df.to_address == address)])
            df2['multiple'] = (np.where(df2['to_address'] == address, 1, -1))
            m = df2.multiple.to_list()
            v = df2.value.to_list()
            c = []
            for i in range(len(v)):
                if i == 0:
                    c.append(str(int(v[i])*m[i]))
                else:
                    c.append(str(int(c[i-1]) + int(v[i])*m[i]))
            df2['balance'] = c    
            df2 = df2.groupby(by='block_number').last().reset_index()
            d = dict(zip(df2.block_number,df2.balance))
            tokenBalances[address] = d

    f = open('balances_{}'.format(file),'wb')
    pickle.dump(tokenBalances,f)
    f.close()
    
for i in range(len(TOKENS)):  
    TOKEN = TOKENS[i]
    DECIMAL = DECIMALS[i]
    FIRST_BLOCK = FIRST_BLOCKS[i]
    
    with futures.ProcessPoolExecutor(max_workers=40) as ex:
        for file in glob.glob('sliced_accounts_{}_*.pkl'.format(TOKEN)):
            ex.submit(balanceCalc, file)
    
    # combines all 40 partial dictionaries into one single dictionary
    d0 = pd.read_pickle('balances_sliced_accounts_{}_1.pkl'.format(TOKEN))
    for file in glob.glob('balances_sliced_accounts_{}_*.pkl'.format(TOKEN)):
        d1 = pd.read_pickle(file)
        d0.update(d1) 
    
    f = open('{}_balances.pkl'.format(TOKEN),'wb')
    pickle.dump(d0,f)
    f.close()
    print('{}_balances.pkl: sucess'.format(TOKEN)) 

    # check for all partial files
    # if exist -> deletes it 
    for file in glob.glob('balances_sliced_accounts_{}_*.pkl'.format(TOKEN)):
        if(os.path.exists(file) and os.path.isfile(file)):
            os.remove(file)
        else:
            print("file not found")