#!/usr/bin/env python3

import pandas as pd
import requests
import time
from tqdm import tqdm



def dataframe_no_duplicates(path_to_csv):
    df = pd.read_csv(path_to_csv)
    df_no_duplicates = df.drop_duplicates(subset='idx_assets_address')

    return df_no_duplicates

def get_activity_data(list_of_adresses):
    address = []
    revealed = []
    balance = []
    numTransactions = []
    firstActivityTime = []
    lastActivityTime = []

    for tz_address in tqdm(list_of_adresses):
        try:
            user_data = requests.get(f"https://api.tzkt.io/v1/accounts/{tz_address}").json()
        except Exception as e:
            print(e)
        if ('type' in user_data.keys() and user_data['type']=='user'):
            address.append(user_data['address'])
            revealed.append(user_data['revealed'])
            balance.append(user_data['balance'])
            numTransactions.append(user_data['numTransactions'])
            firstActivityTime.append(user_data['firstActivityTime'])
            lastActivityTime.append(user_data['lastActivityTime'])
        # time.sleep(2)
    data = {'Address': address, 'Revealed': revealed, 
            'Balance': balance, 'NumTransactions': numTransactions, 
            'FirstActivityTime': firstActivityTime, 'LastActivityTime': lastActivityTime}
    
    return data

if __name__ == '__main__':
    path_to_csv = 'VERI_storage.ledger_live_export.csv'
    df = dataframe_no_duplicates(path_to_csv)
    list_of_adresses = df['idx_assets_address'].to_list()
    data_dictionary = get_activity_data(list_of_adresses)
    data_df = pd.DataFrame(data_dictionary)
    data_df.to_csv('cleaned_veri_data.csv') 







