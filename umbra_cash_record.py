from datetime import datetime, timezone, timedelta,date
import time
import pandas as pd
import numpy as np
import os

def concat_and_drop_duplicates(df, column1, column2):
    df_target_from = df[[column1]].rename(columns={column1: 'sybil_address'})
    df_target_to = df[[column2]].rename(columns={column2: 'sybil_address'})
    df_final = pd.concat([df_target_from, df_target_to]).drop_duplicates()
    
    return df_final


########### [1].Get all umbra.cash send records.
dfs=[]
file_path_list=['./umbra_cash_record/umbra_cash_send_action_0_80000.csv','./umbra_cash_record/umbra_cash_send_action_80000_160000.csv','./umbra_cash_record/umbra_cash_send_action_160000_240000.csv','./umbra_cash_record/umbra_cash_send_action_240000_320000.csv','./umbra_cash_record/umbra_cash_send_action_320000_400000.csv']
for file_path in file_path_list:
  print('### file_path:',file_path)
  df=pd.read_csv(file_path)
  df=df[~df['INTERMEDIATE_ADDRESS'].isnull()]
  dfs.append(df)
  print('len:',len(df))


df_umbra_cash_send_action=pd.concat(dfs).drop(['NUM_RANK'],axis=1)
print(df_umbra_cash_send_action)
print(df_umbra_cash_send_action.columns)



########### [2].Get all umbra.cash withdrawal records.
dfs=[]
file_path_list=['./umbra_cash_record/umbra_cash_withdraw_action_0_80000.csv','./umbra_cash_record/umbra_cash_withdraw_action_80000_160000.csv','./umbra_cash_record/umbra_cash_withdraw_action_160000_240000.csv']
for file_path in file_path_list:
  print('### file_path:',file_path)
  df=pd.read_csv(file_path)
  df=df[~df['INTERMEDIATE_ADDRESS'].isnull()]
  dfs.append(df)
  print('len:',len(df))

df_umbra_cash_withdraw_action=pd.concat(dfs).drop(['NUM_RANK'],axis=1)
print('##### df_umbra_cash_withdraw_action')
print(df_umbra_cash_withdraw_action)
print(df_umbra_cash_withdraw_action.columns)


########### [3].Associate the transaction initiator address and the recipient address via the INTERMEDIATE_ADDRESS.
df_merge_to_get_relation=df_umbra_cash_send_action.merge(df_umbra_cash_withdraw_action,on='INTERMEDIATE_ADDRESS',how='left')
df_merge_to_get_relation=df_merge_to_get_relation[~df_merge_to_get_relation['WITHDRAW_ADDRESS'].isnull()]
print('##### df_merge_to_get_relation')
print(df_merge_to_get_relation)
print(df_merge_to_get_relation.columns)




########### [4].Count how many WITHDRAW_ADDRESS correspond to the same SENDER_ADDRESS.
########### Find all records where the same SENDER_ADDRESS corresponds to more than 10 WITHDRAW_ADDRESS.
df_sender_address_stat=df_merge_to_get_relation.groupby('SENDER_ADDRESS').agg({'WITHDRAW_ADDRESS':'nunique'}).reset_index()
df_sender_address_stat=df_sender_address_stat.sort_values(by='WITHDRAW_ADDRESS',ascending=False)
df_sender_address_stat=df_sender_address_stat[df_sender_address_stat['WITHDRAW_ADDRESS']>20]
print('##### df_sender_address_stat')
print(df_sender_address_stat)
print(df_sender_address_stat.columns)


########### [5].Define both SENDER_ADDRESS and WITHDRAW_ADDRESS of these records as sybil_address, merge them, and remove duplicates.
core_sybil_address_list=df_sender_address_stat['SENDER_ADDRESS'].drop_duplicates().tolist()
df_umbra_record_by_sender_address=df_merge_to_get_relation.copy()
df_umbra_record_by_sender_address=df_umbra_record_by_sender_address[df_umbra_record_by_sender_address['SENDER_ADDRESS'].isin(core_sybil_address_list)]
df_umbra_record_by_sender_address=df_umbra_record_by_sender_address[['SENDER_ADDRESS','WITHDRAW_ADDRESS']].drop_duplicates()
df_sybil_address_by_sender_address=concat_and_drop_duplicates(df_umbra_record_by_sender_address,'SENDER_ADDRESS', 'WITHDRAW_ADDRESS')
print('### df_sybil_address_by_sender_address')
print(df_sybil_address_by_sender_address)



########### [6].Count how many SENDER_ADDRESS correspond to the same WITHDRAW_ADDRESS.
########### Find all records where the same WITHDRAW_ADDRESS corresponds to more than 10 SENDER_ADDRESS.
df_withdraw_address_stat=df_merge_to_get_relation.groupby('WITHDRAW_ADDRESS').agg({'SENDER_ADDRESS':'nunique'}).reset_index()
df_withdraw_address_stat=df_withdraw_address_stat.sort_values(by='SENDER_ADDRESS',ascending=False)
df_withdraw_address_stat=df_withdraw_address_stat[df_withdraw_address_stat['SENDER_ADDRESS']>20]
print('### df_withdraw_address_stat')
print(df_withdraw_address_stat)
print(df_withdraw_address_stat.columns)



########### [7].Define both SENDER_ADDRESS and WITHDRAW_ADDRESS of these records as sybil_address, merge them, and remove duplicates.
core_sybil_address_list=df_withdraw_address_stat['WITHDRAW_ADDRESS'].drop_duplicates().tolist()
df_umbra_record_by_withdraw_address=df_merge_to_get_relation.copy()
df_umbra_record_by_withdraw_address=df_umbra_record_by_withdraw_address[df_umbra_record_by_withdraw_address['WITHDRAW_ADDRESS'].isin(core_sybil_address_list)]
df_umbra_record_by_withdraw_address=df_umbra_record_by_withdraw_address[['SENDER_ADDRESS','WITHDRAW_ADDRESS']].drop_duplicates()
df_sybil_address_by_withdraw_address=concat_and_drop_duplicates(df_umbra_record_by_withdraw_address,'SENDER_ADDRESS', 'WITHDRAW_ADDRESS')
print('### df_sybil_address_by_withdraw_address')
print(df_sybil_address_by_withdraw_address)




########### [8].Merge and deduplicate the sybil_address obtained through SENDER_ADDRESS and WITHDRAW_ADDRESS.
df_umbra_sybil_address_all=pd.concat([df_sybil_address_by_sender_address,df_sybil_address_by_withdraw_address]).drop_duplicates()
print('### df_umbra_sybil_address_all')
print(df_umbra_sybil_address_all)




df_umbra_sybil_address_all.to_csv('./umbra_sybil_address.csv',index=False)





