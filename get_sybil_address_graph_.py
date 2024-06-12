from datetime import datetime, timezone, timedelta,date
import time
import pandas as pd
import numpy as np
import os, random
from tqdm import tqdm


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




########### [3] Get all raw data of layer_zero_sybil_address.
# Associate it with the fund flow data of Umbra, keeping only the Umbra fund flow data of Layer Zero users.
df_layerzero_sybil_address=pd.read_csv('./layer_zero_sybil_address.csv')


layerzero_sybil_address_list=df_layerzero_sybil_address['layer_zero_sybil_address'].drop_duplicates().tolist()
print('### df_layerzero_sybil_address')
print(df_layerzero_sybil_address)
df_layerzero_sybil_record=df_merge_to_get_relation[(df_merge_to_get_relation['SENDER_ADDRESS'].isin(layerzero_sybil_address_list)) &(df_merge_to_get_relation['WITHDRAW_ADDRESS'].isin(layerzero_sybil_address_list))]
print('### df_layerzero_sybil_record')
print(df_layerzero_sybil_record)
print(df_layerzero_sybil_record.columns)



########### [3] Get all layerzero sybil addresses with fund flow relationships, and filter out all industrial witch clusters with fund flows greater than 20.
df_relation=df_layerzero_sybil_record.copy()[['SENDER_ADDRESS','WITHDRAW_ADDRESS']].drop_duplicates()
print('### df_relation')
print(df_relation)

df_SENDER_ADDRESS_stat=df_relation.copy().groupby(['SENDER_ADDRESS']).agg({'WITHDRAW_ADDRESS':'nunique'}).reset_index().sort_values(by='WITHDRAW_ADDRESS',ascending=False).rename(columns={'SENDER_ADDRESS':'core_address','WITHDRAW_ADDRESS':'satellite_address'})
print('### df_SENDER_ADDRESS_stat')
print(df_SENDER_ADDRESS_stat)

df_WITHDRAW_ADDRESS_stat=df_relation.copy().groupby(['WITHDRAW_ADDRESS']).agg({'SENDER_ADDRESS':'nunique'}).reset_index().sort_values(by='SENDER_ADDRESS',ascending=False).rename(columns={'WITHDRAW_ADDRESS':'core_address','SENDER_ADDRESS':'satellite_address'})
print('### df_WITHDRAW_ADDRESS_stat')
print(df_WITHDRAW_ADDRESS_stat)


df_layerzero_clusters_final=pd.concat([df_SENDER_ADDRESS_stat,df_WITHDRAW_ADDRESS_stat]).groupby('core_address').agg({'satellite_address':'max'}).reset_index().sort_values(by='satellite_address',ascending=False).rename(columns={'satellite_address':'satellite_address_count'})


df_layerzero_clusters_final=df_layerzero_clusters_final[df_layerzero_clusters_final['satellite_address_count']>20]
print('#### df_clusters_final')
print(df_layerzero_clusters_final)


core_address_list=df_layerzero_clusters_final['core_address'].drop_duplicates().tolist()
df_layerzero_sybil_relation_final=df_merge_to_get_relation[(df_merge_to_get_relation['SENDER_ADDRESS'].isin(core_address_list))|(df_merge_to_get_relation['WITHDRAW_ADDRESS'].isin(core_address_list))][['SENDER_ADDRESS','WITHDRAW_ADDRESS']].drop_duplicates()
print('### df_layerzero_sybil_relation_final')
print(df_layerzero_sybil_relation_final)



df_layerzero_sybil_all_node=concat_and_drop_duplicates(df_layerzero_sybil_relation_final, 'SENDER_ADDRESS', 'WITHDRAW_ADDRESS')

def get_address_type(sybil_address, core_address_list):
    if sybil_address in core_address_list:
        return 'core'
    else:
        return 'satellite'
df_layerzero_sybil_all_node['label']=df_layerzero_sybil_all_node['sybil_address']
df_layerzero_sybil_all_node['account_type'] = df_layerzero_sybil_all_node['sybil_address'].apply(lambda x: get_address_type(x, core_address_list))
print('### df_layerzero_sybil_all_node')
print(df_layerzero_sybil_all_node)


for core_address in tqdm(core_address_list):
    print('#### core_address:',core_address)
    df_relation=df_layerzero_sybil_relation_final[(df_layerzero_sybil_relation_final['SENDER_ADDRESS'].isin([core_address]))|(df_layerzero_sybil_relation_final['WITHDRAW_ADDRESS'].isin([core_address]))]
    all_node_list=concat_and_drop_duplicates(df_relation, 'SENDER_ADDRESS', 'WITHDRAW_ADDRESS')['sybil_address'].tolist()
    print('### all_node_list:',len(all_node_list))
    print(all_node_list)
    df_all_node=df_layerzero_sybil_all_node[df_layerzero_sybil_all_node['sybil_address'].isin(all_node_list)]
    df_relation=df_relation.rename(columns={'SENDER_ADDRESS':'from_address','WITHDRAW_ADDRESS':'to_address'})
    df_all_node=df_all_node.rename(columns={'sybil_address':'address'})


    df_from_address_stat=df_relation.copy().groupby(['from_address']).agg({'to_address':'nunique'}).reset_index().sort_values(by='to_address',ascending=False).rename(columns={'from_address':'core_address','to_address':'satellite_address'})
    print('### df_from_address_stat')
    print(df_from_address_stat)

    df_to_address_stat=df_relation.copy().groupby(['to_address']).agg({'from_address':'nunique'}).reset_index().sort_values(by='from_address',ascending=False).rename(columns={'to_address':'core_address','from_address':'satellite_address'})
    print('### df_to_address_stat')
    print(df_to_address_stat)


    df_target_layerzero_clusters=pd.concat([df_from_address_stat,df_to_address_stat]).groupby('core_address').agg({'satellite_address':'max'}).reset_index().sort_values(by='satellite_address',ascending=False).rename(columns={'satellite_address':'satellite_address_count'})
    core_address=df_target_layerzero_clusters['core_address'].tolist()[0]

    df_node_list_final=df_all_node.copy()[['address']]
    df_node_list_final['account_type']='satellite'
    df_node_list_final.loc[df_node_list_final['address'] == core_address, 'account_type'] = 'core'
    df_node_list_final=df_node_list_final.sort_values(by='account_type')
    node_list_final=df_node_list_final['address'].drop_duplicates().tolist()
    df_transcation_detail_final=df_layerzero_sybil_record.copy()
    df_transcation_detail_final=df_transcation_detail_final[(df_transcation_detail_final['SENDER_ADDRESS'].isin(node_list_final)) &(df_transcation_detail_final['WITHDRAW_ADDRESS'].isin(node_list_final))]



    print('#### core_address')
    print(core_address)
    print('#### df_node_list_final')
    print(df_node_list_final)
    print('#### df_transcation_detail_final')
    print(df_transcation_detail_final)


    node_size=len(df_node_list_final)
    folder_name = core_address
    os.makedirs(f'./result/{node_size}_nodes_{core_address}', exist_ok=True)

    excel_file_path = f'./result/{node_size}_nodes_{core_address}/{node_size}_nodes_detail_{core_address}.xlsx'
    writer = pd.ExcelWriter(excel_file_path, engine='xlsxwriter')

    df_node_list_final.to_excel(writer, sheet_name='clusters', index=False)

    df_transcation_detail_final.to_excel(writer, sheet_name='transcation_detail', index=False)
    writer.close()