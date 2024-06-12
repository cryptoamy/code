
# Process Data
### 1.1. Get the raw data    
a.Transcation Record of sending action from **Sender Address** to **Intermediate Address**    
https://flipsidecrypto.xyz/edit/queries/3e1de2da-f843-4628-aa3d-0b3f0e65ec5e    
b.Transcation Record of sending action from **Intermediate Address** to **Withdraw Address**  
https://flipsidecrypto.xyz/edit/queries/04b7fdb4-2359-4a90-8510-b317823dffaf

We save the data at the folder named **umbra_cash_record/**    
https://github.com/cryptoamy/layerzero_sybil_scan_report/tree/main/umbra_cash_record

### 1.2. Processing Umbra data, identify potential clusters of industrial sybil clustersthrough the fund flow of all transactions. Afterwards, compile a list of sybil addresses.
a.Merge raw data  together to get fund flow of all transfer throught Umbra
b.Identify clusters with a node count greater than 10, which are considered clusters of industrial sybil clusters All addresses within these clusters are designated as sybil addresses. 
Code:https://github.com/cryptoamy/code/blob/main/umbra_cash_record.py

We have obtained the exported CSV file ```umbra_sybil_address.csv.```    
Csv File from origin github report: https://github.com/cryptoamy/layerzero_sybil_scan_report/blob/main/umbra_sybil_address.csv    

### 1.3. Associate the sybil addresses obtained in 1.2 with the set of Layer Zero User Addresses. The intersection of these sets will be the sybil addresses among Layer Zero users.
Code: https://dune.com/queries/3741946    

### 1.4. Combine the Layer Zero addresses found in 5.3 with the fund flow of all transactions obtained in sector [5.2].Draw the graph of all sybil clusters.
Code: https://github.com/cryptoamy/code/blob/main/get_sybil_address_graph_.py
### Here we  have obtained the exported the final sybil address csv  ```layerzero_sybil_node_final.csv```
Csv File from origin github report:https://github.com/cryptoamy/layerzero_sybil_scan_report/blob/main/layerzero_sybil_node_final.csv
