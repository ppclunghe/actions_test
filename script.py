# %%
import os
import pandas as pd
import csv  
import io
from datetime import datetime
from datetime import date
import time
import requests
from dotenv import load_dotenv

# %%
load_dotenv()
dune_key = os.environ["DUNE_API"]

# %%
def create_dune_dataset():
    
    url = "https://api.dune.com/api/v1/table/create"

    payload = {
        "namespace": "lido",
        "table_name": "fluid_scsd_vaults_stats",
        "description": "Fluid vaults with Smart Col/Smart Debt",
        "schema": 
        [    {"name": "unixtimestamp", "type": "double"}
            , {"name": "id", "type": "double"}
            , {"name": "address", "type": "varbinary"}
            , {"name": "supply_token0", "type": "varbinary"}
            , {"name": "supply_token1", "type": "varbinary"}
            , {"name": "borrow_token0", "type": "varbinary"}
            , {"name": "borrow_token1", "type": "varbinary"}
            , {"name": "liquidity_supply_token0", "type": "double"}
            , {"name": "liquidity_supply_token1", "type": "double"}
            , {"name": "liquidity_borrow_token0", "type": "double"}
            , {"name": "liquidity_borrow_token1", "type": "double"}
            , {"name": "supply_dex_reserve_token0", "type": "double"}
            , {"name": "supply_dex_reserve_token1", "type": "double"}
            , {"name": "borrow_dex_debt_token0", "type": "double"}
            , {"name": "borrow_dex_debt_token1", "type": "double"}
            , {"name": "borrow_dex_reserve_token0", "type": "double"}
            , {"name": "borrow_dex_reserve_token1", "type": "double"}
            
        ],
        "is_private": False
    }

    headers = {
            "X-DUNE-API-KEY": dune_key,
            "Content-Type": "application/json"
        }

    response = requests.request("POST", url, json=payload, headers=headers)

    if response.status_code == 200 or response.status_code == 201:
            print("table creating ok...")
    else:
            print(response.text)
            print("Error: {}".format(response.reason))
      


def insert_to_dune_dataset(dataset_name, csv, key):
    url = f'https://api.dune.com/api/v1/table/lido/{dataset_name}/insert'

    headers = {
    "X-DUNE-API-KEY": key,
    "Content-Type": "text/csv"
    }


    response = requests.request("POST", url, data=csv, headers=headers)
    print('Response status code:', response.status_code)
    print('Response content:', response.content)


# %%
#create_dune_dataset()

# %%
ts = pd.Timestamp(pd.Timestamp.today(), tz='UTC')

# %%
ts = int(time.time()) 
svaults = [ '0x528CF7DBBff878e02e48E83De5097F8071af768D', # https://fluid.instadapp.io/stats/1/vaults#44, other vaults - TBD
            '0xb4a15526d427f4d20b0dAdaF3baB4177C85A699A', #weETH, https://fluid.instadapp.io/stats/1/vaults#74
            '0x9A64E3EB9c2F917CBAdDe75Ad23bb402257acf2E', #rsETH, https://api.fluid.instadapp.io/v2/1/vaults/0x9A64E3EB9c2F917CBAdDe75Ad23bb402257acf2E
            '0x153a0D021AeD5d20D9E59e8B9ecC9E3e9276f6C3', #weETHs, https://fluid.instadapp.io/stats/1/vaults#80

]
for v in svaults:
    print(v)             
    url = f'https://api.fluid.instadapp.io/v2/1/vaults/{v}'
    response = requests.get(url)
    print('status: ', response.status_code)
    if response.status_code == 200:
        dct = {}
        tmpdf = pd.DataFrame()
        dct['unixtimestamp'] = ts
        dct['id'] = response.json()['id']
        dct['address'] = response.json()['address']
        dct['supply_token0'] = response.json()['supplyToken']['token0']['address']
        dct['supply_token1'] = response.json()['supplyToken']['token1']['address']
        dct['borrow_token0'] = response.json()['borrowToken']['token0']['address']
        dct['borrow_token1'] = response.json()['borrowToken']['token1']['address']
        dct['liquidity_supply_token0'] = response.json()['liquiditySupplyData']['token0']['supply']
        dct['liquidity_supply_token1'] = response.json()['liquiditySupplyData']['token1']['supply']
        dct['liquidity_borrow_token0'] = response.json()['liquidityBorrowData']['token0']['borrow']
        dct['liquidity_borrow_token1'] = response.json()['liquidityBorrowData']['token1']['borrow']
        dct['supply_dex_reserve_token0'] = response.json()['supplyDexData']['token0RealReserves']
        dct['supply_dex_reserve_token1'] = response.json()['supplyDexData']['token1RealReserves']
        dct['borrow_dex_debt_token0'] = response.json()['borrowDexData']['token0Debt']
        dct['borrow_dex_debt_token1'] = response.json()['borrowDexData']['token1Debt']
        dct['borrow_dex_reserve_token0'] = response.json()['borrowDexData']['token0RealReserves']
        dct['borrow_dex_reserve_token1'] = response.json()['borrowDexData']['token1RealReserves']
        dct['blockchain'] = 'ethereum'
        
        tmpdf = pd.DataFrame([dct])
        output = io.StringIO()
        tmpdf.to_csv(output, index=False)
        csv_string = output.getvalue()
        insert_to_dune_dataset('fluid_scsd_vaults_stats', csv_string, dune_key)
    else:
        print('vault skipped')
    

# %%
svaults_arb = [ '0xeAEf563015634a9d0EE6CF1357A3b205C35e028D', #16
            '0x3996464c0fCCa8183e13ea5E5e74375e2c8744Dd' #17

]
for v in svaults_arb:
    print(v)             
    url = f'https://api.fluid.instadapp.io/v2/42161/vaults/{v}'
    response = requests.get(url)
    print('status: ', response.status_code)
    if response.status_code == 200:
        dct = {}
        tmpdf = pd.DataFrame()
        dct['unixtimestamp'] = ts
        dct['id'] = response.json()['id']
        dct['address'] = response.json()['address']
        dct['supply_token0'] = response.json()['supplyToken']['token0']['address']
        dct['supply_token1'] = response.json()['supplyToken']['token1']['address']
        dct['borrow_token0'] = response.json()['borrowToken']['token0']['address']
        dct['borrow_token1'] = response.json()['borrowToken']['token1']['address']
        dct['liquidity_supply_token0'] = response.json()['liquiditySupplyData']['token0']['supply']
        dct['liquidity_supply_token1'] = response.json()['liquiditySupplyData']['token1']['supply']
        dct['liquidity_borrow_token0'] = response.json()['liquidityBorrowData']['token0']['borrow']
        dct['liquidity_borrow_token1'] = response.json()['liquidityBorrowData']['token1']['borrow']
        dct['supply_dex_reserve_token0'] = response.json()['supplyDexData']['token0RealReserves']
        dct['supply_dex_reserve_token1'] = response.json()['supplyDexData']['token1RealReserves']
        dct['borrow_dex_debt_token0'] = response.json()['borrowDexData']['token0Debt']
        dct['borrow_dex_debt_token1'] = response.json()['borrowDexData']['token1Debt']
        dct['borrow_dex_reserve_token0'] = response.json()['borrowDexData']['token0RealReserves']
        dct['borrow_dex_reserve_token1'] = response.json()['borrowDexData']['token1RealReserves']
        dct['blockchain'] = 'arbitrum'
        
        tmpdf = pd.DataFrame([dct])
        output = io.StringIO()
        tmpdf.to_csv(output, index=False)
        csv_string = output.getvalue()
        insert_to_dune_dataset('fluid_scsd_vaults_stats', csv_string, dune_key)
    else:
        print('vault skipped')
   


