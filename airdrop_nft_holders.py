import os

from ergpy import appkit
from ergpy import helper_functions
from SQL_functions import get_all_nft_holders
import math

from dotenv import load_dotenv

load_dotenv()

# This will send all holders of comet nft an equal amount of comet based on total_comet_to_send
# Duplicate are NOT excluded
# make sure .env is filled out based on .example-env

# Script is good for up to 1000 address. If this limit is exceeded, chained transaction will need to be implemented

# make sure to run comet_nft_holders_data.py to generate a new db with the latest holder info

db_name = "snapshot_comet_nft_2023-01-01_06-03-48"  # Make sure to change to correct db name
complete_data_table_name = "nft_complete"  # can edit (not recommended) but be consistent
black_listed_wallets = ["9h6DG2TcDaToaq8L8M3FPj9qpKVdHiMkeuhUjqRzBR3XVtmwCwN"]
total_comet_to_send = 10000
comet_token_id = os.getenv("COMET")

holders = get_all_nft_holders(db_name, complete_data_table_name, black_listed_wallets)
total_comet_to_send_per_wallet = math.floor(total_comet_to_send / len(holders))
amount_erg = []
amount_comet = []
comet_token_ids = []

if total_comet_to_send_per_wallet <= 1:
    print("There must be at least one comet sent to each wallet")
    quit(1)

for holder in holders:
    amount_erg.append(0.001)
    amount_comet.append([total_comet_to_send_per_wallet])
    comet_token_ids.append([comet_token_id])

node_url: str = os.getenv("NODE")
ergo = appkit.ErgoAppKit(node_url)
wallet_mnemonic = os.getenv("MNEMONIC")

tx = helper_functions.send_token(ergo=ergo, amount=amount_erg, amount_tokens=amount_comet,
                                 receiver_addresses=holders, tokens=comet_token_ids,
                                 wallet_mnemonic=wallet_mnemonic, return_signed=True)

# print(tx)

submit_tx = ergo.txId(tx)
print(submit_tx)
