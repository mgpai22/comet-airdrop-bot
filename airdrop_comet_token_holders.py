import os

from ergpy import appkit
from ergpy import helper_functions
from SQL_functions import get_token_holders
import math

from dotenv import load_dotenv

load_dotenv()

# This will send all holders of comet nft an equal amount of comet based on total_comet_to_send
# make sure .env is filled out based on .example-env

# Script is good for up to 1000 address. If this limit is exceeded, chained transaction will need to be implemented

# make sure to run comet_holders_data.py to generate a new db with the latest holder info

db_name = "snapshot_2023-01-02_23-01-55" # Make sure to change to correct db name
table_name_sorted_p2pk = "p2pk_sorted" # can edit (not recommended) but be consistent
least_amount_held = 1000000 # only addresses holding at least this amount will be selected
total_comet_to_send = 5000
comet_token_id = os.getenv("COMET")

holders = get_token_holders(db_name, table_name_sorted_p2pk, least_amount_held)
total_comet_to_send_per_wallet = math.floor(total_comet_to_send / len(holders))

if total_comet_to_send_per_wallet <= 1:
    print("There must be at least one comet sent to each wallet")
    quit(1)

amount_erg = []
amount_comet = []
comet_token_ids = []

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
