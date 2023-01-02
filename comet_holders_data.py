import os

import query
from datetime import datetime, timezone
import time
import SQL_functions
from dotenv import load_dotenv

load_dotenv()


start_time = time.time()

# This script retrieves all the holders of comet and the amount they hold and dumps it into a database to further sort
# This will take around 10 minutes, do not stop it

comet_token_mainnet = os.getenv("COMET")
current_datetime_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')

db_name = f'snapshot_{str(current_datetime_utc)}'
# db_name = "snapshot_2023-01-02_23-01-55"
print("make sure to save this name then change the variable 'db_name''s value to whatever is printed! If you care about going back to see stats!")
print("In the end the db name matters the most in the airdrop_comet_token_holders.py file")
print(db_name)

# These names can be edited, just be sure to be consistent
table_name_raw = "raw"
table_name_sorted_p2pk = "p2pk_sorted"
table_name_sorted_script = "scripts_sorted"

# below variables are just for stats, edit them freely
amount_top_to_get = 2
amount_to_exclude_for_excluded_avg = 10
least_amount_held = 1000000


addresses = query.initial_query(db_name, table_name_raw, comet_token_mainnet)
SQL_functions.sort_raw(db_name, table_name_raw, table_name_raw + "_sorted")
SQL_functions.sort_and_merge_addresses(db_name, table_name_raw, table_name_sorted_p2pk, True)
SQL_functions.sort_and_merge_addresses(db_name, table_name_raw, table_name_sorted_script, False)
holders = SQL_functions.get_token_holders(db_name, table_name_sorted_p2pk, least_amount_held)
end_time = time.time()
elapsed_time = end_time - start_time

print(f'Elapsed time: {elapsed_time} seconds')

# Uncomment Below to get status!

# top_raw, top_p2pk, top_script, comet_supply, comet_supply_p2pk, comet_supply_script = SQL_functions.get_token_stats(db_name, table_name_raw + "_sorted", table_name_sorted_p2pk, table_name_sorted_script, amount_top_to_get, amount_to_exclude_for_excluded_avg)


#
# print(f"All Top {amount_top_to_get} Holders!\n")
# for row in top_raw:
#     address = row[0]
#     amountComet = row[1]
#     print(f'Address: {address}, Amount: {amountComet}\n')
#
# print(f"All Top {amount_top_to_get} Wallet Holders!\n")
# for row in top_p2pk:
#     address = row[0]
#     amountComet = row[1]
#     print(f'Address: {address}, Amount: {amountComet}\n')
#
# print(f"All Top {amount_top_to_get} Script Holders!\n")
# for row in top_script:
#     address = row[0]
#     amountComet = row[1]
#     print(f'Address: {address}, Amount: {amountComet}\n')
#


# print(f'Total comet: {comet_supply[0]}, Average comet holder amount: {comet_supply[1]},  Average comet holder excluding top {amount_to_exclude_for_excluded_avg} amount: {comet_supply[2]}')
# print(f'Total comet held by wallets: {comet_supply_p2pk[0]}, Average comet wallet holder amount: {comet_supply_p2pk[1]}')
# print(f'Total comet held by scripts: {comet_supply_script[0]}, Average comet script holder amount: {comet_supply_script[1]}')
# print(f'Total comet burned: {(21 * 10 **9) - int(comet_supply[0])}')
# print(f'There are about {len(holders)} wallets that hold at least {least_amount_held} comet')




