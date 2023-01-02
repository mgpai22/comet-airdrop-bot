import asyncio
import SQL_functions
from datetime import datetime, timezone
from api_nft import main
import time
from dotenv import load_dotenv

load_dotenv()
start_time = time.time()

# run to update holders! Ensure to manually change db name in the variable below!
# entire process should take under 10 seconds to run since it is async!


# Below commented code is a one time thing ran to extract token ids from a txt file and put it into a db table
# with open('/data/message.txt', 'r') as f:
#     text = f.read()
#
# lines = text.split('\n')
#
# ids = []
# for line in lines:
#     id = line[:64]
#     SQL_functions.write_to_comet_nft_ids_table('comet_nft_data', id)

current_datetime_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')

db_name = f'snapshot_comet_nft_{str(current_datetime_utc)}'
# db_name = "snapshot_comet_nft_2023-01-01_17-09-27"
print("make sure to save this name then change the variable 'db_name''s value to whatever is printed!")
print(db_name)

complete_data_table_name = "nft_complete"

token_ids = SQL_functions.get_all_nft_id('comet_nft_data') # Do not modify, gets the tokenIds to query

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) # only enable if python is running on Windows

# asynchronously make api requests to find unspent boxes by tokenIds to find the holder address
addresses = asyncio.run(main(token_ids)) # spits out a [address: str]

data = list(zip(token_ids, addresses)) # puts tokenIds and addresses into a [(tokenId, address)] for easy dumping into a database

SQL_functions.deleteTable('comet_nft_data', "nft_complete") # delete old data if there are any

SQL_functions.write_to_comet_nft_holder_addresses(db_name, complete_data_table_name, data)

end_time = time.time()
elapsed_time = end_time - start_time

print(f'Elapsed time: {elapsed_time} seconds')
