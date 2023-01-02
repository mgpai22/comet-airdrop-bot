import asyncio
import math
import os

import requests
from api_holder import main
from dotenv import load_dotenv

load_dotenv()


# helps make rest api queries to find out token holders asynchronously!

def initial_query(db_name, table_name_raw, token_id):
    api_url = f'{os.getenv("API")}/api/v1/boxes/unspent/byTokenId/{token_id}?limit=1&offset=0'
    total = int(requests.get(api_url).json()['total'])
    final_offset = ((math.floor(total / 100)) * 100) + 100
    offsets = []

    print(f'{total} boxes are about to be queried!')

    for offset in range(0, final_offset, 100):
        offsets.append(offset)

    asyncio.set_event_loop_policy(
        asyncio.WindowsSelectorEventLoopPolicy())  # only enable if python is running on Windows

    asyncio.run(main(offsets, db_name, table_name_raw))
