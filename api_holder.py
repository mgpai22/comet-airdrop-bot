import json
import asyncio
import os
import time
import aiohttp
from SQL_functions import write_to_raw_table
from dotenv import load_dotenv

load_dotenv()


# async code

async def get(offset, session, db_name, table_name_raw):
    tokenId = os.getenv("COMET")
    URL = f'{os.getenv("API")}/api/v1/boxes/unspent/byTokenId/{tokenId}?limit=100&offset={offset}'
    # data = []
    max_retries = 10  # maximum number of retries
    retries = 0  # current number of retries
    success = False  # flag to track whether the request was successful

    while not success and retries < max_retries:
        try:
            async with session.get(url=URL) as response:
                resp = await response.text()
                for item in json.loads(resp)['items']:
                    # data.append(item['address'])
                    address = item['address']
                    box = item['boxId']
                    for token in item['assets']:
                        if token['tokenId'] == tokenId:
                            comet = token['amount']
                    write_to_raw_table(db_name, table_name_raw, address, comet, box)
            success = True  # request succeeded, set flag to True
        except Exception as e:
            retries += 1  # increment retry count
            # print(f'Error: {e}. Retrying...')
            # print(URL)
            time.sleep(1)  # wait 1 second before retrying

    if not success:
        print(f'Error: request failed after {max_retries} retries')
        print("Data is incomplete!")


async def main(offsets: [int], db_name, table_name_raw):
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[get(offset, session, db_name, table_name_raw) for offset in offsets])
