import json
import asyncio
import os
import aiohttp
from dotenv import load_dotenv

load_dotenv()


# async code

async def get(tokenId, session):
    URL = f'{os.getenv("API")}/api/v1/boxes/unspent/byTokenId/{tokenId}'
    data = []
    try:
        async with session.get(url=URL) as response:
            resp = await response.text()
            data.append(json.loads(resp)['items'][0]['address'])
    except Exception as e:
        pass
    return data


async def main(tokenIds: [str]):
    async with aiohttp.ClientSession() as session:
        resp = await asyncio.gather(*[get(url, session) for url in tokenIds])
    return [item for sublist in resp for item in sublist]  # returns the flattened list
