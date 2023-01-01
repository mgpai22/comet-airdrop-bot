import json
import asyncio
import aiohttp

# async code

async def get(tokenId, session):
    URL = f'https://api.ergoplatform.com/api/v1/boxes/unspent/byTokenId/{tokenId}'
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
    return [item for sublist in resp for item in sublist] # returns the flattened list
