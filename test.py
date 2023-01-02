import asyncio
import math
from api_holder import main
import time
import requests
start_time = time.time()
tokenId = "0cd8c9f416e5b1ca9f986a7f10a84191dfb85941619e49e53c0dc30ebf83324b"
api_url = f'https://api.ergoplatform.com/api/v1/boxes/unspent/byTokenId/{tokenId}?limit=1&offset=0'

total = int(requests.get(api_url).json()['total'])
final_offset = ((math.floor(total / 100)) * 100) + 100
offsets = []

print(f'{total} boxes are about to be queried!')

for offset in range(0, final_offset, 100):
    offsets.append(offset)

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) # only enable if python is running on Windows

res = asyncio.run(main(offsets))
end_time = time.time()
print("Done")
print("Time taken to execute:", end_time - start_time)



