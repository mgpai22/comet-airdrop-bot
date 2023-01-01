
# Comet Airdrop Bot

This bot utilizes GraphQL to extract data on all comet holders and stores it in a local SQLite database. It then sorts this information based on wallet holders and script holders, proceeding to airdrop comet tokens to the wallet holders in a single transaction using [ergpy](https://github.com/mgpai22/ergpy). 


Additionally, the bot performs an airdrop of comet tokens to comet NFT holders by making several thousand asynchronous, concurrent REST API requests to locate wallet addresses, which are subsequently stored in a database and airdropped using using [ergpy](https://github.com/mgpai22/ergpy). 




## Deployment

Clone this repo

```bash
 https://github.com/mgpai22/comet-airdrop-bot.git
```
Or download zip and extract

Change dir into root

```
cd comet-airdrop-bot
```

install requirements

```
pip3 install -r requirements.txt

```

Run the `comet_holders_data.py` file to generate a database with all holders of comet. This will take 30 minutes. Make sure to go through the comments!

Run the `comet_nft_holders_data.py` file to generate a database with all holders of comet nfts. This will take ten seconds. Make sure to go through the comments!

Make a file named `.env` and copy and fill out all the parametrs from `.example-env`

Run the `airdrop_comet_token_holders.py` make sure to change `db_name` to the latest snapshot name. Make sure to go through the comments!

Run the `airdrop_comet_nft_holders.py` make sure to change `db_name` to the latest snapshot name. Make sure to go through the comments!