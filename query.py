import time
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
import SQL_functions


# helps make gql queries to find out token holders, synchronous currently unfortunately

def initial_query(db_name, table_name_raw, token_id, url):
    take = 50
    skip = 0
    boxes = []
    has_more = True
    transport = AIOHTTPTransport(url=url)
    client = Client(transport=transport, fetch_schema_from_transport=True)
    # while True:
    while has_more:
        params = {
            "tokenId": token_id,
            "spent": False,
            "take": take,
            "skip": skip
        }
        query_1 = """
                query Query($tokenId: String, $spent: Boolean, $take: Int, $skip: Int) {
                  boxes(tokenId: $tokenId, spent: $spent, take: $take, skip: $skip) {
                    address
                    boxId
                    assets {
                      amount
                      tokenId
                    }
                  }
                }
        """
        query = gql(query_1)
        while True:
            try:
                result = client.execute(query, variable_values=params)
                break
            except Exception as e:
                print(e)
                time.sleep(5)
        for x in result['boxes']:
            # boxes.append(x['address'])
            address = x['address']
            boxId = x['boxId']
            for asset in x['assets']:
                if asset['tokenId'] == token_id:
                    comet_amount = asset['amount']
            # print("address:", address)
            # print("boxId:", boxId)
            # print("comet amount:", comet_amount)
            SQL_functions.write_to_raw_table(db_name, table_name_raw, address, int(comet_amount), boxId)

        if len(result['boxes']) == 50:
            skip += take
            print("boxes queried:", skip)
        else:
            has_more = False
    return boxes

