import sqlite3
from statistics import mean
from address_fun import isP2PK


def create_comet_nft_ids_table(db_name):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    command1 = f'CREATE TABLE IF NOT EXISTS {"nft_ids"}(id TEXT)'
    cursor.execute(command1)
    connection.commit()
    connection.close()


def write_to_raw_table(db_name, table_name_raw, address, amountComet, boxId):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    command1 = f'CREATE TABLE IF NOT EXISTS {table_name_raw}(address TEXT, amountComet NUMBER, boxId TEXT)'
    cursor.execute(command1)
    cursor.execute(f"INSERT INTO {table_name_raw} VALUES (?,?,?)",
                   (address, amountComet, boxId))
    connection.commit()
    connection.close()


def write_to_raw_test_table(db_name, table_name_raw, address):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    command1 = f'CREATE TABLE IF NOT EXISTS {table_name_raw}(address TEXT)'
    cursor.execute(command1)
    cursor.execute(f"INSERT INTO {table_name_raw} VALUES (?)",
                   (address,))
    connection.commit()
    connection.close()


def write_to_comet_nft_ids_table(db_name, id):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    table_name = "nft_ids"
    command1 = f'CREATE TABLE IF NOT EXISTS {table_name}(id TEXT)'
    cursor.execute(command1)
    cursor.execute(f"INSERT INTO {table_name} VALUES (?)",
                   (id,))
    connection.commit()
    connection.close()


def write_to_comet_nft_holder_addresses(db_name, table_name, addresses: [tuple]):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    command1 = f'CREATE TABLE IF NOT EXISTS {table_name}(id TEXT, address TEXT)'
    cursor.execute(command1)
    cursor.executemany(f'''
        INSERT INTO {table_name} (id, address)
        VALUES (?, ?)
    ''', addresses)
    connection.commit()
    cursor.close()
    connection.close()


def sum_addresses_print(db_name):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    connection.create_function("isP2PK", 1, isP2PK)
    cursor = connection.cursor()
    tableName = "raw"
    amt_list = []
    command1 = f'''
    SELECT address, SUM(amountComet)
    FROM {tableName}
    WHERE isP2PK(address) = 1
    GROUP BY address
    ORDER BY SUM(amountComet) DESC
'''
    cursor.execute(command1)
    results = cursor.fetchall()
    for row in results:
        address = row[0]
        amount = row[1]
        amt_list.append(amount)
        print(f'Address: {address}, Amount: {amount}')
    cursor.close()
    connection.close()
    print(sum(amt_list))
    print(max(amt_list))


def sort_and_merge_addresses(db_name, table_name_read, table_name_write, P2PK: bool):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    connection.create_function("isP2PK", 1, isP2PK)
    cursor = connection.cursor()

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name_write} (
            address TEXT,
            amountComet NUMBER
        )
    ''')

    command1 = f'''
    INSERT INTO {table_name_write} (address, amountComet)
    SELECT address, SUM(amountComet)
    FROM {table_name_read}
    WHERE isP2PK(address) = {int(P2PK)}
    GROUP BY address
    ORDER BY SUM(amountComet) DESC
'''
    cursor.execute(command1)
    connection.commit()
    cursor.close()
    connection.close()


def sort_raw(db_name, table_name_read, table_name_write):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name_write} (
            address TEXT,
            amountComet NUMBER
        )
    ''')

    command1 = f'''
    INSERT INTO {table_name_write} (address, amountComet)
    SELECT address, SUM(amountComet)
    FROM {table_name_read}
    GROUP BY address
    ORDER BY amountComet DESC
'''
    cursor.execute(command1)
    connection.commit()
    cursor.close()
    connection.close()


def get_token_holders(db_name, table_name_sorted_p2pk, least_amount_held):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    cursor.execute(f'''
        SELECT address, amountComet
        FROM {table_name_sorted_p2pk}
        WHERE amountComet >= {least_amount_held}
    ''')
    results = cursor.fetchall()
    res = [row[0] for row in results]
    return res


def get_token_stats(db_name, table_name_raw_sorted, table_name_p2pk_sorted, table_name_script_sorted, amount_top_to_get,
                    amount_to_exclude_for_excluded_avg=5):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    top_raw = []
    top_p2pk = []
    top_script = []

    cursor.execute(f'''
        SELECT * FROM {table_name_raw_sorted}
        LIMIT {amount_top_to_get}
    ''')
    top_raw.extend(cursor.fetchall())

    cursor.execute(f'''
        SELECT * FROM {table_name_p2pk_sorted}
        LIMIT {amount_top_to_get}
    ''')
    top_p2pk.extend(cursor.fetchall())

    cursor.execute(f'''
        SELECT * FROM {table_name_script_sorted}
        LIMIT {amount_top_to_get}
    ''')
    top_script.extend(cursor.fetchall())

    cursor.execute(f'''
        SELECT SUM(amountComet) FROM {table_name_raw_sorted}
    ''')
    total_comet_supply = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT SUM(amountComet) FROM {table_name_p2pk_sorted}
    ''')
    total_comet_supply_p2pk = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT SUM(amountComet) FROM {table_name_script_sorted}
    ''')
    total_comet_supply_script = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT AVG(amountComet) FROM {table_name_raw_sorted}
    ''')
    comet_avg = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT AVG(amountComet) FROM {table_name_p2pk_sorted}
    ''')
    comet_avg_p2pk = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT AVG(amountComet) FROM {table_name_script_sorted}
    ''')
    comet_avg_script = cursor.fetchone()[0]

    try:
        # Select the average of the amountComet column, excluding the top 5 rows
        cursor.execute(f'''
            SELECT * FROM {table_name_raw_sorted}
        ''')

        # Fetch the result
        result = cursor.fetchall()
        res = [row[1] for row in result]
        subset = res[amount_to_exclude_for_excluded_avg:]

    except Exception as e:
        print(f'Error: {e}')

    return top_raw, top_p2pk, top_script, [total_comet_supply, comet_avg, mean(subset)], [total_comet_supply_p2pk,
                                                                                          comet_avg_p2pk], [
               total_comet_supply_script, comet_avg_script]


def deleteTable(db_name, tableName):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    try:
        connection.execute(f'DROP TABLE {tableName}')
    except Exception as e:
        pass
    connection.commit()
    connection.close()


def return_table_values(db_name, tableName):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {tableName}")
    results = cursor.fetchall()
    connection.close()
    return results


def get_nft_id(db_name, index):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    table_name = "nft_ids"
    cursor.execute(f'''
        SELECT id
        FROM {table_name}
        LIMIT 1
        OFFSET {index}
    ''')
    result = cursor.fetchone()
    connection.close()
    if result is not None:
        return result[0]
    return None


def get_all_nft_id(db_name):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    table_name = "nft_ids"
    cursor.execute(f'''
        SELECT id
        FROM {table_name}
    ''')
    results = cursor.fetchall()
    list = [row[0] for row in results]
    cursor.close()
    connection.close()
    return list


def get_all_nft_holders(db_name, complete_data_table_name, black_listed_wallets: [str]):
    connection = sqlite3.connect(r'data/' + f'{db_name}.db')
    cursor = connection.cursor()
    if len(black_listed_wallets) == 1:
        command = f'''
            SELECT address
            FROM {complete_data_table_name}
            WHERE address != '{(black_listed_wallets[0])}'
        '''
    else:
        command = f'''
            SELECT address
            FROM {complete_data_table_name}
            WHERE address NOT IN {tuple(black_listed_wallets)}
        '''
    cursor.execute(command)

    results = cursor.fetchall()
    list = [row[0] for row in results]
    cursor.close()
    connection.close()
    return list


def queryWhitelistTable(query):
    connection = sqlite3.connect(r'data/whitelist_tokenDB.db')
    cursor = connection.cursor()
    sqlite_select_query = """SELECT * from tokens WHERE tokenID = ?"""
    cursor.execute(sqlite_select_query, (query,))
    results = cursor.fetchone()
    connection.close()
    return results
