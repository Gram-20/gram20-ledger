import sys
from loguru import logger
from zipfile import ZipFile
import psycopg2
import json

if __name__ == "__main__":
    dump = sys.argv[1]
    logger.info(dump)
    conn = psycopg2.connect()
    logger.warning("Starting dump restore, current data will be destroyed")
    # TODO yes no?
    with ZipFile(dump, "r") as zip_file:
        with conn:
            with conn.cursor() as curs:
                with zip_file.open("state.json") as state:
                    state = json.load(state)
                    seqno = state['seqno']
                    logger.info(f"Restoring dump with seqno {seqno}")
                    curs.execute("delete from gram20_processing")
                    curs.execute("""
                    insert into gram20_processing(seqno, processed_time, lag, actions)
                    values (%s, extract('epoch' from now()), 0, 0)
                    """, (seqno,))

                with zip_file.open("tokens.json") as tokens:
                    logger.info("Removing all tokens")
                    curs.execute("delete from gram20_token")
                    for line in tokens:
                        token = json.loads(line)
                        curs.execute("""
                        insert into gram20_token(address, tick, data, utime, hash, owner, max_supply, 
                        supply, mint_limit, mint_start)
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (token['address'], token['tick'],  token['data'],  token['utime'],
                              token['hash'], token['owner'], token['max_supply'], token['supply'],
                              token['mint_limit'], token['mint_start']))

                with zip_file.open("balances.json") as balances:
                    logger.info("Removing all balances")
                    curs.execute("delete from gram20_ledger")
                    curs.execute("delete from gram20_balances")
                    for line in balances:
                        balance = json.loads(line)

                        curs.execute("""
                        insert into gram20_ledger(owner, tick, balance, seqno, hash, utime, lt)
                        values (%s, %s, %s, %s, %s, %s, %s)
                        returning id
                        """, (balance['address'], balance['tick'],  balance['balance'],  balance['seqno'],
                              balance['hash'], balance['utime'], balance['lt']))
                        state_id = curs.fetchone()[0]

                        curs.execute("""
                        insert into gram20_balances(state_id, owner, tick, balance)
                        values (%s, %s, %s, %s)
                        """, (state_id, balance['address'], balance['tick'], balance['balance']))

            logger.warning("Committing changes")
            conn.commit()
        