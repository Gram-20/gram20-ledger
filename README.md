# Gram-20 ledger

Ledger is a special tool to calculate wallet balances for
[Gram-20 protocol](https://docs.gram20.com/).

## Architecture

Ledger is a single thread process which processes
sequentially TON blocks produced by [ton-base-indexer](https://github.com/Gram-20/ton-base-indexer).
Each block processing includes applying all actions 
related to the Gram-20 protocol to the ledger. Ledger process interacts
with [contracts-executor](https://github.com/shuva10v/contracts-executor)
microservice to execute get methods on stored account states.  

![ledger architecture](./Ledger%20architecture.png)

Ledger maintain consistent view of the Gram-20 balances in the DB using following tables:
* gram20_token - list of the tokens (ticks)
* gram20_ledger - main table with all balances deltas. Each action
  (mint or transfer) produces entry in this table. Every entry has 
a link to previous state (if any).
* gram20_balances - latest balances
* gram20_processing - processing log, also stores a checkpoint for
the last processed block seqno
* gram20_rejections - log with messages rejected by the protocol
* gram20_wallet - cached mapping between wallets and user token addresses

## Deployment

1. Fetch [contracts-executor](https://github.com/shuva10v/contracts-executor) submodule:
````sh
git submodule update
````
2. Build docker image:
````sh
docker compose build
````
3. Prepare ``.env`` with configuration params:
````sh
PGCONNECTION_URL=postgresql+asyncpg://username:password@host/ton_index
GRAM20_MASTER=EQBoPPFXQpGIiXQNkJ8DpQANN_OmMimp5dx6lWjRZmvEgZCI
GRAM20_TOKEN_MASTER_CODE_HASH=0NMuF6+rWdiSPJrt5kG6gBnR2Whr8H+RIA+8rRPLHr4=
GRAM20_USER_CODE_HASH=dyuAQbgsro6lYOVo/5b7Rx9HVvLiKYr7HpElGirGYi0=
````
DB connection url must point to already inited DB from [ton-base-indexer](https://github.com/Gram-20/ton-base-indexer/).
``GRAM20_MASTER``, ``GRAM20_TOKEN_MASTER_CODE_HASH`` and 
``GRAM20_USER_CODE_HASH`` are protocol constants.

4. Download dump from [gramscan.org](https://gramscan.org/dumps)
5. Restore dump using [restore_dump.py](./restore_dump.py) (put you DB credentials to the comman below):
````sh
pip3 install loguru psycopg2-binary
PGHOST=localhost PGPASSWORD=password PGUSER=postgres PGDATABASE=ton_index \
  python3 restore_dump.py dump.zip 
````
6. Run images:
````sh
docker compose up -d
````

In case of successful run you will see logs like this:
````sh
gram20-ledger-ledger-1              | 2024-02-09 10:24:17.175 | INFO     | __main__:processig_iteration:120 - MC block 35970745 is not found
gram20-ledger-ledger-1              | 2024-02-09 10:24:20.187 | INFO     | __main__:processig_iteration:115 - Got last processed seqno: 35970744
gram20-ledger-ledger-1              | 2024-02-09 10:24:20.192 | INFO     | __main__:processig_iteration:122 - got block 35970745, generated at 32 s ago
gram20-ledger-ledger-1              | 2024-02-09 10:24:20.194 | INFO     | __main__:processig_iteration:158 - Got 0 actions to process
gram20-ledger-ledger-1              | 2024-02-09 10:24:20.201 | INFO     | __main__:processig_iteration:115 - Got last processed seqno: 35970745
gram20-ledger-ledger-1              | 2024-02-09 10:24:20.202 | INFO     | __main__:processig_iteration:120 - MC block 35970746 is not found
````