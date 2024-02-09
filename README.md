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
3. Configure DB connection in ``.env``. DB connection
must point to already inited DB from [ton-base-indexer](https://github.com/Gram-20/ton-base-indexer/).
4. Download dump from [gramscan.org](https://gramscan.org/dumps)
5Restore dump using [restore_dump.py](./restore_dump.py) (put you DB credentials to the comman below):
````sh
pip3 install loguru psycopg2-binary
PGHOST=localhost PGPASSWORD=password PGUSER=postgres PGDATABASE=ton_index \
  python3 restore_dump.py dump.zip 
````
6. Run images:
````sh
docker compose up -d
````