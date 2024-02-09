# Gram-20 ledger

Ledger is a special tool to calculate wallet balances for
[Gram-20 protocol](https://docs.gram20.com/).

## Architecture

TODO

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