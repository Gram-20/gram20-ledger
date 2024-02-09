import codecs
import asyncio
from os import environ
from datetime import datetime

from pytonlib.utils.tlb import parse_transaction
from tvm_valuetypes.cell import deserialize_boc

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from sqlalchemy import Column, String, Integer, BigInteger, Boolean, Index, Enum, Numeric, LargeBinary, SmallInteger
from sqlalchemy import ForeignKey, UniqueConstraint, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy import and_, or_, ColumnDefault
from dataclasses import dataclass, asdict

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

# init database
def get_engine(database):
    connection_url = environ["PGCONNECTION_URL"]
    engine = create_async_engine(connection_url, pool_size=20, max_overflow=10, echo=False)
    return engine

engine = get_engine("ton_index")

SessionMaker = sessionmaker(bind=engine, class_=AsyncSession)

# database
Base = declarative_base()

utils_url = str(engine.url).replace('+asyncpg', '')

async def check_database_inited(url):
    if not database_exists(url):
        return False
    return True

async def init_database(create=False):
    logger.info(f"Create db ${utils_url}")
    logger.info(database_exists(utils_url))
    while not await check_database_inited(utils_url):
        logger.info("Create db")
        if create:
            logger.info('Creating database')
            create_database(utils_url)
        asyncio.sleep(0.5)

    if create:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    logger.info("DB ready")

# ton-indexer entities

@dataclass(init=False)
class Code(Base):
    __tablename__ = 'code'

    hash: str = Column(String, primary_key=True)
    code: str = Column(String)


@dataclass(init=False)
class BlockHeader(Base):
    __tablename__ = 'block_headers'

    block_id: int = Column(Integer, ForeignKey('blocks.block_id'), primary_key=True)
    global_id: int = Column(Integer)
    version: int = Column(Integer)
    flags: int = Column(Integer)
    after_merge: bool = Column(Boolean)
    after_split: bool = Column(Boolean)
    before_split: bool = Column(Boolean)
    want_merge: bool = Column(Boolean)
    validator_list_hash_short: int = Column(Integer)
    catchain_seqno: int = Column(Integer)
    min_ref_mc_seqno: int = Column(Integer)
    is_key_block: bool = Column(Boolean)
    prev_key_block_seqno: int = Column(Integer)
    start_lt: int = Column(BigInteger)
    end_lt: int = Column(BigInteger)
    gen_utime: int = Column(BigInteger)
    vert_seqno: int = Column(Integer)

    block = relationship("Block", backref=backref("block_header", uselist=False))

@dataclass(init=False)
class Message(Base):
    __tablename__ = 'messages'
    msg_id: int = Column(BigInteger, primary_key=True)
    source: str = Column(String)
    destination: str = Column(String)
    value: int = Column(BigInteger)
    fwd_fee: int = Column(BigInteger)
    ihr_fee: int = Column(BigInteger)
    created_lt: int = Column(BigInteger)
    hash: str = Column(String(44))
    body_hash: str = Column(String(44))
    op: int = Column(Integer)
    comment: str = Column(String)
    ihr_disabled: bool = Column(Boolean)
    bounce: bool = Column(Boolean)
    bounced: bool = Column(Boolean)
    import_fee: int = Column(BigInteger)
    created_time: int = Column(BigInteger)

    in_tx_id = Column(BigInteger, ForeignKey("transactions.tx_id"))
    in_tx = relationship("Transaction", back_populates="in_msg", uselist=False, foreign_keys=[in_tx_id])

@dataclass(init=False)
class Accounts(Base):
    __tablename__ = 'accounts'

    address: str = Column(String, primary_key=True)
    first_tx = Column(BigInteger, ForeignKey("transactions.tx_id"))
    code_hash: str = Column(String)
    data: str = Column(String)

@dataclass(init=False)
class Block(Base):
    __tablename__ = 'blocks'
    block_id: int = Column(Integer, autoincrement=True, primary_key=True)

    workchain: int = Column(Integer, nullable=False)
    shard: int = Column(BigInteger)
    seqno: int = Column(Integer)
    root_hash: str = Column(String(44))
    file_hash: str = Column(String(44))
    masterchain_block_id = Column(Integer, ForeignKey('blocks.block_id'))

    shards = relationship("Block",
                          backref=backref('masterchain_block', remote_side=[block_id])
                          )
    
@dataclass(init=False)
class Transaction(Base):
    __tablename__ = 'transactions'

    tx_id: int = Column(BigInteger, autoincrement=True, primary_key=True)
    account: str = Column(String)
    lt: int = Column(BigInteger)
    hash: str = Column(String(44))

    utime: int = Column(BigInteger)
    fee: int = Column(BigInteger)
    storage_fee: int = Column(BigInteger)
    other_fee: int = Column(BigInteger)
    transaction_type = Column(Enum('trans_storage', 'trans_ord', 'trans_tick_tock', \
                                   'trans_split_prepare', 'trans_split_install', 'trans_merge_prepare', 'trans_merge_install', name='trans_type'))
    compute_exit_code: int = Column(Integer)
    compute_gas_used: int = Column(Integer)
    compute_gas_limit: int = Column(Integer)
    compute_gas_credit: int = Column(Integer)
    compute_gas_fees: int = Column(BigInteger)
    compute_vm_steps: int = Column(Integer)
    compute_skip_reason: str = Column(Enum('cskip_no_state', 'cskip_bad_state', 'cskip_no_gas', name='compute_skip_reason_type'))
    action_result_code: int = Column(Integer)
    created_time: int = Column(BigInteger)
    action_total_fwd_fees: int = Column(BigInteger)
    action_total_action_fees: int = Column(BigInteger)

    block_id = Column(Integer, ForeignKey("blocks.block_id"))
    block = relationship("Block", backref="transactions")

    in_msg = relationship("Message", uselist=False, back_populates="in_tx", foreign_keys="Message.in_tx_id")
    
# Gram-20
@dataclass(init=False)
class Gram20Token(Base):
    __tablename__ = 'gram20_token'

    UNLOCK_TYPE_FULL = "unlock_full"
    UNLOCK_TYPE_TIMESTAMP = "unlock_ts"

    id: int = Column(BigInteger, primary_key=True)
    msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    address: str = Column(String)
    data: str = Column(String) # contract data at the moment of successful deploy
    created_lt: int = Column(BigInteger) # tx lt
    utime: int = Column(BigInteger) # tx time
    hash: str = Column(String) # msg hash for explorers

    owner: str = Column(String)
    tick: str = Column(String)
    max_supply: int = Column(Numeric(scale=0))
    supply: int = Column(Numeric(scale=0))
    mint_limit: int = Column(Numeric(scale=0))
    mint_start: int = Column(BigInteger)
    interval: int = Column(BigInteger)
    penalty: int = Column(BigInteger)

    def as_dict(self):
        r = asdict(self)
        del r['id']
        return r

    __table_args__ = (Index('gram20_token_1', 'owner', 'tick'),
                      UniqueConstraint('msg_id'),
                      UniqueConstraint('tick'),
                      UniqueConstraint('address')
                      )

@dataclass(init=False)
class Gram20SupplyHistory(Base):
    __tablename__ = 'gram20_supply_history'

    id: int = Column(BigInteger, primary_key=True)
    tick: str = Column(String)
    seqno: int = Column(BigInteger) # mc seqno
    block_time: int = Column(BigInteger) #
    supply: int = Column(Numeric(scale=0)) # supply after mc children processing

    __table_args__ = (Index('gram20_supply_history_1', 'tick', 'seqno'),
                      )

@dataclass(init=False)
class Gram20Balances(Base):
    __tablename__ = 'gram20_balances'

    id: int = Column(BigInteger, primary_key=True)
    state_id: int = Column(BigInteger, ForeignKey('gram20_ledger.id')) # ledger
    owner: str = Column(String) # balance owner
    tick: str = Column(String) # tick for token
    balance: int = Column(Numeric(scale=0)) # balance

    __table_args__ = (
        UniqueConstraint('tick', 'owner', name='gram20_balances_owner_tick'),
        Index('gram20_balances_top_holders_idx', 'tick', 'balance')
    )

@dataclass(init=False)
class Gram20Ledger(Base):
    __tablename__ = 'gram20_ledger'
    ACTION_TYPE_MINT = 0
    ACTION_TYPE_TRANSFER = 1

    id: int = Column(BigInteger, primary_key=True)
    prev_state: int = Column(BigInteger)
    msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    seqno: int = Column(BigInteger) # mc seqno
    lt: int = Column(BigInteger) # user tx lt
    utime: int = Column(BigInteger) # user tx time
    hash: str = Column(String) # msg hash for explorers

    owner: str = Column(String) # balance owner
    tick: str = Column(String) # tick for token

    balance: int = Column(Numeric(scale=0)) # balance after
    delta: int = Column(Numeric(scale=0)) # delta (positive for increasing, negative for decreasing)
    action: int = Column(SmallInteger) # 0 for mint, 1 for transfer, 2 for premint
    comment: str = Column(String) # for memo purposes
    peer: str = Column(String) # for transfers

    def as_dict(self):
        r = asdict(self)
        del r['id']
        return r

__table_args__ = (Index('gram20_ledger_1', 'owner', 'tick', 'id'),
                  )

@dataclass(init=False)
class Gram20Wallet(Base):
    __tablename__ = 'gram20_wallet'

    id: int = Column(BigInteger, primary_key=True)
    address: str = Column(String)
    owner: str = Column(String)
    tick: str = Column(String)

    def as_dict(self):
        r = asdict(self)
        del r['id']
        return r

    __table_args__ = (UniqueConstraint('address'),
                      )


@dataclass(init=False)
class Gram20ProcessingHistory(Base):
    __tablename__ = 'gram20_processing'

    id: int = Column(BigInteger, primary_key=True)
    seqno: int = Column(BigInteger) # mc seqno
    processed_time: int = Column(BigInteger)
    lag: int = Column(BigInteger)
    actions: int = Column(BigInteger)

    __table_args__ = (Index('gram20_processing_1', 'processed_time'),
                      UniqueConstraint('seqno'),
                      )

@dataclass(init=False)
class Gram20Rejection(Base):
    __tablename__ = 'gram20_rejection'

    id: int = Column(BigInteger, primary_key=True)
    msg_id: int = Column(BigInteger, ForeignKey('messages.msg_id'))
    reason: str = Column(String)
    log: str = Column(String)
    owner: str = Column(String)
    block_time: str = Column(BigInteger)

    __table_args__ = (Index('gram20_rejection_1_1', 'owner'),
                      UniqueConstraint('msg_id'),
                      )
