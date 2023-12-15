
from sqlalchemy.future import select
from sqlalchemy import insert, update
from sqlalchemy.orm import contains_eager
from indexer.database import *
from datetime import datetime

async def get_last_seqno(session):
    res = (await session.execute(select(Gram20ProcessingHistory)
                                 .order_by(Gram20ProcessingHistory.seqno.desc()))).first()
    if res:
        return res.seqno
    return None

async def get_mc_block_time(session, seqno):
    last_block = await (session.execute(select(BlockHeader.gen_utime) \
                                        .join(BlockHeader.block).options(contains_eager(BlockHeader.block)) \
                                        .filter(Block.workchain == -1).filter(Block.seqno == seqno))).first()
    if last_block is None:
        return None
    else:
        return last_block.gen_utime

async def update_processing_history(session, new_seqno, last_mc_time, actions):
    processed = int(datetime.now())

    await session.execute(insert(Gram20ProcessingHistory).values(
        seqno=new_seqno,
        processed_time=processed,
        lag=processed - last_mc_time,
        actions=actions
    )
    )

async def get_account_info(session, address):
    res = (await session.execute(select(Accounts).filter(Accounts.address == address))).first()
    return res

async def get_code(session, code_hash):
    res = (await session.execute(select(Code.code).filter(Code.hash == code_hash))).first()
    return res


async def get_gram20_wallet(session, address):
    res = (await session.execute(select(Gram20Wallet).filter(Gram20Wallet.address == address))).first()
    return res

async def get_gram20_token(session, address):
    res = (await session.execute(select(Gram20Token).filter(Gram20Token.address == address))).first()
    return res


async def get_last_state(session, address, tick):
    res = (await session.execute(select(Gram20Ledger)
                                 .filter(Gram20Ledger.owner == address)
                                 .filter(Gram20Ledger.tick == tick)
                                 .order_by(Gram20Ledger.lt.desc()))).first()
    # init empty state
    if not res:
        res = Gram20Ledger(
            id=None,
            owner=address,
            tick=tick,
            balance=0
        )