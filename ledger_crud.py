
from sqlalchemy.future import select
from sqlalchemy import insert
from sqlalchemy.orm import contains_eager
from indexer.database import *
from datetime.datetime import now

async def get_last_seqno(session):
    res = (await session.execute(select(Gram20ProcessingHistory)
                                 .order_by(Gram20ProcessingHistory.seqno.desc()))).first()
    if res:
        return res.seqno
    return None

async def update_processing_history(session, last_seqno, new_seqno, actions):
    processed = int(now())
    last_block = await (session.execute(select(BlockHeader.gen_utime)\
                    .join(BlockHeader.block).options(contains_eager(BlockHeader.block))\
                    .filter(Block.workchain == -1).filter(Block.seqno == last_seqno))).first()
    assert last_block is not None, f"Unable to find last block for {last_seqno}"
    await session.execute(insert(Gram20ProcessingHistory).values(
        seqno=new_seqno,
        processed_time=processed,
        lag=processed - last_block.gen_utime,
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