
from sqlalchemy.future import select
from sqlalchemy import insert, update, text
from sqlalchemy.dialects.postgresql import insert as insert_pg
from sqlalchemy.orm import contains_eager
from indexer.database import *
from time import time

async def get_last_seqno(session):
    res = (await session.execute(select(Gram20ProcessingHistory)
                                 .order_by(Gram20ProcessingHistory.seqno.desc()))).first()
    if res:
        return res.seqno
    return None

async def get_mc_block_time(session, seqno):
    last_block = (await (session.execute(select(BlockHeader) \
                                        .join(BlockHeader.block).options(contains_eager(BlockHeader.block)) \
                                        .filter(Block.workchain == -1).filter(Block.seqno == seqno)))).first()
    if last_block is None:
        return None
    else:
        return last_block.gen_utime

async def update_processing_history(session, new_seqno, last_mc_time, actions):
    processed = int(time())

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
    if res is not None:
        return res.code
    return res


async def get_gram20_wallet(session, address):
    res = (await session.execute(select(Gram20Wallet).filter(Gram20Wallet.address == address))).first()
    return res

async def get_gram20_token(session, address):
    res = (await session.execute(select(Gram20Token).filter(Gram20Token.address == address))).first()
    return res

async def get_gram20_token_by_tick(session, tick) -> Gram20Token:
    res = (await session.execute(select(Gram20Token).filter(Gram20Token.tick == tick))).first()
    return res

async def get_gram20_tokens_for_premint_check(session):
    res = await session.execute(select(Gram20Token)
                                 .filter(Gram20Token.premint > 0)
                                 .filter(Gram20Token.preminted == False))
    return res.all()


async def get_sale(session, address) -> Gram20Sale:
    res = (await session.execute(select(Gram20Sale).filter(Gram20Sale.address == address))).first()
    return res

async def get_last_state(session, address, tick):
    res = (await session.execute(select(Gram20Ledger)
                                 .filter(Gram20Ledger.owner == address)
                                 .filter(Gram20Ledger.tick == tick)
                                 .order_by(Gram20Ledger.id.desc()))).first()
    # init empty state
    if not res:
        res = Gram20Ledger(
            id=None,
            owner=address,
            tick=tick,
            balance=0
        )
    return res

async def get_missing_contracts(session, code_hash):
    res = await session.execute(text("""
                with x as (
                select  distinct a.address  from gram20_balances cb 
                join accounts a on a.address  = cb."owner" 
                where cb.balance  > 0 and a.code_hash = '%s'
                ), delta as (
                select  * from x
                except select distinct gs.address from gram20_sale gs 
                ) 
                select delta.address from delta
                join gram20_balances cb on cb."owner"  = delta.address
                where cb.balance > 23
            """ % code_hash))
    return res


async def get_transfer_to(session, address) -> Gram20Ledger:
    res = (await session.execute(select(Gram20Ledger)\
                                 .filter(Gram20Ledger.owner == address) \
                                 .filter(Gram20Ledger.action == Gram20Ledger.ACTION_TYPE_TRANSFER) \
                                 .filter(Gram20Ledger.delta > 0) \
                                 )).all()
    return res

async def update_balance(session, owner, tick, balance, state_id):
    await session.execute(insert_pg(Gram20Balances).values(
        state_id=state_id,
        owner=owner,
        tick=tick,
        balance=balance
    ).on_conflict_do_update(
        constraint='gram20_balances_owner_tick',
        set_=dict(balance=balance, state_id=state_id)
    ))