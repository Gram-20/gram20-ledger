#!/usr/bin/env python

import asyncio
import os
import base64

from loguru import logger
from tonsdk.utils import Address, bytes_to_b64str
from tonsdk.boc import Cell

from indexer.crud import get_messages_by_masterchain_seqno
from indexer.database import init_database, engine
from ledger_crud import *
import json
import aiohttp
import traceback
from dataclasses import dataclass


@dataclass
class Gram20Action:
    source: str
    destination: str
    lt: int
    utime: int
    obj: dict
    tick: str
    msg: Message
    op: str


GRAM20_PREFIX = """data:application/json,"""
GRAM20_MASTER = "EQDvNr9zjQ1nptP0ON3_dPbMLNOoQWSWT4cjr-nl-yIgwEkN"
GRAM20_TOKEN_MASTER_CODE_HASH = "AvzC2WZxNMP0iFeZukQE8yYxgznho0HJdPo1IPFPlCE="
GRAM20_USER_CODE_HASH = "T27PUT+H3P0Vh3rblkXwyXmfLfwxylEwBZuq2UWRdY8="

VALID_BASIC_WALLETS = set([
    "1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=",
    "WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=",
    "XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=",
    "/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=",
    "thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=",
    "hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=",
    "ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=",
    "/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA="
])

class Gram20LedgerUpdater:
    def __init__(self, executor_url):
        self.executor_url = executor_url

    async def init(self):
        await init_database(False)
        meta = Base.metadata
        self.gram20_wallets_t = meta.tables[Gram20Wallet.__tablename__]
        self.gram20_token_t = meta.tables[Gram20Token.__tablename__]
        async with engine.begin() as conn:
            logger.info("Initializing smart contracts codes")
            master_state = await get_account_info(conn, GRAM20_MASTER)
            self.master_data = master_state.data
            assert self.master_data is not None
            
            self.user_code = await get_code(conn, GRAM20_USER_CODE_HASH)
            assert self.user_code is not None
            self.master_code = await get_code(conn, master_state.code_hash)
            assert self.master_code is not None

            self.token_master_code = await get_code(conn, GRAM20_TOKEN_MASTER_CODE_HASH)
            assert self.token_master_code is not None
            logger.info("Smart contract codes have been initialized")

    async def _execute(self, code, data, method, types, address=None, arguments=[]):
        req = {'code': code, 'data': data, 'method': method,
               'expected': types, 'address': address, 'arguments': arguments}
        if address is not None:
            req[address] = address
        async with aiohttp.ClientSession() as session:
            resp = await session.post(self.executor_url, json=req)
            async with resp:
                assert resp.status == 200, "Error during contract executor call: %s" % resp
                res = await resp.json()
                if res['exit_code'] != 0:
                    logger.warning("Non-zero exit code: %s" % res)
                    return None
                return res['result']

    async def start_processing(self):
        logger.info("Starting ledger processing!")
        while True:
            try:
                await self.processig_iteration()
                await asyncio.sleep(4)
            except Exception as e:
                logger.error(f"Failed to process ledger iteration: {e} {traceback.format_exc()}")

    async def processig_iteration(self):
        async with engine.begin() as conn:
            last_seqno = await get_last_seqno(conn)
            logger.info(f"Got last processed seqno: {last_seqno}")
            assert last_seqno is not None
            current_seqno = last_seqno + 1
            current_block_time = await get_mc_block_time(conn, current_seqno)
            if current_block_time is None:
                logger.info(f"MC block {current_seqno} is not found")
                return
            logger.info(f"got block {current_seqno}, generated at {int(time() - current_block_time)} s ago")
            messages = await get_messages_by_masterchain_seqno(conn, current_seqno)
            all_actions = []
            for msg in messages:
                if msg.comment and msg.comment.startswith(GRAM20_PREFIX):
                    try:
                        obj = json.loads(msg.comment[len("data:application/json,"):])
                        if obj['p'] != 'gram-20':
                            continue
                        op = obj['op']
                        is_valid = False
                        if op == 'deploy' and msg.source == GRAM20_MASTER:
                            is_valid = True
                        elif op == 'mint':
                            is_valid = await self.validate_mint(conn, msg)
                        elif op == 'transfer':
                            is_valid = await self.validate_transfer(conn, msg)

                        if is_valid:
                            all_actions.append(Gram20Action(
                                source=msg.source,
                                destination=msg.destination,
                                lt=msg.lt,
                                utime=msg.utime,
                                obj=obj,
                                op=op,
                                msg=msg,
                                tick=obj.get('tick', None)
                            ))
                    except Exception as p_e:
                        logger.error(f"Failed to parse message {msg.hash} {p_e} {traceback.format_exc()}")
            logger.info(f"Got {len(all_actions)} actions to process")

            inserted_actions = 0
            # process deploy actions
            for action in all_actions:
                if action.op == 'deploy':
                    if await self.deploy_token(conn, action, current_seqno):
                        inserted_actions += 1
            # next sort all actions:
            all_actions = sorted(all_actions, key=lambda action: (action.lt, int.from_bytes(base64.b64decode(action.msg.hash), byteorder='big')) )
            self.supply_updates = {}
            for action in all_actions:
                logger.info(f"Applying action {action}")
                if action.op == 'mint':
                    if await self.apply_mint(conn, action, current_seqno):
                        inserted_actions +=1
                elif action.op == 'transfer':
                    if await self.apply_transfer(conn, action, current_seqno):
                        inserted_actions += 1
            await self.update_supply_history(conn, current_seqno)

            await self.check_premints(conn, current_seqno, current_block_time)
            await update_processing_history(conn, current_seqno, current_block_time, inserted_actions)

            await conn.commit() # finally commit all this stuff

    async def update_supply_history(self, conn, seqno):
        if len(self.supply_updates) > 0:
            updates = []
            for tick, supply in self.supply_updates:
                updates.append({
                    'tick': tick,
                    'seqno': seqno,
                    'supply': supply
                })
            await conn.execute(insert(Gram20SupplyHistory).values(updates))

    async def apply_mint(self, conn, action, seqno):
        raise
        # TODO check mint start time
        assert action.op == 'mint'
        minter = action.source
        state = await get_last_state(conn, minter, action.tick)
        amount = int(action.obj['amt'])
        token_info = await get_gram20_token(conn, minter)
        assert token_info is not None # not possible actually
        if amount > token_info.mint_limit:
            logger.warning(f"Mint attempt over limit ({amount} over {token_info.mint_limit} by {minter} for {action.tick}")
            return False
        allowed_to_mint = token_info.max_supply - token_info.supply
        if allowed_to_mint <= 0: # only eq zero, negative is not possible but who knows...
            # TODO - may be store empty mints items to the ledger?
            logger.warning("Mint is not possible for")
            return False
        amount = min(allowed_to_mint, amount) # avoid overmint
        # prev state
        new_state = Gram20Ledger(
            prev_state=state.id,
            msg_id=action.msg.msg_id,
            hash=action.msg.hash,
            seqno=seqno,
            lt=action.lt,
            utime=action.utime,
            owner=minter,
            tick=action.tick,
            balance=state.balance + amount,
            delta=amount,
            action=Gram20Ledger.ACTION_TYPE_MINT
        )
        await conn.execute(insert(Gram20Ledger, [new_state.as_dict()]))
        token_info.supply += amount
        await conn.execute(update(Gram20Token).where(Gram20Token.id == token_info.id).values(supply=token_info.supply))
        self.supply_updates[action.token] = token_info.supply # track supply per seqno for further actions

    async def apply_transfer(self, conn, action, seqno):
        assert action.op == 'transfer'
        sender = action.source
        state = await get_last_state(conn, sender, action.tick)
        amount = int(action.obj['amt'])
        memo = str(action.obj.get('memo', ''))

        if amount > state.balance:
            logger.warning("Transfer is not possible due to low balance")
            return False

        recipient = Address(action.obj['to']).to_string(1, 1, 1)

        new_state_sender = Gram20Ledger(
            prev_state=state.id,
            msg_id=action.msg.msg_id,
            hash=action.msg.hash,
            seqno=seqno,
            lt=action.lt,
            utime=action.utime,
            owner=sender,
            tick=action.tick,
            balance=state.balance - amount,
            delta=-1 * amount,
            action=Gram20Ledger.ACTION_TYPE_TRANSFER,
            comment=memo,
            peer=recipient
        )

        recipient_state = await get_last_state(conn, recipient, action.tick)

        new_state_recipient = Gram20Ledger(
            prev_state=recipient_state.id,
            msg_id=action.msg.msg_id,
            hash=action.msg.hash,
            seqno=seqno,
            lt=action.lt,
            utime=action.utime,
            owner=recipient,
            tick=action.tick,
            balance=recipient_state.balance + amount,
            delta=amount,
            action=Gram20Ledger.ACTION_TYPE_TRANSFER,
            comment=memo,
            peer=sender
        )
        await conn.execute(insert(Gram20Ledger, [new_state_sender.as_dict(), new_state_recipient.as_dict()]))

    async def check_premints(self, conn, seqno, block_ts):
        for token in (await get_gram20_tokens_for_premint_check(conn)):
            allowed = False
            if token.lock_type == Gram20Token.UNLOCK_TYPE_FULL:
                if token.supply >= token.max_supply:
                    logger.info(f"max supply reached for {token.tick}, preminting {token.premint}")
                    allowed = True
            elif token.unlock_type == Gram20Token.UNLOCK_TYPE_TIMESTAMP:
                if block_ts >= token.unlock:
                    logger.info(f"premint unlock time reached for {token.tick}, preminting {token.premint}")
                    allowed = True
            if allowed:
                recipient_state = await get_last_state(conn, token.owner, token.tick)

                new_state_recipient = Gram20Ledger(
                    prev_state=recipient_state.id,
                    msg_id=token.msg_id,
                    hash=token.hash, # the same as for deploy
                    seqno=seqno,
                    lt=token.lt, # there are no lt for this action, so just use from token deploy
                    utime=block_ts,
                    owner=token.owner,
                    tick=token.tick,
                    balance=recipient_state.balance + token.premint,
                    delta=token.premint,
                    action=Gram20Ledger.ACTION_TYPE_PREMINT
                )
                await conn.execute(insert(Gram20Ledger, [new_state_recipient.as_dict()]))
                await conn.execute(update(Gram20Token).where(Gram20Token.id == token.id).values(preminted=True))


    async def deploy_token(self, conn, action: Gram20Action, seqno):
        acc_state = await get_account_info(conn, action.destination)
        if acc_state is None or not acc_state.data or not acc_state.code_hash:
            raise Exception(f"Unable to get account state for token master {action}")
        if acc_state.code_hash != GRAM20_TOKEN_MASTER_CODE_HASH:
            # TODO logging errors
            logger.warning(f"Unable to deploy Gram20 token master with code hash {acc_state.code_hash}")
            return False
        tick = action.tick
        if not tick or len(tick) != 4:
            logger.warning(f"Unable to deploy Gram20 token for tick {tick}")
            return False
        tick_cell = Cell()
        tick_cell.bits.write_string(tick)
        logger.info(bytes_to_b64str(tick_cell.to_boc(False)))
        real_token_master, = await self._execute(code=self.master_code, data=self.master_data,
                                                method='calculate_root_address', types=['address'],
                                                address=GRAM20_MASTER,
                                                arguments=[bytes_to_b64str(tick_cell.to_boc(False))])
        if real_token_master != action.destination:
            logger.warning(f"Wrong token master address: {action.destination}, but for tick {tick} it should be {real_token_master}")
            return False

        is_inited, = await self._execute(code=self.token_master_code, data=acc_state.data,
                                         address=action.destination,
                                                method='get_root_data', types=['int'], arguments=[])
        
        assert is_inited == '-1', f"Token {action.destination} is not inited: {is_inited}" # must be always, but who knows..

        _, _, _, token_owner = await self._execute(code=self.token_master_code, data=acc_state.data,
                                                method='get_token_data', types=['int', 'int', 'int', 'address'], arguments=[])

        lock_type = 'none'
        unlock_ts = None
        obj = action.obj
        if obj['lock_type'] == 'unlock':
            if obj['unlock'] == 'full':
                lock_type = Gram20Token.UNLOCK_TYPE_FULL
            else:
                lock_type = Gram20Token.UNLOCK_TYPE_TIMESTAMP
                unlock_ts = int(obj['unlock'])
        token = Gram20Token(
            msg_id=action.msg.msg_id,
            hash=action.msg.hash,
            address=action.destination,
            data=acc_state.data,
            created_lt=action.msg.lt,
            utime=action.msg.utime,
            owner=token_owner,
            tick=tick,
            max_supply=int(obj['max']),
            supply=int(obj['premint']),
            mint_limit=int(obj['limit']),
            premint=int(obj['premint']),
            lock_type=lock_type,
            unlock=unlock_ts,
            mint_start=int(obj['start']),
            interval=int(obj['interval']),
            penalty=int(obj['penalty']),
            preminted=lock_type == 'none'
        )
        logger.info(f"Saving new token {token}")
        await conn.execute(self.gram20_token_t.insert(), [token.as_dict()])

        await conn.execute(insert(Gram20SupplyHistory).values({
            'tick': tick,
            'seqno': seqno,
            'supply': int(obj['premint'])
        }))


        return True

    async def validate_mint(self, conn, msg):
        raise
        return await self.validate_action(conn, msg, validate_wallet_type=True)

    async def validate_transfer(self, conn, msg):
        return await self.validate_action(conn, msg, validate_wallet_type=False)

    ## Validates sender wallet code and destination wallet
    async def validate_action(self, conn, msg, validate_wallet_type=False):
        if validate_wallet_type:
            src_acc = await get_account_info(conn, msg['source'])
            src_code_hash = None
            if src_acc:
                src_code_hash = src_acc.code_hash
            if src_code_hash not in VALID_BASIC_WALLETS:
                # TODO logging
                logger.warning(f"Ignoring sender with wrong code_hash: {msg['source']} {src_code_hash}")
                return False

        wallet_address = msg['destination']
        gram20_wallet = await get_gram20_wallet(conn, wallet_address)
        if not gram20_wallet:
            logger.warning(f"Gram20 not inited for {wallet_address}")
            dst_acc = await get_account_info(conn, wallet_address)
            if not dst_acc or not dst_acc.code_hash or not dst_acc.data:
                logger.warning(f"Gram20 wallet {wallet_address} is not inited")
                return False
            if dst_acc.code_hash != GRAM20_USER_CODE_HASH:
                logger.warning(f"Gram20 mint {msg['hash']} has been sent to wrong contract with code hash {dst_acc.code_hash}")
                return False
            root_address, owner_address = await self._execute(code=get_code(dst_acc.code_hash), data=dst_acc.data, method='get_info', types=['address', 'address'])
            gram20_token = await get_gram20_token(root_address)
            if gram20_token is None:
                logger.warning(f"Gram20 token {root_address} is not deployed yet!")
                return False

            real_wallet_address = await self._execute(code=self.token_master_code, data=gram20_token.data, method='get_wallet_address', types=['address', 'address'], arguments=[owner_address])
            if real_wallet_address != wallet_address:
                logger.warning(f"Address calculated by {root_address} is {real_wallet_address}, but smart contract address is {msg['destination']}")
                return False
            await conn.execute(self.gram20_wallets_t.insert(), [Gram20Wallet(
                address=wallet_address,
                owner=owner_address,
                tick=gram20_token.tick
            ).as_dict()])
            if owner_address != msg['source']:
                logger.warning(f"Owner address for {wallet_address} is {owner_address}, but mint has been sent from {msg['source']}")
                return False
        else:
            if gram20_wallet.owner_address != msg['source']:
                logger.warning(f"Owner address for {wallet_address} is {gram20_wallet.owner_address}, but mint has been sent from {msg['source']}")
                return False

        return True

    async def run(self):
        await self.init()
        await self.start_processing()

if __name__ == "__main__":
    ledger = Gram20LedgerUpdater(executor_url=os.getenv("EXECUTOR_URL", "http://localhost:9090/execute"))
    asyncio.run(ledger.run())