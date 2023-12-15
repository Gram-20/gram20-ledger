#!/usr/bin/env python

import asyncio
import os
import base64

from loguru import logger

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
    msg: Message


GRAM20_PREFIX = """data:application/json,"""
GRAM20_MASTER = "EQ????"
GRAM20_TOKEN_MASTER_CODE_HASH = "xxx"
GRAM20_USER_CODE_HASH = "xxx"
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

class Gram20Ledger:
    def __init__(self, executor_url):
        self.executor_url = executor_url

    async def init(self):
        await init_database(False)
        meta = Base.metadata
        self.gram20_wallets_t = meta.tables[Gram20Wallet.__tablename__]
        self.gram20_token_t = meta.tables[Gram20Token.__tablename__]
        async with engine.begin() as conn:
            self.user_code = await get_code(conn, GRAM20_USER_CODE_HASH)
            self.master_code = await get_code(conn, GRAM20_MASTER)
            self.master_data = (await get_account_info(conn, GRAM20_MASTER)).data
            self.token_master_code = await get_code(conn, GRAM20_TOKEN_MASTER_CODE_HASH)

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
        try:
            async with engine.begin() as conn:
                last_seqno = await get_last_seqno(conn)
                logger.info(f"Got last processed seqno: {last_seqno}")
                messages = await get_messages_by_masterchain_seqno(conn, last_seqno + 1)
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
                                    msg=msg
                                ))
                        except Exception as p_e:
                            logger.error(f"Failed to parse message {msg.hash} {p_e} {traceback.format_exc()}")
                logger.info(f"Got {len(all_actions)} actions to process")

                inserted_actions = 0
                # process deploy actions
                for action in all_actions:
                    if action.op == 'deploy':
                        if await self.deploy_token(conn, action):
                            inserted_actions += 1
                # next sort all actions:
                all_actions = sorted(all_actions, key=lambda action: (action.lt, int.from_bytes(base64.b64decode(action.msg.hash))) )
                for action in all_actions:
                    logger.info(f"Applying action {action}")
                    if action.op == 'mint':
                        if await self.apply_mint(conn, action):
                            inserted_actions +=1
                    elif action.op == 'transfer':
                        if await self.apply_transfer(conn, action):
                            inserted_actions += 1

                await self.check_premints()
                await update_processing_history(conn, last_seqno, last_seqno + 1, inserted_actions)

                await conn.commit() # finally commit all this stuff

        except Exception as e:
            logger.error(f"Failed to process ledger iteration: {e} {traceback.format_exc()}")

    async def deploy_token(self, conn, action: Gram20Action):
        acc_state = await get_account_info(conn, action.destination)
        if acc_state is None or not acc_state.data or not acc_state.code_hash:
            raise Exception(f"Unable to get account state for token master {action}")
        if acc_state.code_hash != GRAM20_TOKEN_MASTER_CODE_HASH:
            # TODO logging errors
            logger.warning(f"Unable to deploy Gram20 token master with code hash {acc_state.code_hash}")
            return False
        tick = action.obj['tick']
        if not tick or len(tick) != 4:
            logger.warning(f"Unable to deploy Gram20 token for tick {tick}")
            return False
        real_token_master = await self._execute(code=self.master_code, data=self.master_data,
                                                method='get_master_address', types=['address'], arguments=[tick])
        if real_token_master != action.destination:
            logger.warning(f"Wrong token master address: {action.destination}, but for tick {tick} it should be {real_token_master}")
            return False

        token_owner, tick = await self._execute(code=self.token_master_code, data=acc_state.data,
                                                method='get_info', types=['address', 'string'], arguments=[])

        lock_type = 'none'
        unlock_ts = None
        obj = action.obj
        if obj['lock_type'] == 'unlock':
            if obj['unlock'] == 'full':
                lock_type = 'unlock_full'
            else:
                lock_type = 'unlock_ts'
                unlock_ts = int(obj['unlock'])
        token = Gram20Token(
            msg_id=action.msg.msg_id,
            address=action.destination,
            data=acc_state.data,
            created_lt=action.msg.lt,
            utime=action.msg.utime,
            owner=token_owner,
            tick=tick,
            max_supply=int(obj['max']),
            supply=0,
            mint_limit=int(obj['limit']),
            premint=int(obj['premint']),
            lock_type=lock_type,
            unlock=unlock_ts,
            mint_start=int(obj['start']),
            interval=int(obj['interval']),
            penalty=int(obj['penalty'])
        )
        logger.info(f"Saving new token {token}")
        await conn.execute(self.gram20_token_t.insert(), [token])
        return True

    async def validate_mint(self, conn, msg):
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
            )])
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
    ledger = Gram20Ledger(executor_url=os.getenv("EXECUTOR_URL", "http://localhost:9090"))
    asyncio.run(ledger.run())