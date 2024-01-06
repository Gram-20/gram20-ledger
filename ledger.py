#!/usr/bin/env python

import asyncio
import math
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
GRAM20_MASTER = os.getenv("GRAM20_MASTER")
GRAM20_TOKEN_MASTER_CODE_HASH = os.getenv("GRAM20_TOKEN_MASTER_CODE_HASH")
GRAM20_USER_CODE_HASH = os.getenv("GRAM20_USER_CODE_HASH")
GRAM20_SALE_CONTRACT_CODE_HASH = os.getenv("GRAM20_SALE_CONTRACT_CODE_HASH")

# Contact executor exception - should be treated as a problem
class ExecutorException(Exception):
    def __init__(self, msg):
        self.message = msg

# soft exception - should be logged in rejections log
class ProcessingFailed(Exception):
    def __init__(self, msg, log):
        self.message = msg
        self.log = log

VALID_BASIC_WALLETS = set([
    "oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=", # wallet v1 r1
    "1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=", # wallet v1 r2
    "WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=", # wallet v1 r3
    "XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=", # wallet v2 r1
    "/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=", # wallet v2 r2
    "thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=", # wallet v3 r1
    "hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=", # wallet v3 r2
    "ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=", # wallet v4 r1
    "/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=" # wallet v4 r2
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
                if resp.status != 200:
                    raise ExecutorException("Error during contract executor call: %s" % resp)
                res = await resp.json()
                if res['exit_code'] != 0:
                    raise ExecutorException("Non-zero exit code: %s" % res)
                return res['result']

    async def start_processing(self):
        logger.info("Starting ledger processing!")
        while True:
            try:
                if await self.processig_iteration() < 5:
                    await asyncio.sleep(3)
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
                return 0
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
                        tick = obj.get('tick', None)
                        is_valid = False
                        if op == 'deploy' and msg.source == GRAM20_MASTER:
                            is_valid = True
                        elif op == 'mint':
                            is_valid = await self.validate_mint(conn, msg, tick)
                        elif op == 'transfer':
                            is_valid = await self.validate_transfer(conn, msg, tick)

                        if is_valid:
                            all_actions.append(Gram20Action(
                                source=msg.source,
                                destination=msg.destination,
                                lt=msg.lt,
                                utime=msg.utime,
                                obj=obj,
                                op=op,
                                msg=msg,
                                tick=tick
                            ))
                    except ProcessingFailed as failed:
                        await self.handle_rejection(conn, msg, failed, current_block_time)
                    except ExecutorException as e_e:
                        raise e_e
                    except Exception as p_e:
                        logger.error(f"Failed to parse message {msg.hash} {p_e} {traceback.format_exc()}")
            logger.info(f"Got {len(all_actions)} actions to process")

            inserted_actions = 0
            # process deploy actions
            for action in all_actions:
                if action.op == 'deploy':
                    try:
                        if await self.deploy_token(conn, action, current_seqno, current_block_time):
                            inserted_actions += 1
                    except ProcessingFailed as failed:
                        await self.handle_rejection(conn, action.msg, failed, current_block_time)
            # next sort all actions:
            all_actions = sorted(all_actions, key=lambda action: (action.lt, int.from_bytes(base64.b64decode(action.msg.hash), byteorder='big')))
            self.supply_updates = {}
            for action in all_actions:
                logger.info(f"Applying action {action}")
                try:
                    if action.op == 'mint':
                        if await self.apply_mint(conn, action, current_seqno, current_block_time):
                            inserted_actions +=1
                    elif action.op == 'transfer':
                        if await self.apply_transfer(conn, action, current_seqno, current_block_time):
                            inserted_actions += 1
                except ProcessingFailed as failed:
                    await self.handle_rejection(conn, action.msg, failed, current_block_time)

            await self.update_supply_history(conn, current_seqno, current_block_time)

            # await self.check_premints(conn, current_seqno, current_block_time)
            await update_processing_history(conn, current_seqno, current_block_time, inserted_actions)

            await conn.commit() # finally commit all this stuff
            return time() - current_block_time

    async def handle_rejection(self, conn, msg, err, block_time):
        await conn.execute(insert(Gram20Rejection).values([{
            "msg_id": msg.msg_id,
            "owner": msg.source,
            "reason": err.message,
            'log': err.log,
            'block_time': block_time
        }]))

    def validate_condition(self, condition, message, log):
        if not condition:
            logger.warning(f"Condition not met: {message}: {log}")
            raise ProcessingFailed(message, log)

    async def update_supply_history(self, conn, seqno, block_time):
        if len(self.supply_updates) > 0:
            updates = []
            for tick, supply in self.supply_updates.items():
                updates.append({
                    'tick': tick,
                    'seqno': seqno,
                    'supply': supply,
                    'block_time': block_time
                })
            await conn.execute(insert(Gram20SupplyHistory).values(updates))

    async def apply_mint(self, conn, action, seqno, block_time):
        assert action.op == 'mint'
        minter = action.source
        state = await self.get_last_state(conn, minter, action.tick)
        try:
            repeat = int(action.obj['repeat'])
        except:
            raise ProcessingFailed("wrong_repeat", "wrong repeat: " + action.obj['repeat'])
        try:
            amount = int(action.obj['amt'])# * repeat
        except:
            raise ProcessingFailed("wrong_amt", "wrong_amt: " + action.obj['amt'])
        self.validate_condition(amount > 0, "mint_non_positive", f"Cant mint {amount}")

        token_info = await get_gram20_token_by_tick(conn, action.tick)
        assert token_info is not None # not possible actually
        self.validate_condition(block_time >= token_info.mint_start, "mint_before_start",
                                f"Mint is not started for {token_info.tick}, blocked {minter}")

        self.validate_condition(amount <= token_info.mint_limit, "mint_over_limit",
                                f"Mint attempt over limit ({amount} over {token_info.mint_limit} by {minter} for {action.tick}")
        amount = int(action.obj['amt']) * repeat
        self.validate_condition(amount > 0, "mint_non_positive", f"Cant mint {amount}")

        allowed_to_mint = token_info.max_supply - token_info.supply
        self.validate_condition(allowed_to_mint > 0, "overmint",
                                f"Mint is not possible for {token_info.tick}")

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
            action=Gram20Ledger.ACTION_TYPE_MINT,
            tx_fee=action.msg.fee,
            protocol_fee=action.msg.value - action.msg.fee
        )
        await conn.execute(insert(Gram20Ledger, [new_state.as_dict()]))
        new_supply = token_info.supply + amount
        await conn.execute(update(Gram20Token).where(Gram20Token.id == token_info.id).values(supply=new_supply))
        if new_state.balance > 0 and state.balance == 0: # new holder
            await conn.execute(update(Gram20Token).where(Gram20Token.id == token_info.id)
                               .values(total_holders=token_info.total_holders + 1))

        self.supply_updates[action.tick] = new_supply # track supply per seqno for further actions
        return True

    async def get_last_state(self, conn, sender, tick):
        return await get_last_state(conn, sender, tick)

    async def apply_transfer(self, conn, action, seqno, current_block_time):
        assert action.op == 'transfer'
        # Sun Dec 24 2023 06:00:00 GMT+0000
        self.validate_condition(current_block_time >= 1703397600, "transfer_not_enabled", "Transfers before Sun Dec 24 2023 06UTC")
        # gram_token = await get_gram20_token_by_tick(conn, "gram")
        # self.validate_condition(gram_token.supply >= gram_token.max_supply, "transfers_not_enable",
        #                         f"Transfers not enabled while gram token not fully minted")

        sender = action.source
        state = await self.get_last_state(conn, sender, action.tick)
        try:
            amount = int(action.obj['amt'])
        except:
            raise ProcessingFailed("wrong_amount", f"wrong amount: {action.obj}")

        self.validate_condition(amount > 0, "transfer_non_positive", f"Cant transfer {amount}")
        memo = str(action.obj.get('memo', ''))

        self.validate_condition(amount <= state.balance, "transfer_low_balance", f"Transfer is not possible due to low balance {state.balance}")

        recipient = action.obj['to']
        try:
            recipient = Address(recipient).to_string(1, 1, 1)
        except:
            logger.warning(f"Unable to convert address: {recipient}")
            raise ProcessingFailed("transfer_bad_format", f"Unable to convert address: {recipient}")

        self.validate_condition(recipient != sender, "transfer_to_self_not_permitted", "Transfers to self not permitted")


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
            peer=recipient,
            tx_fee=action.msg.fee,
            protocol_fee=action.msg.value - action.msg.fee
        )

        if new_state_sender.balance == 0 and state.balance > 0: # -1 holder
            token_info = await get_gram20_token_by_tick(conn, action.tick)
            await conn.execute(update(Gram20Token).where(Gram20Token.id == token_info.id)
                               .values(total_holders=token_info.total_holders - 1))

        recipient_state = await self.get_last_state(conn, recipient, action.tick)

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
            peer=sender,
            tx_fee=0,
            protocol_fee=0,
        )
        transfer_out = (await conn.execute(insert(Gram20Ledger, [new_state_sender.as_dict()]).returning(Gram20Ledger.id))).first()
        new_state_sender.id = transfer_out[0]
        transfer_in = (await conn.execute(insert(Gram20Ledger, [new_state_recipient.as_dict()]).returning(Gram20Ledger.id))).first()
        new_state_recipient.id = transfer_in[0]
        for transfer in [new_state_sender, new_state_recipient]:
            try:
                await self.handle_transfer_postprocessing(conn, transfer, seqno, current_block_time)
            except:
                logger.error(f"Failed to handle {transfer}: {traceback.format_exc()}")

        if new_state_recipient.balance > 0 and recipient_state.balance == 0: # +1 holder
            token_info = await get_gram20_token_by_tick(conn, action.tick)
            await conn.execute(update(Gram20Token).where(Gram20Token.id == token_info.id)
                               .values(total_holders=token_info.total_holders + 1))

        return True

    async def handle_transfer_postprocessing(self, conn, transfer: Gram20Ledger, seqno, current_block_time):
        if transfer.delta > 0: # transfer to sale contract
            src_acc = await get_account_info(conn, transfer.owner)
            if src_acc:
                if src_acc.code_hash == GRAM20_SALE_CONTRACT_CODE_HASH:
                    logger.info(f"Detected transfer to our sale contract {transfer.owner}")
                    if await get_sale(conn, transfer.owner):
                        logger.warning("Sale contract already inited, ignoring")
                        return

                    data_cell = Cell.one_from_boc(base64.b64decode(src_acc.data)).begin_parse()
                    created_at = data_cell.read_uint(32)
                    seller_address = data_cell.read_msg_addr().to_string(1, 1, 1)
                    token_amount = data_cell.read_uint(256)
                    status = data_cell.read_uint(8)
                    market_address = data_cell.read_msg_addr().to_string(1, 1, 1)
                    market_fee_nominator = data_cell.read_uint(16)
                    market_fee_denominator = data_cell.read_uint(16)
                    price = data_cell.read_coins()
                    tick = data_cell.read_uint(32)
                    logger.info(f"Parsed from sale contact: crated_at={created_at}, seller_address={seller_address}, "
                                f"tick={tick}, amount={token_amount}, price={price}, status={status}, "
                                f"market_address={market_address}, fee={market_fee_nominator}/{market_fee_denominator}")
                    # TODO validation
                    sale = Gram20Sale(
                        address=transfer.owner,
                        seller=seller_address,
                        buyer=None,
                        tick=transfer.tick, # tick.to_bytes(4).decode("utf-8"), 
                        amount=token_amount,
                        total_cost=price,
                        price_per_unit=1.0 * price / token_amount,
                        transfer_in=transfer.id,
                        transfer_out=None,
                        created_at=current_block_time,
                        closed_at=None,
                        status=0,
                        market_address=market_address,
                        market_fee_nominator=market_fee_nominator,
                        market_fee_denominator=market_fee_denominator
                    )
                    await conn.execute(insert(Gram20Sale, [sale.as_dict()]))

        elif transfer.delta < 0: # transfer from sale contract - check it is exists
            sale = await get_sale(conn, transfer.owner)
            if not sale:
                logger.warning(f"Sale contract not found for {transfer.owner}")
                return
            logger.info(f"Handling transfer from sale contract {sale.address}")
            await conn.execute(update(Gram20Sale).where(Gram20Sale.id == sale.id).values(
                buyer=transfer.peer,
                status=1 if transfer.peer != sale.seller else 2, # TODO - handle memo
                transfer_out=transfer.id,
                closed_at=current_block_time
            ))

    async def check_premints(self, conn, seqno, block_ts):
        for token in (await get_gram20_tokens_for_premint_check(conn)):
            allowed = False
            if token.lock_type == Gram20Token.UNLOCK_TYPE_FULL:
                if token.supply >= token.max_supply:
                    logger.info(f"max supply reached for {token.tick}, preminting {token.premint}")
                    allowed = True
            elif token.lock_type == Gram20Token.UNLOCK_TYPE_TIMESTAMP:
                if block_ts >= token.unlock:
                    logger.info(f"premint unlock time reached for {token.tick}, preminting {token.premint} {block_ts} >= {token.unlock}")
                    allowed = True
            if allowed:
                recipient_state = await self.get_last_state(conn, token.owner, token.tick)

                new_state_recipient = Gram20Ledger(
                    prev_state=recipient_state.id,
                    msg_id=token.msg_id,
                    hash=token.hash, # the same as for deploy
                    seqno=seqno,
                    lt=token.created_lt if recipient_state.lt is None else recipient_state.lt + 1, # if we have lt from prev state, use lt+1, otherwise just lt of token deploy
                    utime=block_ts,
                    owner=token.owner,
                    tick=token.tick,
                    balance=recipient_state.balance + token.premint,
                    delta=token.premint,
                    action=Gram20Ledger.ACTION_TYPE_PREMINT,
                    protocol_fee=0,
                    tx_fee=0
                )
                await conn.execute(insert(Gram20Ledger, [new_state_recipient.as_dict()]))
                # TODO handle total holders
                await conn.execute(update(Gram20Token).where(Gram20Token.id == token.id).values(preminted=True))


    async def deploy_token(self, conn, action: Gram20Action, seqno, block_time):
        acc_state = await get_account_info(conn, action.destination)
        if acc_state is None or not acc_state.data or not acc_state.code_hash:
            raise Exception(f"Unable to get account state for token master {action}")

        self.validate_condition(acc_state.code_hash == GRAM20_TOKEN_MASTER_CODE_HASH, "wrong_token_root_sc",
                                f"Unable to deploy Gram20 token master with code hash {acc_state.code_hash}")
        tick = action.tick
        self.validate_condition(tick and len(tick) == 4, "token_root_bad_tick",
                                f"Unable to deploy Gram20 token for tick {tick}")
        tick_cell = Cell()
        tick_cell.bits.write_string(tick)
        # logger.info(bytes_to_b64str(tick_cell.to_boc(False)))
        real_token_master, = await self._execute(code=self.master_code, data=self.master_data,
                                                method='calculate_root_address', types=['address'],
                                                address=GRAM20_MASTER,
                                                arguments=[bytes_to_b64str(tick_cell.to_boc(False))])
        self.validate_condition(real_token_master == action.destination, "token_root_wrong_address",
                        f"Wrong token master address: {action.destination}, but for tick {tick} it should be {real_token_master}")

        is_inited, = await self._execute(code=self.token_master_code, data=acc_state.data,
                                         address=action.destination,
                                                method='get_root_data', types=['int'], arguments=[])

        assert is_inited == '-1', f"Token {action.destination} is not inited: {is_inited}" # must be always, but who knows..

        _, _, _, token_owner = await self._execute(code=self.token_master_code, data=acc_state.data,
                                                method='get_token_data', types=['int', 'int', 'int', 'address'], arguments=[])

        lock_type = 'none'
        unlock_ts = None
        obj = action.obj
        # if obj['lock_type'] == 'unlock':
        #     if obj['unlock'] == 'full':
        #         lock_type = Gram20Token.UNLOCK_TYPE_FULL
        #     else:
        #         lock_type = Gram20Token.UNLOCK_TYPE_TIMESTAMP
        #         unlock_ts = int(obj['unlock'])

        max_supply = int(obj['max'])
        self.validate_condition(max_supply < 340282366920938463463374607431768211455, "tick_supply_too_large",
                                f"Max supply is too large: {max_supply}")

        if block_time <= 1703872800:
            max_supply = max(4000000, max_supply)
        mint_limit = int(obj['limit'])
        if block_time <= 1703872800:
            if max_supply / mint_limit < 4000000:
                mint_limit = math.ceil(max_supply / 4000000.0)


        token = Gram20Token(
            msg_id=action.msg.msg_id,
            hash=action.msg.hash,
            address=action.destination,
            data=acc_state.data,
            created_lt=action.msg.lt,
            utime=action.msg.utime,
            owner=token_owner,
            tick=tick,
            max_supply=max_supply,
            supply=0, #, int(obj['premint']),
            total_holders=0,
            mint_limit=mint_limit,
            premint=0, #int(obj['premint']),
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
            'supply': token.supply,
            'block_time': block_time
        }))

        return True

    async def validate_mint(self, conn, msg, tick):
        return await self.validate_action(conn, msg, tick, validate_wallet_type=True)

    async def validate_transfer(self, conn, msg, tick):
        return await self.validate_action(conn, msg, tick, validate_wallet_type=False)

    ## Validates sender wallet code and destination wallet
    async def validate_action(self, conn, msg, tick, validate_wallet_type=False):
        if validate_wallet_type:
            src_acc = await get_account_info(conn, msg['source'])
            src_code_hash = None
            if src_acc:
                src_code_hash = src_acc.code_hash
            self.validate_condition(src_code_hash in VALID_BASIC_WALLETS, "basic_walet",
                                    f"Ignoring sender with wrong code_hash: {msg['source']} {src_code_hash}")

        wallet_address = msg['destination']
        gram20_wallet = await get_gram20_wallet(conn, wallet_address)
        if not gram20_wallet:
            logger.warning(f"Gram20 not inited for {wallet_address}")
            dst_acc = await get_account_info(conn, wallet_address)
            self.validate_condition(dst_acc and dst_acc.code_hash and dst_acc.data, "user_wallet_uninit",
                                    f"Gram20 wallet {wallet_address} is not inited")
            self.validate_condition(dst_acc.code_hash == GRAM20_USER_CODE_HASH, "user_wallet_wrong_sc",
                f"Gram20 mint {msg['hash']} has been sent to wrong contract with code hash {dst_acc.code_hash}")

            root_address, owner_address, _, _, _, _ = await self._execute(code=self.user_code,
                                                              data=dst_acc.data,
                                                              address=wallet_address,
                                                              method='get_user_data',
                                                              types=['address', 'address', 'int', 'int', 'int', 'int'])
            gram20_token = await get_gram20_token(conn, root_address)
            self.validate_condition(gram20_token is not None, "token_root_not_deployed",
                                    f"Gram20 token {root_address} is not deployed yet!")

            _, real_wallet_address = await self._execute(code=self.token_master_code, address=root_address,
                                                      data=gram20_token.data, method='get_user_data', types=['address', 'address'], arguments=[owner_address])
            self.validate_condition(real_wallet_address == wallet_address, "user_wallet_wrong_address",
                                    f"Address calculated by {root_address} is {real_wallet_address}, but smart contract address is {msg['destination']}")

            await conn.execute(self.gram20_wallets_t.insert(), [Gram20Wallet(
                address=wallet_address,
                owner=owner_address,
                tick=gram20_token.tick
            ).as_dict()])

            self.validate_condition(owner_address == msg['source'], "user_wallet_wrong_sender",
                                    f"Owner address for {wallet_address} is {owner_address}, but mint has been sent from {msg['source']}")
            self.validate_condition(gram20_token.tick == tick, "wrong_tick",
                                    f"Tick from user wallet is {gram20_token.tick}, but action with tick {tick}")
        else:
            self.validate_condition(gram20_wallet.owner == msg['source'], "user_wallet_wrong_sender",
                                    f"Owner address for {wallet_address} is {gram20_wallet.owner}, but mint has been sent from {msg['source']}")
            self.validate_condition(gram20_wallet.tick == tick, "wrong_tick",
                                    f"Tick from user wallet is {gram20_wallet.tick}, but action with tick {tick}")

        return True

    async def run(self):
        await self.init()
        await self.start_processing()

if __name__ == "__main__":
    ledger = Gram20LedgerUpdater(executor_url=os.getenv("EXECUTOR_URL", "http://localhost:9090/execute"))
    asyncio.run(ledger.run())