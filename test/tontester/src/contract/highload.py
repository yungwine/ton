import os
import time
from typing import final, Self
from dataclasses import dataclass

from pytoniq_core import begin_cell, Builder, Cell, StateInit, InternalMsgInfo  # pyright: ignore [reportMissingTypeStubs]
from pytoniq_core.tlb.custom.wallet import (  # pyright: ignore [reportMissingTypeStubs]
    HighloadWalletV3Data,
    WalletMessage,
)

from .contract import Provider
from .crypto import private_key_to_public_key, sign_message
from .wallet import BaseWallet


@final
@dataclass
class HighloadWalletV3(BaseWallet):
    CODE_BOC = "b5ee9c7241021001000228000114ff00f4a413f4bcf2c80b01020120020d02014803040078d020d74bc00101c060b0915be101d0d3030171b0915be0fa4030f828c705b39130e0d31f018210ae42e5a4ba9d8040d721d74cf82a01ed55fb04e030020120050a02027306070011adce76a2686b85ffc00201200809001aabb6ed44d0810122d721d70b3f0018aa3bed44d08307d721d70b1f0201200b0c001bb9a6eed44d0810162d721d70b15800e5b8bf2eda2edfb21ab09028409b0ed44d0810120d721f404f404d33fd315d1058e1bf82325a15210b99f326df82305aa0015a112b992306dde923033e2923033e25230800df40f6fa19ed021d721d70a00955f037fdb31e09130e259800df40f6fa19cd001d721d70a00937fdb31e0915be270801f6f2d48308d718d121f900ed44d0d3ffd31ff404f404d33fd315d1f82321a15220b98e12336df82324aa00a112b9926d32de58f82301de541675f910f2a106d0d31fd4d307d30cd309d33fd315d15168baf2a2515abaf2a6f8232aa15250bcf2a304f823bbf2a35304800df40f6fa199d024d721d70a00f2649130e20e01fe5309800df40f6fa18e13d05004d718d20001f264c858cf16cf8301cf168e1030c824cf40cf8384095005a1a514cf40e2f800c94039800df41704c8cbff13cb1ff40012f40012cb3f12cb15c9ed54f80f21d0d30001f265d3020171b0925f03e0fa4001d70b01c000f2a5fa4031fa0031f401fa0031fa00318060d721d300010f0020f265d2000193d431d19130e272b1fb00b585bf03"

    timeout: int
    wallet_id: int

    @staticmethod
    def create_data_cell(
        public_key: bytes,
        wallet_id: int,
        timeout: int,
        old_queries: dict[int, Cell] | None = None,
        queries: dict[int, Cell] | None = None,
    ) -> Cell:
        return HighloadWalletV3Data(
            wallet_id=wallet_id,
            public_key=public_key,
            last_clean_time=0,
            old_queries=old_queries,
            queries=queries,
            timeout=timeout,
        ).serialize()

    @classmethod
    def from_params(
        cls,
        public_key: bytes,
        wc: int = 0,
        wallet_id: int | None = None,
        timeout: int | None = None,
        private_key: bytes | None = None,
    ) -> Self:
        if wallet_id is None:
            wallet_id = 0x10AD + wc
        if timeout is None:
            timeout = 3600
        data = cls.create_data_cell(public_key=public_key, wallet_id=wallet_id, timeout=timeout)
        state_init = StateInit(code=Cell.one_from_boc(cls.CODE_BOC), data=data)
        address = cls._compute_address(wc, state_init)
        return cls(
            address=address,
            state_init=state_init,
            private_key=private_key,
            wallet_id=wallet_id,
            timeout=timeout,
        )

    @classmethod
    def from_private_key(
        cls,
        private_key: bytes,
        wc: int = 0,
        wallet_id: int | None = None,
        timeout: int | None = None,
    ):
        public_key: bytes = private_key_to_public_key(private_key)
        return cls.from_params(
            public_key=public_key,
            wc=wc,
            wallet_id=wallet_id,
            timeout=timeout,
            private_key=private_key,
        )

    @classmethod
    def create(cls, wc: int = 0, wallet_id: int | None = None, timeout: int | None = None):
        return cls.from_private_key(os.urandom(32), wc, wallet_id, timeout)

    def _pack_actions(
        self,
        messages: list[WalletMessage],
        query_id: int,
    ) -> WalletMessage:
        message_per_pack = 253

        if len(messages) > message_per_pack:
            rest = self._pack_actions(messages[message_per_pack:], query_id)
            messages = messages[:message_per_pack] + [rest]

        amt: int = 0
        list_cell = Cell.empty()

        for msg in messages:
            assert isinstance(msg.message.info, InternalMsgInfo)
            amt += int(msg.message.info.value.grams)
            msg = (
                begin_cell()
                .store_uint(0x0EC3C86D, 32)
                .store_uint(msg.send_mode, 8)
                .store_ref(msg.message.serialize())
                .end_cell()
            )
            list_cell = begin_cell().store_ref(list_cell).store_cell(msg).end_cell()

        # attach some coins for internal message processing gas fees
        fees: int = 7 * 10**6 * len(messages) + 10**7  # 0.007 per message + 0.01 for all
        amt += fees

        return self.create_wallet_internal_message(
            destination=self.address,
            send_mode=3,
            value=amt,
            body=(
                begin_cell()
                .store_uint(0xAE42E5A4, 32)
                .store_uint(query_id, 64)
                .store_ref(list_cell)
                .end_cell()
            ),
        )

    def raw_create_transfer_msg(
        self,
        private_key: bytes,
        wallet_id: int,
        messages: list[WalletMessage],
        query_id: int | None = None,
        timeout: int | None = None,
        created_at: int | None = None,
    ) -> Cell:
        if created_at is None:
            created_at = int(time.time()) - 30
        if query_id is None:
            query_id = created_at % (1 << 23)
        if timeout is None:
            timeout = self.timeout

        assert len(messages) > 0, "messages should not be empty"
        assert len(messages) <= 254 * 254, (
            "for highload v3 wallet maximum messages amount is 254*254"
        )
        assert timeout < (1 << 22), "timeout is too big"
        assert timeout > 5, "timeout is too small"
        assert query_id < (1 << 23), "query id is too big"
        assert created_at > 0, "created_at should be positive"

        if len(messages) == 1 and messages[0].message.init is None:
            msg = messages[0]
        else:
            msg = self._pack_actions(messages, query_id)

        signing_message = (
            begin_cell()
            .store_uint(wallet_id, 32)
            .store_ref(msg.message.serialize())
            .store_uint(msg.send_mode, 8)
            .store_uint(query_id, 23)
            .store_uint(created_at, 64)
            .store_uint(timeout, 22)
            .end_cell()
        )

        signature: bytes = sign_message(signing_message.hash, private_key)
        return Builder().store_bytes(signature).store_ref(signing_message).end_cell()

    async def transfer(
        self,
        provider: Provider,
        msgs: list[WalletMessage],
        query_id: int | None = None,
        timeout: int | None = None,
    ):
        assert self.private_key is not None, "must specify wallet private key!"

        transfer_msg = self.raw_create_transfer_msg(
            private_key=self.private_key,
            wallet_id=self.wallet_id,
            query_id=query_id,
            timeout=timeout,
            messages=msgs,
        )

        return await self.send_external(provider=provider, body=transfer_msg)

    async def send_init_external(self, provider: Provider):
        assert self.state_init is not None, "contract does not have state_init"
        assert self.private_key is not None, "must specify wallet private key!"
        body = self.raw_create_transfer_msg(
            private_key=self.private_key,
            wallet_id=self.wallet_id,
            messages=[
                self.create_wallet_internal_message(destination=self.address, send_mode=3, value=0)
            ],
        )
        return await self.send_external(provider=provider, state_init=self.state_init, body=body)
