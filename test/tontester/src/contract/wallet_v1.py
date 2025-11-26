import os
from dataclasses import dataclass
from typing import final, Self

from pytoniq_core import Cell, StateInit, Builder, WalletMessage, begin_cell  # pyright: ignore [reportMissingTypeStubs]

from .crypto import private_key_to_public_key, sign_message
from .wallet import BaseWallet
from .contract import Provider


@final
@dataclass
class WalletV1(BaseWallet):
    CODE_BOC = "b5ee9c7201010101004e000098ff0020dd2082014c97ba9730ed44d0d70b1fe0a4f260810200d71820d70b1fed44d0d31fd3ffd15112baf2a122f901541044f910f2a2f80001d31f31d307d4d101fb00a4c8cb1fcbffc9ed54"

    @staticmethod
    def create_data_cell(seqno: int, public_key: bytes) -> Cell:
        return begin_cell().store_uint(seqno, 32).store_bytes(public_key).end_cell()

    @classmethod
    def from_params(
        cls, public_key: bytes, wc: int = 0, private_key: bytes | None = None
    ) -> "Self":
        data = cls.create_data_cell(public_key=public_key, seqno=0)
        state_init = StateInit(code=Cell.one_from_boc(cls.CODE_BOC), data=data)
        address = cls._compute_address(wc, state_init)
        return cls(address=address, state_init=state_init, private_key=private_key)

    @classmethod
    def from_private_key(cls, private_key: bytes, wc: int = 0) -> Self:
        public_key: bytes = private_key_to_public_key(private_key)
        return cls.from_params(public_key=public_key, wc=wc, private_key=private_key)

    @classmethod
    def create(cls, wc: int = 0) -> Self:
        return cls.from_private_key(os.urandom(32), wc)

    @staticmethod
    def raw_create_transfer_msg(private_key: bytes, seqno: int, message: WalletMessage) -> Cell:
        signing_message = (
            Builder().store_uint(seqno, 32).store_cell(message.serialize())
        ).end_cell()
        signature = sign_message(signing_message.hash, private_key)
        return Builder().store_bytes(signature).store_cell(signing_message).end_cell()

    async def transfer(self, provider: Provider, seqno: int, message: WalletMessage):
        assert self.private_key is not None, "must specify wallet private key!"
        transfer_msg = self.raw_create_transfer_msg(
            private_key=self.private_key, seqno=seqno, message=message
        )
        return await self.send_external(provider=provider, body=transfer_msg)

    async def send_init_external(self, provider: Provider):
        assert self.state_init is not None, "contract does not have state_init"
        assert self.private_key is not None, "must specify wallet private key!"
        body = self.raw_create_transfer_msg(
            private_key=self.private_key,
            seqno=0,
            message=self.create_wallet_internal_message(
                destination=self.address, send_mode=3, value=0
            ),
        )
        return await self.send_external(provider=provider, state_init=self.state_init, body=body)
