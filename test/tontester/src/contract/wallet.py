from dataclasses import dataclass

from pytoniq_core import Cell, StateInit, Builder, WalletMessage  # pyright: ignore [reportMissingTypeStubs]

from .contract import Contract
from .address import SMCAddress


@dataclass
class BaseWallet(Contract):
    private_key: bytes | None

    @staticmethod
    def create_wallet_internal_message(
        destination: SMCAddress,
        send_mode: int,
        value: int,
        body: Cell | str | None = None,
        state_init: StateInit | None = None,
    ) -> WalletMessage:
        if isinstance(body, str):
            body = Builder().store_uint(0, 32).store_snake_string(body).end_cell()
        if body is None:
            body = Cell.empty()

        message = Contract.create_internal_msg(
            dest=destination, value=value, body=body, state_init=state_init
        )
        return WalletMessage(send_mode=send_mode, message=message)
