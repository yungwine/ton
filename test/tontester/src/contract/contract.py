from dataclasses import dataclass
from typing import Self, ClassVar, Protocol

from pytoniq_core import Cell, ExternalMsgInfo, MessageAny, CurrencyCollection, InternalMsgInfo, StateInit  # pyright: ignore [reportMissingTypeStubs]
from tontester.tl import tonlib_api

from .address import SMCAddress


class Provider(Protocol):
    async def raw_send_message(self, serialized_boc: bytes) -> tonlib_api.TypeOk:
        ...


@dataclass
class Contract:
    CODE_BOC: ClassVar[str]
    address: SMCAddress
    state_init: StateInit | None

    @classmethod
    def at(cls, address: SMCAddress) -> "Contract":
        return cls(address=address, state_init=None)

    @staticmethod
    def _compute_address(workchain: int, init: StateInit) -> SMCAddress:
        return SMCAddress((workchain, init.serialize().hash))

    @classmethod
    def from_state_init(cls, workchain: int, state_init: StateInit):
        address = cls._compute_address(workchain, state_init)
        return cls(address=address, state_init=state_init)

    @classmethod
    def from_data(cls, workchain: int, data: Cell) -> Self:
        state_init = StateInit(code=Cell.one_from_boc(cls.CODE_BOC), data=data)
        return cls.from_state_init(workchain=workchain, state_init=state_init)

    @staticmethod
    def create_external_msg(dest: SMCAddress, src: SMCAddress | None = None,
                            import_fee: int = 0, state_init: StateInit | None = None,
                            body: Cell | None = None) -> MessageAny:
        info = ExternalMsgInfo(src, dest, import_fee)  # pyright: ignore [reportArgumentType], src can be None in ext message
        if body is None:
            body = Cell.empty()
        message = MessageAny(info=info, init=state_init, body=body)
        return message

    @staticmethod
    def create_internal_msg(dest: SMCAddress, ihr_disabled: bool = True, bounce: bool | None = None, bounced: bool = False,
                            src: SMCAddress | None = None,
                            value: CurrencyCollection | int = 0, ihr_fee: int = 0, fwd_fee: int = 0,
                            created_lt: int = 0, created_at: int = 0, state_init: StateInit | None = None,
                            body: Cell | None = None) -> MessageAny:
        if isinstance(value, int):
            value = CurrencyCollection(grams=value, other=None)  # pyright: ignore [reportArgumentType]
        if bounce is None:
            bounce = dest.is_bounceable
        info = InternalMsgInfo(ihr_disabled, bounce, bounced, src, dest, value, ihr_fee, fwd_fee, created_lt, created_at)  # pyright: ignore [reportArgumentType], since actually wallet allows None src
        if body is None:
            body = Cell.empty()
        message = MessageAny(info=info, init=state_init, body=body)
        return message

    async def send_external(self, provider: Provider, src: SMCAddress | None = None, import_fee: int = 0,
                            state_init: StateInit | None = None, body: Cell | None = None):
        message = self.create_external_msg(src=src, dest=self.address, import_fee=import_fee,
                                           state_init=state_init, body=body)
        return await provider.raw_send_message(message.serialize().to_boc())
