from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional

from .generated.axtp_ids_generated import RpcEncoding

ByteSink = Callable[[bytes], None]


class TransportKind(str, Enum):
    Tcp = "tcp"
    WebSocket = "websocket"
    Hid = "hid"
    Ble = "ble"
    Uart = "uart"
    Mock = "mock"
    Custom = "custom"


class AxtpWireMode(str, Enum):
    FramedBinary = "framedBinary"
    WebSocketJsonRpc = "webSocketJsonRpc"


@dataclass
class TransportProfile:
    kind: TransportKind = TransportKind.Custom
    wire_mode: AxtpWireMode = AxtpWireMode.FramedBinary
    default_rpc_encoding: RpcEncoding = RpcEncoding.Json
    message_oriented: bool = False
    supports_text_message: bool = False
    supports_binary_message: bool = True
    preferred_frame_size: int = 4096


class MockTransport:
    def __init__(self, profile: Optional[TransportProfile] = None) -> None:
        self._sink: Optional[ByteSink] = None
        self._outgoing: List[bytes] = []
        self._open = False
        self._profile = profile or TransportProfile(kind=TransportKind.Mock)

    def bind(self, sink: ByteSink) -> None:
        self._sink = sink

    def open(self) -> None:
        self._open = True

    def close(self) -> None:
        self._open = False

    def is_open(self) -> bool:
        return self._open

    def inject_incoming(self, data: bytes) -> None:
        if self._sink is not None:
            self._sink(bytes(data))

    def send_bytes(self, data: bytes) -> None:
        self._outgoing.append(bytes(data))

    def try_pop_outgoing(self) -> Optional[bytes]:
        if not self._outgoing:
            return None
        return self._outgoing.pop(0)

    def queued_outgoing_count(self) -> int:
        return len(self._outgoing)

    def profile(self) -> TransportProfile:
        return TransportProfile(**self._profile.__dict__)
