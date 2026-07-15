from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Dict, List, Optional, Set

from .broker import BrokerResult, BrokerResultType
from .codec import InboundProcessor, OutboundProcessor
from .generated.axtp_ids_generated import ControlOpcode, ErrorCode, RpcOp
from .model import ControlPayload, RpcPayload, SourceProtocol, StreamPayload
from .transport import TransportProfile


class CoreEventType(str, Enum):
    RpcRequest = "rpcRequest"
    RpcEvent = "rpcEvent"
    StreamData = "streamData"
    ControlNotice = "controlNotice"
    ProtocolError = "protocolError"


@dataclass
class CoreEvent:
    type: CoreEventType
    rpc: Optional[RpcPayload] = None
    stream: Optional[StreamPayload] = None
    control: Optional[ControlPayload] = None
    error: Optional[ErrorCode] = None


class PendingCallTable:
    """Thread-safe pending/resolved response table keyed by session and request."""

    def __init__(self) -> None:
        self._pending: Set[tuple[int, int]] = set()
        self._resolved: Dict[tuple[int, int], RpcPayload] = {}
        self._lock = RLock()

    def expect(self, request_id: int, session_id: int = 0) -> None:
        with self._lock:
            self._pending.add((session_id, request_id))

    def resolve_if_pending(self, request_id: int, payload: RpcPayload, session_id: int = 0) -> bool:
        key = (session_id, request_id)
        with self._lock:
            if key not in self._pending:
                return False
            self._pending.remove(key)
            self._resolved[key] = payload
            return True

    def is_pending(self, request_id: int, session_id: int = 0) -> bool:
        with self._lock:
            return (session_id, request_id) in self._pending

    def try_take_resolved(self, request_id: int, session_id: Optional[int] = None) -> Optional[RpcPayload]:
        with self._lock:
            if session_id is not None:
                return self._resolved.pop((session_id, request_id), None)
            matches = [key for key in self._resolved if key[1] == request_id]
            if len(matches) != 1:
                return None
            return self._resolved.pop(matches[0])


class ControlSession:
    def __init__(self) -> None:
        self._open = False

    def handle(self, payload: ControlPayload) -> Optional[ControlPayload]:
        if payload.opcode == ControlOpcode.Open:
            self._open = True
            return ControlPayload(ControlOpcode.Accept, payload.control_id, ErrorCode.Success, payload.meta)
        if payload.opcode == ControlOpcode.Ping:
            return ControlPayload(ControlOpcode.Pong, payload.control_id, ErrorCode.Success, payload.meta)
        if payload.opcode == ControlOpcode.Close:
            self._open = False
            return ControlPayload(ControlOpcode.CloseAck, payload.control_id, ErrorCode.Success, payload.meta)
        return None

    def is_open(self) -> bool:
        return self._open


class AxtpCore:
    def __init__(self) -> None:
        self._events: List[CoreEvent] = []
        self._outbound_bytes: List[bytes] = []
        self._control_session = ControlSession()
        self._pending_calls = PendingCallTable()
        self._transport_profile = TransportProfile()
        self._inbound = InboundProcessor(self)
        self._outbound = OutboundProcessor(lambda data: self._outbound_bytes.append(bytes(data)))

    def configure(self, profile: TransportProfile) -> None:
        self._transport_profile = profile
        self._inbound.set_wire_mode(profile.wire_mode)
        self._outbound.set_wire_mode(profile.wire_mode)
        if profile.preferred_frame_size > 0:
            self._outbound.set_max_frame_size(profile.preferred_frame_size)

    def on_bytes(self, data: bytes) -> None:
        self._inbound.on_bytes(data)

    def on_control(self, payload: ControlPayload) -> None:
        response = self._control_session.handle(payload)
        if response is not None:
            self._outbound.send_control(response)

    def on_rpc(self, payload: RpcPayload) -> None:
        if payload.op == RpcOp.Request:
            self._events.append(CoreEvent(CoreEventType.RpcRequest, rpc=payload))
        elif payload.op == RpcOp.Event:
            self._events.append(CoreEvent(CoreEventType.RpcEvent, rpc=payload))
        elif payload.op == RpcOp.RequestResponse:
            sid = payload.meta.session_id
            resolved = self._pending_calls.resolve_if_pending(payload.request_id, payload, sid)
            if not resolved and payload.meta.source_protocol == SourceProtocol.JsonRpc:
                self._outbound.send_rpc_response(payload)

    def on_stream(self, payload: StreamPayload) -> None:
        self._events.append(CoreEvent(CoreEventType.StreamData, stream=payload))

    def poll_event(self) -> Optional[CoreEvent]:
        if not self._events:
            return None
        return self._events.pop(0)

    def handle_broker_result(self, result: BrokerResult) -> None:
        if result.rpc is None:
            return
        if result.type == BrokerResultType.RpcResponse:
            self._outbound.send_rpc_response(result.rpc)
        elif result.type == BrokerResultType.RpcError:
            self._outbound.send_rpc_error(result.rpc)
        elif result.type == BrokerResultType.Event:
            self._outbound.send_event(result.rpc)

    def expect_rpc_response(self, request_id: int, session_id: int = 0) -> None:
        self._pending_calls.expect(request_id, session_id)

    def try_take_rpc_response(self, request_id: int, session_id: Optional[int] = None) -> Optional[RpcPayload]:
        return self._pending_calls.try_take_resolved(request_id, session_id)

    def try_pop_outbound_bytes(self) -> Optional[bytes]:
        if not self._outbound_bytes:
            return None
        return self._outbound_bytes.pop(0)

    def control_session_open(self) -> bool:
        return self._control_session.is_open()

    def send_rpc_request(self, payload: RpcPayload) -> None:
        self._outbound.send_rpc_request(payload)
