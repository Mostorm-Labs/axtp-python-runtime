from dataclasses import dataclass, field
from enum import IntEnum

from .generated.axtp_ids_generated import (
    ControlOpcode,
    ErrorCode,
    PayloadType,
    RpcBodyEncoding,
    RpcEncoding,
    RpcOp,
)

K_AXTP_STANDARD_MAGIC0 = 0x41
K_AXTP_STANDARD_MAGIC1 = 0x58
K_AXTP_VERSION1 = 0x01
K_STANDARD_FRAME_HEADER_SIZE = 12
K_STANDARD_FRAME_CRC_SIZE = 2
K_CONTROL_PAYLOAD_HEADER_SIZE = 5
K_BINARY_RPC_HEADER_SIZE = 11
K_STREAM_PAYLOAD_HEADER_SIZE = 16
K_RPC_ENCODING_JSON_BINARY_VALUE = 0x04


def rpc_encoding_json_binary() -> RpcEncoding:
    return RpcEncoding(K_RPC_ENCODING_JSON_BINARY_VALUE)


def is_json_binary_rpc_encoding(encoding: RpcEncoding) -> bool:
    return int(encoding) == K_RPC_ENCODING_JSON_BINARY_VALUE


def body_encoding_for_rpc_encoding(encoding: RpcEncoding) -> RpcBodyEncoding:
    return RpcBodyEncoding.Tlv8 if is_json_binary_rpc_encoding(encoding) else RpcBodyEncoding.None_


class SourceProtocol(IntEnum):
    AxtpV1 = 0x01
    JsonRpc = 0x02


@dataclass
class PayloadMeta:
    source_protocol: SourceProtocol = SourceProtocol.AxtpV1
    session_id: int = 0
    request_id: int = 0
    json_sid: str = ""
    json_method_or_event_name: str = ""


@dataclass
class ControlPayload:
    opcode: ControlOpcode = ControlOpcode.Open
    control_id: int = 0
    status_code: ErrorCode = ErrorCode.Success
    meta: PayloadMeta = field(default_factory=PayloadMeta)
    body: bytes = b""


@dataclass
class RpcPayload:
    encoding: RpcEncoding = RpcEncoding.Json
    op: RpcOp = RpcOp.Request
    request_id: int = 0
    method_or_event_id: int = 0
    status_code: ErrorCode = ErrorCode.Success
    body_encoding: RpcBodyEncoding = RpcBodyEncoding.None_
    meta: PayloadMeta = field(default_factory=PayloadMeta)
    body: bytes = b""


@dataclass
class StreamPayload:
    stream_id: int = 0
    seq_id: int = 0
    cursor: int = 0
    meta: PayloadMeta = field(default_factory=PayloadMeta)
    data: bytes = b""


@dataclass
class FrameHeader:
    version: int
    payload_type: PayloadType
    payload_length: int
    source_id: int
    destination_id: int
    message_id: int
    frame_index: int
    frame_count: int


@dataclass
class Frame:
    header: FrameHeader
    payload: bytes
    crc16: int = 0


@dataclass
class Message:
    message_id: int
    payload_type: PayloadType
    body: bytes


def default_payload_meta() -> PayloadMeta:
    return PayloadMeta()


def control_payload(**kwargs: object) -> ControlPayload:
    return ControlPayload(**kwargs)


def rpc_payload(**kwargs: object) -> RpcPayload:
    return RpcPayload(**kwargs)


def stream_payload(**kwargs: object) -> StreamPayload:
    return StreamPayload(**kwargs)
