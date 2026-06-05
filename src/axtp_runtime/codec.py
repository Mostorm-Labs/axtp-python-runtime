from typing import Dict, List, Optional, Protocol

from .generated.axtp_ids_generated import (
    ControlOpcode,
    ErrorCode,
    PayloadType,
    RpcBodyEncoding,
    RpcEncoding,
    RpcOp,
)
from .model import (
    K_AXTP_STANDARD_MAGIC0,
    K_AXTP_STANDARD_MAGIC1,
    K_AXTP_VERSION1,
    K_BINARY_RPC_HEADER_SIZE,
    K_CONTROL_PAYLOAD_HEADER_SIZE,
    K_STANDARD_FRAME_CRC_SIZE,
    K_STANDARD_FRAME_HEADER_SIZE,
    K_STREAM_PAYLOAD_HEADER_SIZE,
    ControlPayload,
    Frame,
    FrameHeader,
    Message,
    PayloadMeta,
    RpcPayload,
    StreamPayload,
)
from .transport import AxtpWireMode
from .wire_io import ByteReader, ByteWriter, crc16_ccitt_false


class PayloadSink(Protocol):
    def on_control(self, payload: ControlPayload) -> None:
        ...

    def on_rpc(self, payload: RpcPayload) -> None:
        ...

    def on_stream(self, payload: StreamPayload) -> None:
        ...


class FrameEncoder:
    def encode(self, frame: Frame) -> bytes:
        writer = ByteWriter()
        writer.write_u8(K_AXTP_STANDARD_MAGIC0)
        writer.write_u8(K_AXTP_STANDARD_MAGIC1)
        writer.write_u8(frame.header.version)
        writer.write_u8(int(frame.header.payload_type))
        writer.write_u16(len(frame.payload))
        writer.write_u8(frame.header.source_id)
        writer.write_u8(frame.header.destination_id)
        writer.write_u16(frame.header.message_id)
        writer.write_u8(frame.header.frame_index)
        writer.write_u8(frame.header.frame_count)
        writer.write_bytes(frame.payload)
        writer.write_u16(crc16_ccitt_false(writer.bytes()))
        return writer.take_bytes()


class FrameDecoder:
    def __init__(self, next_sink: "FrameSink") -> None:
        self._next = next_sink
        self._buffer = bytearray()

    def on_bytes(self, data: bytes) -> None:
        self._buffer.extend(data)
        self._parse_loop()

    def _consume(self, count: int) -> None:
        del self._buffer[:count]

    def _resync_to_magic(self) -> None:
        for index in range(max(0, len(self._buffer) - 1)):
            if self._buffer[index] == K_AXTP_STANDARD_MAGIC0 and self._buffer[index + 1] == K_AXTP_STANDARD_MAGIC1:
                if index > 0:
                    self._consume(index)
                return
        if not self._buffer:
            return
        keep = 1 if self._buffer[-1] == K_AXTP_STANDARD_MAGIC0 else 0
        self._consume(len(self._buffer) - keep)

    def _parse_loop(self) -> None:
        while True:
            self._resync_to_magic()
            if len(self._buffer) < K_STANDARD_FRAME_HEADER_SIZE + K_STANDARD_FRAME_CRC_SIZE:
                return
            reader = ByteReader(bytes(self._buffer[:K_STANDARD_FRAME_HEADER_SIZE]))
            reader.read_u8()
            reader.read_u8()
            version = reader.read_u8()
            payload_type = reader.read_u8()
            payload_length = reader.read_u16()
            source_id = reader.read_u8()
            destination_id = reader.read_u8()
            message_id = reader.read_u16()
            frame_index = reader.read_u8()
            frame_count = reader.read_u8()
            if None in (version, payload_type, payload_length, source_id, destination_id, message_id, frame_index, frame_count):
                return
            if version != K_AXTP_VERSION1 or payload_type not in [int(PayloadType.Control), int(PayloadType.Rpc), int(PayloadType.Stream)]:
                self._consume(1)
                continue
            if frame_count == 0 or frame_index >= frame_count:
                self._consume(1)
                continue
            total_size = K_STANDARD_FRAME_HEADER_SIZE + int(payload_length) + K_STANDARD_FRAME_CRC_SIZE
            if len(self._buffer) < total_size:
                return
            frame_bytes = bytes(self._buffer[:total_size])
            expected_crc = ByteReader(frame_bytes[-K_STANDARD_FRAME_CRC_SIZE:]).read_u16()
            actual_crc = crc16_ccitt_false(frame_bytes[:-K_STANDARD_FRAME_CRC_SIZE])
            if expected_crc != actual_crc:
                self._consume(1)
                continue
            self._consume(total_size)
            self._next.on_frame(Frame(
                header=FrameHeader(
                    version=int(version),
                    payload_type=PayloadType(int(payload_type)),
                    payload_length=int(payload_length),
                    source_id=int(source_id),
                    destination_id=int(destination_id),
                    message_id=int(message_id),
                    frame_index=int(frame_index),
                    frame_count=int(frame_count),
                ),
                payload=frame_bytes[K_STANDARD_FRAME_HEADER_SIZE:K_STANDARD_FRAME_HEADER_SIZE + int(payload_length)],
                crc16=int(expected_crc),
            ))


class FrameSink(Protocol):
    def on_frame(self, frame: Frame) -> None:
        ...


class MessageReassembler:
    def __init__(self, next_sink: "MessageSink", max_message_size: int = 1024 * 1024) -> None:
        self._next = next_sink
        self._max_message_size = max_message_size
        self._assemblies: Dict[int, Dict[str, object]] = {}

    def on_frame(self, frame: Frame) -> None:
        if frame.header.frame_count == 1:
            if frame.header.frame_index == 0:
                self._next.on_message(Message(frame.header.message_id, frame.header.payload_type, frame.payload))
            return
        assembly = self._assemblies.setdefault(frame.header.message_id, {
            "payload_type": frame.header.payload_type,
            "frame_count": frame.header.frame_count,
            "total_size": 0,
            "fragments": [None] * frame.header.frame_count,
        })
        if assembly["payload_type"] != frame.header.payload_type or assembly["frame_count"] != frame.header.frame_count:
            self._assemblies.pop(frame.header.message_id, None)
            return
        fragments = assembly["fragments"]
        assert isinstance(fragments, list)
        if frame.header.frame_index >= len(fragments) or fragments[frame.header.frame_index] is not None:
            return
        assembly["total_size"] = int(assembly["total_size"]) + len(frame.payload)
        if int(assembly["total_size"]) > self._max_message_size:
            self._assemblies.pop(frame.header.message_id, None)
            return
        fragments[frame.header.frame_index] = frame.payload
        if any(fragment is None for fragment in fragments):
            return
        self._assemblies.pop(frame.header.message_id, None)
        self._next.on_message(Message(
            frame.header.message_id,
            frame.header.payload_type,
            b"".join(fragment for fragment in fragments if fragment is not None),
        ))


class MessageSink(Protocol):
    def on_message(self, message: Message) -> None:
        ...


class MessageFragmenter:
    def __init__(self, max_frame_size: int = 4096) -> None:
        self._max_frame_size = max_frame_size
        self._next_message_id = 1

    def set_max_frame_size(self, max_frame_size: int) -> None:
        self._max_frame_size = max_frame_size

    def fragment(self, message: Message) -> List[Frame]:
        capacity = max(0, self._max_frame_size - K_STANDARD_FRAME_HEADER_SIZE - K_STANDARD_FRAME_CRC_SIZE)
        message_id = self._take_message_id()
        if capacity == 0 or not message.body:
            return [self._make_frame(message, message_id, 0, 1, b"")]
        frame_count = (len(message.body) + capacity - 1) // capacity
        if frame_count > 255:
            raise ValueError("AXTP message requires more than 255 fragments")
        return [
            self._make_frame(
                message,
                message_id,
                index,
                frame_count,
                message.body[index * capacity:min(len(message.body), (index + 1) * capacity)],
            )
            for index in range(frame_count)
        ]

    def _take_message_id(self) -> int:
        value = self._next_message_id
        self._next_message_id += 1
        if self._next_message_id > 0xFFFF:
            self._next_message_id = 1
        return value

    @staticmethod
    def _make_frame(message: Message, message_id: int, frame_index: int, frame_count: int, payload: bytes) -> Frame:
        return Frame(
            header=FrameHeader(K_AXTP_VERSION1, message.payload_type, len(payload), 0, 0, message_id, frame_index, frame_count),
            payload=payload,
        )


class PayloadDecoder:
    def __init__(self, next_sink: PayloadSink) -> None:
        self._next = next_sink

    def on_message(self, message: Message) -> None:
        if message.payload_type == PayloadType.Control:
            self._decode_control(message)
        elif message.payload_type == PayloadType.Rpc:
            self._decode_rpc(message)
        elif message.payload_type == PayloadType.Stream:
            self._decode_stream(message)

    def _decode_control(self, message: Message) -> None:
        if len(message.body) < K_CONTROL_PAYLOAD_HEADER_SIZE:
            return
        reader = ByteReader(message.body)
        opcode = reader.read_u8()
        control_id = reader.read_u16()
        status_code = reader.read_u16()
        body = reader.read_bytes(reader.remaining())
        if None in (opcode, control_id, status_code, body):
            return
        self._next.on_control(ControlPayload(ControlOpcode(int(opcode)), int(control_id), ErrorCode(int(status_code)), PayloadMeta(), body or b""))

    def _decode_rpc(self, message: Message) -> None:
        if len(message.body) < K_BINARY_RPC_HEADER_SIZE:
            return
        reader = ByteReader(message.body)
        encoding = reader.read_u8()
        op = reader.read_u8()
        request_id = reader.read_u32()
        method_or_event_id = reader.read_u16()
        status_code = reader.read_u16()
        body_encoding = reader.read_u8()
        body = reader.read_bytes(reader.remaining())
        if None in (encoding, op, request_id, method_or_event_id, status_code, body_encoding, body):
            return
        self._next.on_rpc(RpcPayload(
            encoding=RpcEncoding(int(encoding)),
            op=RpcOp(int(op)),
            request_id=int(request_id),
            method_or_event_id=int(method_or_event_id),
            status_code=ErrorCode(int(status_code)),
            body_encoding=RpcBodyEncoding(int(body_encoding)),
            meta=PayloadMeta(request_id=int(request_id)),
            body=body or b"",
        ))

    def _decode_stream(self, message: Message) -> None:
        if len(message.body) < K_STREAM_PAYLOAD_HEADER_SIZE:
            return
        reader = ByteReader(message.body)
        stream_id = reader.read_u32()
        seq_id = reader.read_u32()
        cursor = reader.read_u64()
        data = reader.read_bytes(reader.remaining())
        if None in (stream_id, seq_id, cursor, data):
            return
        self._next.on_stream(StreamPayload(int(stream_id), int(seq_id), int(cursor), PayloadMeta(), data or b""))


class PayloadEncoder:
    def encode_control(self, payload: ControlPayload) -> Message:
        writer = ByteWriter()
        writer.write_u8(int(payload.opcode))
        writer.write_u16(payload.control_id)
        writer.write_u16(int(payload.status_code))
        writer.write_bytes(payload.body)
        return Message(0, PayloadType.Control, writer.take_bytes())

    def encode_rpc(self, payload: RpcPayload) -> Message:
        writer = ByteWriter()
        writer.write_u8(int(payload.encoding))
        writer.write_u8(int(payload.op))
        writer.write_u32(payload.request_id)
        writer.write_u16(payload.method_or_event_id)
        writer.write_u16(int(payload.status_code))
        writer.write_u8(int(payload.body_encoding))
        writer.write_bytes(payload.body)
        return Message(0, PayloadType.Rpc, writer.take_bytes())

    def encode_stream(self, payload: StreamPayload) -> Message:
        writer = ByteWriter()
        writer.write_u32(payload.stream_id)
        writer.write_u32(payload.seq_id)
        writer.write_u64(payload.cursor)
        writer.write_bytes(payload.data)
        return Message(0, PayloadType.Stream, writer.take_bytes())


class InboundProcessor:
    def __init__(self, payload_sink: PayloadSink) -> None:
        self._payload_decoder = PayloadDecoder(payload_sink)
        self._reassembler = MessageReassembler(self._payload_decoder)
        self._frame_decoder = FrameDecoder(self._reassembler)
        self._wire_mode = AxtpWireMode.FramedBinary

    def set_wire_mode(self, wire_mode: AxtpWireMode) -> None:
        self._wire_mode = wire_mode

    def on_bytes(self, data: bytes) -> None:
        if self._wire_mode == AxtpWireMode.FramedBinary:
            self._frame_decoder.on_bytes(data)


class OutboundProcessor:
    def __init__(self, write_bytes, max_frame_size: int = 4096) -> None:
        self._write_bytes = write_bytes
        self._payload_encoder = PayloadEncoder()
        self._frame_encoder = FrameEncoder()
        self._fragmenter = MessageFragmenter(max_frame_size)
        self._wire_mode = AxtpWireMode.FramedBinary

    def set_wire_mode(self, wire_mode: AxtpWireMode) -> None:
        self._wire_mode = wire_mode

    def set_max_frame_size(self, max_frame_size: int) -> None:
        self._fragmenter.set_max_frame_size(max_frame_size)

    def send_control(self, payload: ControlPayload) -> None:
        self._send_message(self._payload_encoder.encode_control(payload))

    def send_rpc_request(self, payload: RpcPayload) -> None:
        payload.op = RpcOp.Request
        self._send_message(self._payload_encoder.encode_rpc(payload))

    def send_rpc_response(self, payload: RpcPayload) -> None:
        payload.op = RpcOp.RequestResponse
        self._send_message(self._payload_encoder.encode_rpc(payload))

    def send_rpc_error(self, payload: RpcPayload) -> None:
        payload.op = RpcOp.RequestResponse
        self._send_message(self._payload_encoder.encode_rpc(payload))

    def send_event(self, payload: RpcPayload) -> None:
        payload.op = RpcOp.Event
        self._send_message(self._payload_encoder.encode_rpc(payload))

    def send_stream(self, payload: StreamPayload) -> None:
        self._send_message(self._payload_encoder.encode_stream(payload))

    def _send_message(self, message: Message) -> None:
        if self._wire_mode != AxtpWireMode.FramedBinary:
            return
        for frame in self._fragmenter.fragment(message):
            self._write_bytes(self._frame_encoder.encode(frame))
