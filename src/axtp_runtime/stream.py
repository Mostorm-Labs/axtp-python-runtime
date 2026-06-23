from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Protocol

from .generated.axtp_ids_generated import ErrorCode
from .model import StreamPayload


@dataclass
class StreamInfo:
    stream_id: int = 0
    kind: str = ""
    source: str = ""
    stream_profile: str = ""
    cursor_unit: str = ""
    payload_format: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class ActiveStream:
    stream_id: int = 0
    kind: str = ""
    source: str = ""
    stream_profile: str = ""


@dataclass
class StreamStats:
    chunks: int = 0
    bytes: int = 0
    unknown_chunks: int = 0
    seq_gaps: int = 0
    duplicate_seq: int = 0


class StreamSink(Protocol):
    def on_stream_opened(self, info: StreamInfo) -> None: ...
    def on_stream_chunk(self, info: StreamInfo, stream: StreamPayload) -> None: ...
    def on_stream_closed(self, info: StreamInfo) -> None: ...


@dataclass
class _StreamContext:
    info: StreamInfo
    expected_seq: int = 0
    has_seq: bool = False
    chunks: int = 0
    bytes: int = 0


def to_hex_u32(value: int) -> str:
    return f"0x{value & 0xFFFFFFFF:08X}"


class StreamRegistry:
    def __init__(self, stream_sink: Optional[StreamSink] = None) -> None:
        self._stream_sink = stream_sink
        self._streams: Dict[int, _StreamContext] = {}
        self._stats = StreamStats()

    @staticmethod
    def should_log_chunk_count(count: int) -> bool:
        return count <= 50 or count % 100 == 0

    def has_open_stream(self, kind: str, source: str) -> bool:
        return any(context.info.kind == kind and context.info.source == source for context in self._streams.values())

    def has_stream(self, stream_id: int) -> bool:
        return stream_id in self._streams

    def find_stream(self, stream_id: int) -> Optional[StreamInfo]:
        context = self._streams.get(stream_id)
        return None if context is None else _clone_info(context.info)

    def register_stream(self, info: StreamInfo, reject_duplicate_kind_source: bool = True) -> ErrorCode:
        if info.stream_id == 0:
            return ErrorCode.StreamIdInvalid
        if not info.kind:
            return ErrorCode.StreamPayloadInvalid
        if info.stream_id in self._streams:
            return ErrorCode.StreamAlreadyOpen
        if reject_duplicate_kind_source and self.has_open_stream(info.kind, info.source):
            return ErrorCode.StreamAlreadyOpen

        stored = _clone_info(info)
        self._streams[stored.stream_id] = _StreamContext(stored)
        if self._stream_sink is not None:
            self._stream_sink.on_stream_opened(_clone_info(stored))
        return ErrorCode.Success

    def close(self, stream_id: int) -> Optional[StreamInfo]:
        context = self._streams.pop(stream_id, None)
        if context is None:
            return None
        info = _clone_info(context.info)
        if self._stream_sink is not None:
            self._stream_sink.on_stream_closed(info)
        return info

    def handle_stream(self, stream: StreamPayload) -> None:
        context = self._streams.get(stream.stream_id)
        if context is None:
            self._stats.unknown_chunks += 1
            return

        if context.has_seq:
            if stream.seq_id == context.expected_seq - 1:
                self._stats.duplicate_seq += 1
            elif stream.seq_id != context.expected_seq:
                self._stats.seq_gaps += 1
        context.has_seq = True
        context.expected_seq = (stream.seq_id + 1) & 0xFFFFFFFF
        context.chunks += 1
        context.bytes += len(stream.data)
        self._stats.chunks += 1
        self._stats.bytes += len(stream.data)
        if self._stream_sink is not None:
            self._stream_sink.on_stream_chunk(_clone_info(context.info), stream)

    def stats(self) -> StreamStats:
        return replace(self._stats)

    def active_stream_count(self) -> int:
        return len(self._streams)

    def active_streams_snapshot(self) -> List[ActiveStream]:
        return [
            ActiveStream(context.info.stream_id, context.info.kind, context.info.source, context.info.stream_profile)
            for context in self._streams.values()
        ]


def _clone_info(info: StreamInfo) -> StreamInfo:
    return replace(info, metadata=dict(info.metadata))
