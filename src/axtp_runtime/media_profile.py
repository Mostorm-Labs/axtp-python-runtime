from dataclasses import dataclass, field, replace
from enum import Enum
import json
from typing import Optional, Protocol

from .broker import BasicBroker, RpcResponseData
from .generated.axtp_ids_generated import ErrorCode, MethodId, RpcEncoding
from .model import StreamPayload
from .stream import ActiveStream, StreamInfo, StreamRegistry, StreamSink


class MediaKind(str, Enum):
    Video = "video"
    Audio = "audio"


class OpenMode(str, Enum):
    ReceiverPull = "receiver-pull"
    ProducerOpen = "producer-open"
    Both = "both"


@dataclass
class MediaStreamStats:
    video_chunks: int = 0
    audio_chunks: int = 0
    video_bytes: int = 0
    audio_bytes: int = 0
    unknown_chunks: int = 0
    seq_gaps: int = 0
    duplicate_seq: int = 0


@dataclass
class MediaStreamInfo:
    kind: MediaKind = MediaKind.Video
    stream_id: int = 0
    source: str = ""
    codec: str = ""
    stream_profile: str = ""
    cursor_unit: str = ""
    width: int = 0
    height: int = 0
    sample_rate: int = 0
    channels: int = 0
    metadata: dict = field(default_factory=dict)


@dataclass
class ActiveMediaStream:
    kind: MediaKind = MediaKind.Video
    stream_id: int = 0
    source: str = ""


class MediaStreamSink(Protocol):
    def on_stream_opened(self, info: MediaStreamInfo) -> None: ...
    def on_stream_chunk(self, kind: MediaKind, stream: StreamPayload) -> None: ...
    def on_stream_closed(self, kind: MediaKind, stream_id: int) -> None: ...


@dataclass
class OpenStreamResult:
    status: ErrorCode = ErrorCode.Success
    body: dict = field(default_factory=dict)


def receiver_pull_enabled(mode: OpenMode) -> bool:
    return mode in (OpenMode.ReceiverPull, OpenMode.Both)


def producer_open_enabled(mode: OpenMode) -> bool:
    return mode in (OpenMode.ProducerOpen, OpenMode.Both)


class MediaStreamRegistry(StreamSink):
    def __init__(
        self,
        accept_video: bool = True,
        accept_audio: bool = True,
        open_mode: OpenMode = OpenMode.ReceiverPull,
        source: str = "wireless_cast",
        audio_format: str = "adts",
        audio_sample_rate: int = 48000,
        audio_channels: int = 1,
        stream_sink: Optional[MediaStreamSink] = None,
    ) -> None:
        self.accept_video = accept_video
        self.accept_audio = accept_audio
        self.open_mode = open_mode
        self.source = source
        self.audio_format = audio_format
        self.audio_sample_rate = audio_sample_rate
        self.audio_channels = audio_channels
        self.stream_sink = stream_sink
        self._streams = StreamRegistry(stream_sink=self)
        self._stats = MediaStreamStats()
        self._next_video_stream_id = 0x1001
        self._next_audio_stream_id = 0x2001

    def receiver_pull_enabled(self) -> bool:
        return receiver_pull_enabled(self.open_mode)

    def producer_open_enabled(self) -> bool:
        return producer_open_enabled(self.open_mode)

    def media_enabled(self, kind: MediaKind) -> bool:
        return self.accept_video if kind == MediaKind.Video else self.accept_audio

    def source_for(self, kind: MediaKind) -> str:
        if not self.source or self.source == "wireless_cast":
            return "wireless_cast_video" if kind == MediaKind.Video else "wireless_cast_audio"
        return self.source

    def has_open_stream(self, kind: MediaKind, source: str) -> bool:
        return self._streams.has_open_stream(kind.value, source)

    def accept_producer_open(self, kind: MediaKind, params_text: str) -> OpenStreamResult:
        if not self.producer_open_enabled():
            return self._error(ErrorCode.RpcParamInvalid)
        if not self.media_enabled(kind):
            return self._error(ErrorCode.NotSupported)
        params = _parse_object(params_text)
        if params is None:
            return self._error(ErrorCode.RpcParamInvalid)

        source = _json_string_or(params, "source", self.source_for(kind))
        peer_role = _json_string_or(params, "peerRole", "receiver")
        sync_group_id = _json_string_or(params, "syncGroupId", "")
        cast_session_id = _json_string_or(params, "castSessionId", "")
        max_data_size = _json_u32_or(params, "maxDataSize", 0)

        if kind == MediaKind.Video:
            codec = _json_string_or(params, "codec", "h264")
            if codec != "h264":
                return self._error(ErrorCode.MediaCodecUnsupported)
            return self._open_accepted(
                kind,
                self._allocate_stream_id(kind),
                source,
                peer_role,
                "h264",
                "media.video",
                "timestampUs",
                sync_group_id,
                cast_session_id,
                max_data_size,
                {"codecFormat": "annexb", "parameterSetsInKeyFrame": True},
            )

        codec = _json_string_or(params, "codec", "aac")
        if codec != "aac":
            return self._error(ErrorCode.MediaCodecUnsupported)
        transport_format = _json_string_or(params, "transportFormat", self.audio_format or "adts")
        if transport_format != "adts":
            return self._error(ErrorCode.MediaCodecUnsupported)
        return self._open_accepted(
            kind,
            self._allocate_stream_id(kind),
            source,
            peer_role,
            "aac",
            "media.audio",
            "timestampUs",
            sync_group_id,
            cast_session_id,
            max_data_size,
            {
                "transportFormat": transport_format,
                "sampleRate": _json_u32_or(params, "sampleRate", self.audio_sample_rate or 48000),
                "channels": _json_u32_or(params, "channels", self.audio_channels or 1),
            },
        )

    def register_pulled_open(self, kind: MediaKind, response_text: str) -> OpenStreamResult:
        if not self.media_enabled(kind):
            return self._error(ErrorCode.NotSupported)
        result = _parse_object(response_text)
        if result is None:
            return self._error(ErrorCode.RpcPayloadInvalid)
        stream_id = _json_u32_or(result, "streamId", 0)
        if stream_id == 0:
            return self._error(ErrorCode.RpcPayloadInvalid)
        codec = _json_string_or(result, "codec", "h264" if kind == MediaKind.Video else "aac")
        if kind == MediaKind.Video and codec != "h264":
            return self._error(ErrorCode.MediaCodecUnsupported)
        if kind == MediaKind.Audio and (codec != "aac" or _json_string_or(result, "transportFormat", "adts") != "adts"):
            return self._error(ErrorCode.MediaCodecUnsupported)
        extra = dict(result)
        if kind == MediaKind.Audio:
            extra.setdefault("sampleRate", self.audio_sample_rate or 48000)
            extra.setdefault("channels", self.audio_channels or 1)
        return self._open_accepted(
            kind,
            stream_id,
            _json_string_or(result, "source", self.source_for(kind)),
            _json_string_or(result, "peerRole", "transmitter"),
            codec,
            _json_string_or(result, "streamProfile", "media.video" if kind == MediaKind.Video else "media.audio"),
            _json_string_or(result, "cursorUnit", "timestampUs"),
            _json_string_or(result, "syncGroupId", ""),
            _json_string_or(result, "castSessionId", ""),
            _json_u32_or(result, "maxDataSize", 0),
            extra,
        )

    def close(self, kind: MediaKind, params_text: str) -> OpenStreamResult:
        params = _parse_object(params_text)
        if params is None:
            return self._error(ErrorCode.RpcParamInvalid)
        stream_id = _json_u32_or(params, "streamId", 0)
        if stream_id == 0:
            return self._error(ErrorCode.RpcParamMissing)
        already_closed = True
        info = self._streams.find_stream(stream_id)
        if info is not None:
            already_closed = False
            if _kind_from_stream_info(info) != kind:
                return self._error(ErrorCode.StreamNotFound)
            self._streams.close(stream_id)
        return OpenStreamResult(ErrorCode.Success, {"streamId": stream_id, "state": "closed", "alreadyClosed": already_closed})

    def close_local(self, kind: MediaKind, stream_id: int) -> OpenStreamResult:
        return self.close(kind, json.dumps({"streamId": stream_id, "peerRole": "transmitter"}))

    def handle_stream(self, stream: StreamPayload) -> None:
        self._streams.handle_stream(stream)
        stream_stats = self._streams.stats()
        self._stats.unknown_chunks = stream_stats.unknown_chunks
        self._stats.seq_gaps = stream_stats.seq_gaps
        self._stats.duplicate_seq = stream_stats.duplicate_seq

    def stats(self) -> MediaStreamStats:
        stream_stats = self._streams.stats()
        return replace(
            self._stats,
            unknown_chunks=stream_stats.unknown_chunks,
            seq_gaps=stream_stats.seq_gaps,
            duplicate_seq=stream_stats.duplicate_seq,
        )

    def active_stream_count(self) -> int:
        return self._streams.active_stream_count()

    def active_streams_snapshot(self):
        return [
            ActiveMediaStream(_kind_from_name(stream.kind), stream.stream_id, stream.source)
            for stream in self._streams.active_streams_snapshot()
        ]

    def on_stream_opened(self, info: StreamInfo) -> None:
        if self.stream_sink is not None:
            self.stream_sink.on_stream_opened(_to_media_info(info))

    def on_stream_chunk(self, info: StreamInfo, stream: StreamPayload) -> None:
        kind = _kind_from_stream_info(info)
        if kind == MediaKind.Video:
            self._stats.video_chunks += 1
            self._stats.video_bytes += len(stream.data)
        else:
            self._stats.audio_chunks += 1
            self._stats.audio_bytes += len(stream.data)
        if self.stream_sink is not None:
            self.stream_sink.on_stream_chunk(kind, stream)

    def on_stream_closed(self, info: StreamInfo) -> None:
        if self.stream_sink is not None:
            self.stream_sink.on_stream_closed(_kind_from_stream_info(info), info.stream_id)

    def _open_accepted(
        self,
        kind: MediaKind,
        stream_id: int,
        source: str,
        peer_role: str,
        codec: str,
        stream_profile: str,
        cursor_unit: str,
        sync_group_id: str,
        cast_session_id: str,
        max_data_size: int,
        extra: dict,
    ) -> OpenStreamResult:
        body = {
            "streamId": stream_id,
            "state": "streaming",
            "source": source,
            "peerRole": peer_role,
            "codec": codec,
            "streamProfile": stream_profile,
            "cursorUnit": cursor_unit,
        }
        if sync_group_id:
            body["syncGroupId"] = sync_group_id
        if cast_session_id:
            body["castSessionId"] = cast_session_id
        if max_data_size:
            body["maxDataSize"] = max_data_size
        body.update(extra)

        status = self._streams.register_stream(
            StreamInfo(
                stream_id=stream_id,
                kind=kind.value,
                source=str(body.get("source", "")),
                payload_format=str(body.get("codec", "")),
                stream_profile=str(body.get("streamProfile", "")),
                cursor_unit=str(body.get("cursorUnit", "")),
                metadata=dict(body),
            ),
            reject_duplicate_kind_source=True,
        )
        if status != ErrorCode.Success:
            return self._error(status)
        return OpenStreamResult(ErrorCode.Success, body)

    def _allocate_stream_id(self, kind: MediaKind) -> int:
        if kind == MediaKind.Video:
            value = self._next_video_stream_id
            self._next_video_stream_id += 1
            return value
        value = self._next_audio_stream_id
        self._next_audio_stream_id += 1
        return value

    @staticmethod
    def _error(status: ErrorCode) -> OpenStreamResult:
        return OpenStreamResult(status, {})


def install_media_host_handlers(broker: BasicBroker, registry: MediaStreamRegistry) -> None:
    def make_handler(kind: MediaKind, open_: bool):
        def handler(_context, request):
            text = request.body.decode("utf-8") if request.body else ""
            result = registry.accept_producer_open(kind, text) if open_ else registry.close(kind, text)
            return RpcResponseData(
                body=json.dumps(result.body, separators=(",", ":")).encode("utf-8") if result.status == ErrorCode.Success else b"",
                encoding=RpcEncoding.Json,
                override_encoding=True,
                status_code=result.status,
                override_status=True,
            )

        return handler

    broker.register_raw_method(int(MethodId.VideoOpenStream), make_handler(MediaKind.Video, True))
    broker.register_raw_method(int(MethodId.AudioOpenStream), make_handler(MediaKind.Audio, True))
    broker.register_raw_method(int(MethodId.VideoCloseStream), make_handler(MediaKind.Video, False))
    broker.register_raw_method(int(MethodId.AudioCloseStream), make_handler(MediaKind.Audio, False))
    broker.register_stream_handler(lambda _context, stream: registry.handle_stream(stream))


def _parse_object(text: str) -> Optional[dict]:
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _json_string_or(obj: dict, name: str, fallback: str) -> str:
    value = obj.get(name)
    return value if isinstance(value, str) else fallback


def _json_u32_or(obj: dict, name: str, fallback: int) -> int:
    value = obj.get(name)
    return value if isinstance(value, int) and 0 <= value <= 0xFFFFFFFF else fallback


def _kind_from_name(kind: str) -> MediaKind:
    return MediaKind.Audio if kind == MediaKind.Audio.value else MediaKind.Video


def _kind_from_stream_info(info: StreamInfo) -> MediaKind:
    return _kind_from_name(info.kind)


def _to_media_info(info: StreamInfo) -> MediaStreamInfo:
    return MediaStreamInfo(
        kind=_kind_from_stream_info(info),
        stream_id=info.stream_id,
        source=info.source,
        codec=info.payload_format,
        stream_profile=info.stream_profile,
        cursor_unit=info.cursor_unit,
        width=_json_u32_or(info.metadata, "width", _json_u32_or(info.metadata, "codedWidth", 0)),
        height=_json_u32_or(info.metadata, "height", _json_u32_or(info.metadata, "codedHeight", 0)),
        sample_rate=_json_u32_or(info.metadata, "sampleRate", 0),
        channels=_json_u32_or(info.metadata, "channels", 0),
        metadata=dict(info.metadata),
    )
