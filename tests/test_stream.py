from axtp_runtime import ErrorCode, StreamRegistry, StreamInfo, StreamPayload


class RecordingStreamSink:
    def __init__(self):
        self.opened = []
        self.chunks = []
        self.closed = []

    def on_stream_opened(self, info):
        self.opened.append(info)

    def on_stream_chunk(self, info, stream):
        self.chunks.append((info, stream))

    def on_stream_closed(self, info):
        self.closed.append(info)


def chunk(stream_id, seq_id, cursor, size):
    return StreamPayload(stream_id=stream_id, seq_id=seq_id, cursor=cursor, data=bytes([seq_id + 1]) * size)


def test_stream_registry_tracks_lifecycle_stats_and_sequence_anomalies():
    sink = RecordingStreamSink()
    registry = StreamRegistry(stream_sink=sink)
    info = StreamInfo(
        stream_id=0x10,
        kind="file",
        source="firmware.bin",
        stream_profile="file.transfer",
        cursor_unit="offsetBytes",
        payload_format="binary",
        metadata={"sha256": "abc"},
    )

    assert registry.register_stream(info, reject_duplicate_kind_source=True) == ErrorCode.Success
    assert registry.has_stream(0x10)
    assert registry.has_open_stream("file", "firmware.bin")
    assert registry.active_stream_count() == 1
    assert sink.opened == [info]

    assert registry.register_stream(info, reject_duplicate_kind_source=True) == ErrorCode.StreamAlreadyOpen

    registry.handle_stream(chunk(0x10, 0, 0, 3))
    registry.handle_stream(chunk(0x10, 2, 3, 5))
    registry.handle_stream(chunk(0x10, 2, 3, 7))
    registry.handle_stream(chunk(0x99, 0, 0, 11))

    assert registry.stats().chunks == 3
    assert registry.stats().bytes == 15
    assert registry.stats().seq_gaps == 1
    assert registry.stats().duplicate_seq == 1
    assert registry.stats().unknown_chunks == 1
    assert len(sink.chunks) == 3

    assert registry.close(0x10) == info
    assert sink.closed == [info]
    assert registry.active_stream_count() == 0
    assert registry.close(0x10) is None
