import json

from axtp_runtime import (
    BasicBroker,
    BrokerTask,
    BrokerTaskType,
    ErrorCode,
    MediaKind,
    MediaStreamRegistry,
    MethodId,
    OpenMode,
    RpcBodyEncoding,
    RpcEncoding,
    RpcOp,
    SourceProtocol,
    StreamPayload,
    default_payload_meta,
    install_media_host_handlers,
    rpc_payload,
)


class RecordingMediaSink:
    def __init__(self):
        self.opened = []
        self.chunks = []
        self.closed = []

    def on_stream_opened(self, info):
        self.opened.append(info)

    def on_stream_chunk(self, kind, stream):
        self.chunks.append((kind, stream))

    def on_stream_closed(self, kind, stream_id):
        self.closed.append((kind, stream_id))


def test_media_profile_opens_producer_video_streams_and_routes_chunks():
    sink = RecordingMediaSink()
    broker = BasicBroker()
    registry = MediaStreamRegistry(open_mode=OpenMode.ProducerOpen, stream_sink=sink)
    install_media_host_handlers(broker, registry)

    open_rpc = rpc_payload(
        encoding=RpcEncoding.Json,
        op=RpcOp.Request,
        request_id=77,
        method_or_event_id=MethodId.VideoOpenStream,
        body_encoding=RpcBodyEncoding.None_,
        meta=default_payload_meta(source_protocol=SourceProtocol.JsonRpc, request_id=77),
        body=b'{"source":"wireless_cast_video","peerRole":"receiver","codec":"h264"}',
    )
    broker.submit(BrokerTask(BrokerTaskType.RpcRequest, rpc=open_rpc))
    broker.poll()

    open_result = broker.poll_result().rpc
    assert open_result.status_code == ErrorCode.Success
    open_body = json.loads(open_result.body)
    assert open_body["streamId"] == 0x1001
    assert open_body["codec"] == "h264"
    assert open_body["codecFormat"] == "annexb"
    assert sink.opened[0].kind == MediaKind.Video

    broker.submit(
        BrokerTask(
            BrokerTaskType.StreamData,
            stream=StreamPayload(
                stream_id=0x1001,
                seq_id=0,
                cursor=1000,
                data=b"\x00\x00\x01\x67\x42",
            ),
        )
    )
    broker.poll()
    assert broker.poll_result() is None
    assert registry.stats().video_chunks == 1
    assert registry.stats().video_bytes == 5
    assert sink.chunks[0][1].stream_id == 0x1001

    close_rpc = rpc_payload(
        encoding=RpcEncoding.Json,
        op=RpcOp.Request,
        request_id=78,
        method_or_event_id=MethodId.VideoCloseStream,
        body_encoding=RpcBodyEncoding.None_,
        body=b'{"streamId":4097,"peerRole":"transmitter"}',
    )
    broker.submit(BrokerTask(BrokerTaskType.RpcRequest, rpc=close_rpc))
    broker.poll()

    close_result = broker.poll_result().rpc
    assert close_result.status_code == ErrorCode.Success
    assert sink.closed == [(MediaKind.Video, 0x1001)]
    assert registry.active_stream_count() == 0
