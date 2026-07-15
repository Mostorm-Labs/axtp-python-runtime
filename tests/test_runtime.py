import json

from axtp_runtime import (
    AxtpClient,
    AxtpEndpoint,
    AxtpServer,
    BasicBroker,
    ByteReader,
    ByteWriter,
    ErrorCode,
    InboundProcessor,
    MethodId,
    MockTransport,
    OutboundProcessor,
    PayloadType,
    RpcBodyEncoding,
    RpcEncoding,
    RpcOp,
    crc16_ccitt_false,
    rpc_payload,
)
from axtp_runtime.broker import BrokerTask, BrokerTaskType
from axtp_runtime.model import PayloadMeta, SourceProtocol, RpcPayload


class CaptureSink:
    def __init__(self):
        self.controls = []
        self.rpcs = []
        self.streams = []

    def on_control(self, payload):
        self.controls.append(payload)

    def on_rpc(self, payload):
        self.rpcs.append(payload)

    def on_stream(self, payload):
        self.streams.append(payload)


def encode_rpc(payload, max_frame_size=4096):
    chunks = []
    outbound = OutboundProcessor(lambda data: chunks.append(data), max_frame_size=max_frame_size)
    outbound.send_rpc_request(payload)
    return b"".join(chunks)


def bridge(left, right):
    moved = 0
    while True:
        chunk = left.try_pop_outgoing()
        if chunk is None:
            return moved
        moved += 1
        right.inject_incoming(chunk)


def test_byte_io_and_crc16():
    writer = ByteWriter()
    writer.write_u8(0x12)
    writer.write_u16(0x3456)
    writer.write_u32(0x789ABCDE)
    writer.write_u64(0x1122334455667788)
    assert writer.bytes() == bytes([
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
    ])
    reader = ByteReader(writer.bytes())
    assert reader.read_u8() == 0x12
    assert reader.read_u16() == 0x3456
    assert reader.read_u32() == 0x789ABCDE
    assert reader.read_u64() == 0x1122334455667788
    assert reader.empty()
    assert crc16_ccitt_false(b"123456789") == 0x29B1


def test_framed_binary_rpc_round_trip_and_crc_drop():
    payload = rpc_payload(
        encoding=RpcEncoding.Json,
        op=RpcOp.Request,
        request_id=7,
        method_or_event_id=MethodId.AudioGetAlgorithmConfig,
        body_encoding=RpcBodyEncoding.None_,
        body=b"{}",
    )
    encoded = encode_rpc(payload)
    sink = CaptureSink()
    inbound = InboundProcessor(sink)
    inbound.on_bytes(encoded[:5])
    assert sink.rpcs == []
    inbound.on_bytes(encoded[5:])
    assert sink.rpcs[0].request_id == 7
    assert sink.rpcs[0].body == b"{}"

    invalid = bytearray(encoded)
    invalid[-1] ^= 0xFF
    inbound.on_bytes(bytes(invalid))
    assert len(sink.rpcs) == 1


def test_endpoint_broker_client_server_json_loop():
    client = AxtpClient()
    server = AxtpServer()
    client_transport = MockTransport()
    server_transport = MockTransport()
    client.attach_transport(client_transport)
    server.attach_transport(server_transport)

    def handler(context, params_json):
        params = json.loads(params_json or "{}")
        return json.dumps({"method": context.method_name, "ok": True, "echo": params}, separators=(",", ":"))

    server.on_json("audio.getAlgorithmConfig", handler)

    def pump():
        bridge(client_transport, server_transport)
        server.poll()
        bridge(server_transport, client_transport)

    response = client.call_json("audio.getAlgorithmConfig", '{"profile":"default"}', pump=pump)
    assert json.loads(response) == {
        "method": "audio.getAlgorithmConfig",
        "ok": True,
        "echo": {"profile": "default"},
    }
    assert client.last_error() == ErrorCode.Success


def test_known_unbound_method_is_not_supported_but_unknown_is_not_found():
    broker = BasicBroker()
    known = RpcPayload(method_or_event_id=MethodId.AudioGetAlgorithmConfig)
    unknown = RpcPayload(method_or_event_id=0xFFFF)
    broker.submit(BrokerTask(BrokerTaskType.RpcRequest, rpc=known))
    broker.submit(BrokerTask(BrokerTaskType.RpcRequest, rpc=unknown))
    broker.poll()
    assert broker.poll_result().rpc.status_code == ErrorCode.NotSupported
    assert broker.poll_result().rpc.status_code == ErrorCode.RpcMethodNotFound


def test_pending_rpc_matching_is_session_scoped():
    from axtp_runtime import AxtpCore
    core = AxtpCore()
    core.expect_rpc_response(7, session_id=1)
    response = RpcPayload(op=RpcOp.RequestResponse, request_id=7,
                          meta=PayloadMeta(source_protocol=SourceProtocol.AxtpV1, session_id=2))
    core.on_rpc(response)
    assert core.try_take_rpc_response(7, session_id=1) is None
    assert core.try_take_rpc_response(7, session_id=2) is None


def test_duplicate_response_does_not_replace_first_resolution():
    from axtp_runtime import AxtpCore
    core = AxtpCore()
    core.expect_rpc_response(9, session_id=3)
    first = RpcPayload(op=RpcOp.RequestResponse, request_id=9, body=b"first",
                       meta=PayloadMeta(source_protocol=SourceProtocol.AxtpV1, session_id=3))
    duplicate = RpcPayload(op=RpcOp.RequestResponse, request_id=9, body=b"duplicate",
                           meta=PayloadMeta(source_protocol=SourceProtocol.AxtpV1, session_id=3))
    core.on_rpc(first)
    core.on_rpc(duplicate)
    assert core.try_take_rpc_response(9, session_id=3) is first
    assert core.try_take_rpc_response(9, session_id=3) is None


def test_omitted_session_takes_only_unambiguous_resolved_response():
    from axtp_runtime import AxtpCore
    core = AxtpCore()
    core.expect_rpc_response(10, session_id=4)
    unique = RpcPayload(op=RpcOp.RequestResponse, request_id=10,
                        meta=PayloadMeta(source_protocol=SourceProtocol.AxtpV1, session_id=4))
    core.on_rpc(unique)
    assert core.try_take_rpc_response(10) is unique

    for sid in (5, 6):
        core.expect_rpc_response(11, session_id=sid)
        core.on_rpc(RpcPayload(op=RpcOp.RequestResponse, request_id=11,
                              meta=PayloadMeta(source_protocol=SourceProtocol.AxtpV1, session_id=sid)))
    assert core.try_take_rpc_response(11) is None
    assert core.try_take_rpc_response(11, session_id=5) is not None
    assert core.try_take_rpc_response(11) is not None


def test_endpoint_rpc_matching_is_session_scoped_end_to_end():
    endpoint = AxtpEndpoint(BasicBroker())
    endpoint.send_rpc_request(RpcPayload(
        op=RpcOp.Request, request_id=8,
        meta=PayloadMeta(source_protocol=SourceProtocol.AxtpV1, session_id=1),
    ))
    wrong_session = RpcPayload(
        op=RpcOp.RequestResponse, request_id=8,
        meta=PayloadMeta(source_protocol=SourceProtocol.AxtpV1, session_id=2),
    )
    endpoint.core().on_rpc(wrong_session)
    assert endpoint.try_take_rpc_response(8, session_id=2) is None
    assert endpoint.try_take_rpc_response(8, session_id=1) is None

    expected = RpcPayload(
        op=RpcOp.RequestResponse, request_id=8,
        meta=PayloadMeta(source_protocol=SourceProtocol.AxtpV1, session_id=1),
    )
    endpoint.core().on_rpc(expected)
    assert endpoint.try_take_rpc_response(8, session_id=1) is not None


def test_business_router_stays_live_across_not_supported_not_found_and_success():
    broker = BasicBroker()
    known_id = MethodId.AudioGetAlgorithmConfig
    broker.submit(BrokerTask(BrokerTaskType.RpcRequest, rpc=RpcPayload(method_or_event_id=known_id)))
    broker.submit(BrokerTask(BrokerTaskType.RpcRequest, rpc=RpcPayload(method_or_event_id=0xFFFF)))
    broker.poll()
    assert broker.poll_result().rpc.status_code == ErrorCode.NotSupported
    assert broker.poll_result().rpc.status_code == ErrorCode.RpcMethodNotFound

    broker.register_json_method(known_id, lambda _context, _params: '{"ok":true}')
    broker.submit(BrokerTask(BrokerTaskType.RpcRequest, rpc=RpcPayload(
        encoding=RpcEncoding.Json, method_or_event_id=known_id, body=b"{}",
    )))
    broker.poll()
    result = broker.poll_result().rpc
    assert result.status_code == ErrorCode.Success
    assert json.loads(result.body) == {"ok": True}
