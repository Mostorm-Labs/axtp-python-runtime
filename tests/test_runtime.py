import json

from axtp_runtime import (
    AxtpClient,
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
        0x12, 0x56, 0x34, 0xDE, 0xBC, 0x9A, 0x78,
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
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
        body_encoding=RpcBodyEncoding.RawBytes,
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
