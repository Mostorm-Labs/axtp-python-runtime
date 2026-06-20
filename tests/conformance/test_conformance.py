import json
import os
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Callable, List, Optional

from axtp_runtime import (
    AxtpCore,
    AxtpEndpoint,
    BasicBroker,
    CapabilityId,
    ControlOpcode,
    ControlPayload,
    ErrorCode,
    EventId,
    InboundProcessor,
    MethodId,
    MockTransport,
    OutboundProcessor,
    PayloadMeta,
    RpcBodyEncoding,
    RpcEncoding,
    RpcOp,
    RpcPayload,
    SourceProtocol,
    StreamPayload,
)
from axtp_runtime.generated.axtp_generated_version import AXTP_GENERATED_VERSION
from axtp_runtime.generated.registry import (
    K_CAPABILITY_REGISTRY,
    K_METHOD_REGISTRY,
    RegistryLookup,
)


@dataclass
class CaseResult:
    id: str
    level: str
    requirement: str
    status: str
    message: str = ""
    duration_ms: float = 0.0

    def to_json(self):
        return {
            "id": self.id,
            "level": self.level,
            "requirement": self.requirement,
            "status": self.status,
            "durationMs": self.duration_ms,
            "message": self.message,
        }


cases: List[CaseResult] = [
    CaseResult("handshake.open_accept", "framed-binary", "required", "pending"),
    CaseResult(
        "handshake.open_reject",
        "framed-binary",
        "not-selected",
        "skipped",
        "control open rejection policy is not part of the v1 framed-binary required set",
    ),
    CaseResult("handshake.close", "framed-binary", "required", "pending"),
    CaseResult("handshake.ping_pong", "framed-binary", "required", "pending"),
    CaseResult(
        "session.hello_identify_identified",
        "websocket-jsonrpc",
        "unsupported",
        "unsupported",
        "Python runtime does not implement WebSocket JSON-RPC wire mode",
    ),
    CaseResult(
        "session.request_before_identified",
        "websocket-jsonrpc",
        "unsupported",
        "unsupported",
        "Python runtime does not implement WebSocket JSON-RPC wire mode",
    ),
    CaseResult("rpc.request_response_json", "core", "required", "pending"),
    CaseResult("rpc.method_not_found", "core", "required", "pending"),
    CaseResult(
        "rpc.invalid_params",
        "core",
        "not-selected",
        "skipped",
        "schema-aware parameter validation is outside the required Python core profile",
    ),
    CaseResult("rpc.request_id_match", "core", "required", "pending"),
    CaseResult(
        "event.subscribe_event",
        "event",
        "optional",
        "unsupported",
        "event subscription intent requires WebSocket JSON-RPC",
    ),
    CaseResult(
        "event.unsubscribe_event",
        "event",
        "optional",
        "unsupported",
        "event subscription intent requires WebSocket JSON-RPC",
    ),
    CaseResult("event.emit_event", "event", "optional", "pending"),
    CaseResult("capability.get_all", "capability", "optional", "pending"),
    CaseResult("capability.method_binding", "capability", "optional", "pending"),
    CaseResult("capability.unsupported_method", "capability", "optional", "pending"),
    CaseResult("error.standard_error_shape", "core", "required", "pending"),
    CaseResult(
        "error.unauthorized",
        "core",
        "not-selected",
        "skipped",
        "auth policy hooks are outside the required Python core profile",
    ),
    CaseResult(
        "error.server_busy",
        "core",
        "not-selected",
        "skipped",
        "busy-state policy hooks are outside the required Python core profile",
    ),
    CaseResult(
        "stream.stream_open",
        "stream",
        "optional",
        "skipped",
        "stream.open RPC control-plane method is not part of the generated spec/v0.0.2 registry",
    ),
    CaseResult("stream.stream_data", "stream", "optional", "pending"),
    CaseResult(
        "stream.stream_close",
        "stream",
        "optional",
        "skipped",
        "stream.close RPC control-plane method is not part of the generated spec/v0.0.2 registry",
    ),
]


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


def run_case(case_id: str, fn: Callable[[], bool]) -> None:
    item = next(case for case in cases if case.id == case_id)
    start = perf_counter()
    try:
        ok = fn()
        item.status = "passed" if ok else "failed"
        if not ok and not item.message:
            item.message = "case returned false"
    except Exception as exc:  # noqa: BLE001 - record conformance failure detail
        item.status = "failed"
        item.message = str(exc)
    finally:
        item.duration_ms = (perf_counter() - start) * 1000.0


def round_trip_request(request: RpcPayload, configure_broker=None):
    broker = BasicBroker()
    if configure_broker is not None:
        configure_broker(broker)
    endpoint = AxtpEndpoint(broker)
    transport = MockTransport()
    endpoint.attach_transport(transport)

    chunks = []
    OutboundProcessor(chunks.append).send_rpc_request(request)
    for chunk in chunks:
        transport.inject_incoming(chunk)
    endpoint.poll()

    outgoing = transport.try_pop_outgoing()
    if outgoing is None:
        raise AssertionError("endpoint did not emit an RPC response")
    sink = CaptureSink()
    InboundProcessor(sink).on_bytes(outgoing)
    return sink.rpcs


def one_control_response(core: AxtpCore, control: ControlPayload):
    chunks = []
    OutboundProcessor(chunks.append).send_control(control)
    for chunk in chunks:
        core.on_bytes(chunk)
    outgoing = core.try_pop_outbound_bytes()
    if outgoing is None:
        raise AssertionError("core did not emit a control response")
    sink = CaptureSink()
    InboundProcessor(sink).on_bytes(outgoing)
    if len(sink.controls) != 1:
        raise AssertionError("expected exactly one decoded control response")
    return sink.controls[0]


def case_open_accept():
    core = AxtpCore()
    response = one_control_response(core, ControlPayload(ControlOpcode.Open, 1))
    return (
        response.opcode == ControlOpcode.Accept
        and response.control_id == 1
        and response.status_code == ErrorCode.Success
        and core.control_session_open()
    )


def case_close():
    core = AxtpCore()
    one_control_response(core, ControlPayload(ControlOpcode.Open, 1))
    response = one_control_response(core, ControlPayload(ControlOpcode.Close, 2))
    return (
        response.opcode == ControlOpcode.CloseAck
        and response.control_id == 2
        and not core.control_session_open()
    )


def case_ping_pong():
    core = AxtpCore()
    response = one_control_response(core, ControlPayload(ControlOpcode.Ping, 3))
    return response.opcode == ControlOpcode.Pong and response.control_id == 3


def case_request_response_json():
    def configure(broker):
        def handler(context, params_json):
            if context.method_name != "audio.getAlgorithmConfig" or params_json != "{}":
                raise AssertionError("unexpected JSON handler context")
            return json.dumps({"noiseSuppression": {"enabled": True, "level": 3}}, separators=(",", ":"))

        broker.register_json_method("audio.getAlgorithmConfig", handler)

    responses = round_trip_request(
        RpcPayload(
            encoding=RpcEncoding.Json,
            op=RpcOp.Request,
            request_id=1,
            method_or_event_id=MethodId.AudioGetAlgorithmConfig,
            body_encoding=RpcBodyEncoding.None_,
            body=b"{}",
        ),
        configure,
    )
    body = json.loads(responses[0].body.decode("utf-8"))
    return (
        len(responses) == 1
        and responses[0].op == RpcOp.RequestResponse
        and responses[0].request_id == 1
        and responses[0].status_code == ErrorCode.Success
        and "noiseSuppression" in body
    )


def case_method_not_found_with_id(request_id: int):
    responses = round_trip_request(
        RpcPayload(
            encoding=RpcEncoding.Json,
            op=RpcOp.Request,
            request_id=request_id,
            method_or_event_id=0x7FFF,
            body_encoding=RpcBodyEncoding.None_,
            body=b"{}",
        )
    )
    return (
        len(responses) == 1
        and responses[0].op == RpcOp.RequestResponse
        and responses[0].request_id == request_id
        and responses[0].status_code == ErrorCode.RpcMethodNotFound
    )


def case_event_emit():
    chunks = []
    outbound = OutboundProcessor(chunks.append)
    outbound.send_event(
        RpcPayload(
            encoding=RpcEncoding.Json,
            op=RpcOp.Event,
            method_or_event_id=EventId.AudioAlgorithmConfigChanged,
            body_encoding=RpcBodyEncoding.None_,
            meta=PayloadMeta(SourceProtocol.JsonRpc, 0, 0, "s1"),
            body=b'{"reason":"user_request","applyState":"applied"}',
        )
    )
    sink = CaptureSink()
    for chunk in chunks:
        InboundProcessor(sink).on_bytes(chunk)
    return (
        len(sink.rpcs) == 1
        and sink.rpcs[0].op == RpcOp.Event
        and sink.rpcs[0].method_or_event_id == EventId.AudioAlgorithmConfigChanged
        and json.loads(sink.rpcs[0].body.decode("utf-8"))["reason"] == "user_request"
    )


def case_capability_get_all():
    return (
        len(K_METHOD_REGISTRY) >= 4
        and RegistryLookup.method_id_by_name("audio.getAlgorithmConfig") == MethodId.AudioGetAlgorithmConfig
        and RegistryLookup.method_id_by_name("audio.getAlgorithmCapabilities") == MethodId.AudioGetAlgorithmCapabilities
        and RegistryLookup.method_id_by_name("audio.setAlgorithmConfig") == MethodId.AudioSetAlgorithmConfig
        and RegistryLookup.method_id_by_name("audio.resetAlgorithmConfig") == MethodId.AudioResetAlgorithmConfig
    )


def case_capability_method_binding():
    capability = [
        item
        for item in K_CAPABILITY_REGISTRY
        if item.id == CapabilityId.AudioAlgorithm and item.name == "audio.algorithm"
    ]
    method = RegistryLookup.method_by_id(MethodId.AudioGetAlgorithmConfig)
    event = RegistryLookup.event_by_id(EventId.AudioAlgorithmConfigChanged)
    return len(capability) == 1 and method is not None and event is not None and method.domain == "audio" and event.domain == "audio"


def case_stream_data():
    chunks = []
    payload = StreamPayload(9, 1, 0, PayloadMeta(), b"\xAA\xBB\xCC")
    OutboundProcessor(chunks.append).send_stream(payload)
    sink = CaptureSink()
    inbound = InboundProcessor(sink)
    for chunk in chunks:
        inbound.on_bytes(chunk)
    return (
        len(sink.streams) == 1
        and sink.streams[0].stream_id == 9
        and sink.streams[0].seq_id == 1
        and sink.streams[0].cursor == 0
        and sink.streams[0].data == b"\xAA\xBB\xCC"
    )


def write_result(output_path: Path, profile_path: str) -> None:
    summary = {
        "total": len(cases),
        "passed": sum(1 for case in cases if case.status == "passed"),
        "failed": sum(1 for case in cases if case.status in {"failed", "pending"}),
        "skipped": sum(1 for case in cases if case.status == "skipped"),
        "unsupported": sum(1 for case in cases if case.status == "unsupported"),
    }
    result = {
        "runtime": "axtp-python-runtime",
        "runtimeVersion": AXTP_GENERATED_VERSION["runtimeVersion"],
        "specTag": AXTP_GENERATED_VERSION["specTag"],
        "profile": profile_path,
        "requiredLevels": ["core", "framed-binary"],
        "optionalLevels": ["capability", "event", "stream"],
        "unsupportedLevels": ["websocket-jsonrpc"],
        "summary": summary,
        "cases": [case.to_json() for case in cases],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def env_is_true(name: str) -> bool:
    return os.environ.get(name) == "true"


def resolve_spec_path() -> Optional[Path]:
    for value in (os.environ.get("AXTP_SPEC_PATH"), "third_party/axtp-spec", ".axtp-spec"):
        if value is None:
            continue
        candidate = Path(value)
        if Path(candidate, "docs/conformance/manifest.yaml").is_file() or Path(
            candidate, "conformance/manifest.yaml"
        ).is_file():
            return candidate
    return None


def test_conformance():
    spec_path = resolve_spec_path()
    profile_path = os.environ.get("CONFORMANCE_PROFILE_PATH", "devtools/conformance/runtime-profile.yaml")
    result_path = Path(os.environ.get("CONFORMANCE_RESULT_PATH", "build/conformance-results/result.json"))
    assert spec_path is not None
    assert Path(profile_path).is_file()

    run_case("handshake.open_accept", case_open_accept)
    run_case("handshake.close", case_close)
    run_case("handshake.ping_pong", case_ping_pong)
    run_case("rpc.request_response_json", case_request_response_json)
    run_case("rpc.method_not_found", lambda: case_method_not_found_with_id(2))
    run_case("rpc.request_id_match", lambda: case_method_not_found_with_id(55))
    run_case("event.emit_event", case_event_emit)
    run_case("capability.get_all", case_capability_get_all)
    run_case("capability.method_binding", case_capability_method_binding)
    run_case("capability.unsupported_method", lambda: case_method_not_found_with_id(4))
    run_case("error.standard_error_shape", lambda: case_method_not_found_with_id(99))
    run_case("stream.stream_data", case_stream_data)

    write_result(result_path, profile_path)

    required_issue = any(case.requirement == "required" and case.status != "passed" for case in cases)
    optional_issue = any(case.requirement == "optional" and case.status != "passed" for case in cases)
    if required_issue and not env_is_true("CONFORMANCE_ALLOW_INCOMPLETE"):
        raise AssertionError("required AXTP conformance cases failed")
    if optional_issue and env_is_true("CONFORMANCE_STRICT_OPTIONAL"):
        raise AssertionError("optional AXTP conformance cases failed or were skipped")
