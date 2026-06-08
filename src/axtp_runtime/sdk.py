import time
from typing import Callable, Optional, Union

from .broker import BasicBroker, JsonRpcHandler, RawRpcHandler
from .endpoint import AxtpEndpoint
from .generated.axtp_ids_generated import ErrorCode, RpcBodyEncoding, RpcEncoding, RpcOp
from .generated.registry import MethodRegistry
from .model import RpcPayload, body_encoding_for_rpc_encoding
from .transport import MockTransport


def _body_encoding_for(encoding: RpcEncoding) -> RpcBodyEncoding:
    return body_encoding_for_rpc_encoding(encoding)


class AxtpClient:
    def __init__(self, timeout_seconds: float = 1.0) -> None:
        self._broker = BasicBroker()
        self._endpoint = AxtpEndpoint(self._broker)
        self._transport: Optional[MockTransport] = None
        self._connected = False
        self._next_request_id = 1
        self._timeout_seconds = timeout_seconds
        self._registry = MethodRegistry.from_generated_defaults()
        self._last_error = ErrorCode.Success

    def attach_transport(self, transport: MockTransport, auto_open: bool = True) -> None:
        self.close()
        self._transport = transport
        self._endpoint = AxtpEndpoint(self._broker)
        self._endpoint.attach_transport(transport)
        if auto_open:
            transport.open()
        self._connected = True

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def last_error(self) -> ErrorCode:
        return self._last_error

    def registry(self) -> MethodRegistry:
        return self._registry

    def poll(self) -> None:
        self._endpoint.poll()

    def send_raw(self, method_id: int, encoding: RpcEncoding, body: bytes = b"", request_id: Optional[int] = None) -> int:
        rid = request_id or self._take_request_id()
        payload = RpcPayload(
            encoding=encoding,
            op=RpcOp.Request,
            request_id=rid,
            method_or_event_id=method_id,
            status_code=ErrorCode.Success,
            body_encoding=_body_encoding_for(encoding),
            body=bytes(body),
        )
        self._endpoint.send_rpc_request(payload)
        return rid

    def send_json(self, method: Union[int, str], params_json: str = "{}", request_id: Optional[int] = None) -> int:
        method_id = method if isinstance(method, int) else self._registry.find_method_id(method)
        if method_id is None:
            self._last_error = ErrorCode.RpcMethodNotFound
            return 0
        return self.send_raw(int(method_id), RpcEncoding.Json, params_json.encode("utf-8"), request_id)

    def try_take_rpc_response(self, request_id: int) -> Optional[RpcPayload]:
        response = self._endpoint.try_take_rpc_response(request_id)
        if response is not None:
            self._last_error = response.status_code
        return response

    def try_take_json_response(self, request_id: int) -> Optional[str]:
        response = self.try_take_rpc_response(request_id)
        if response is None:
            return None
        return response.body.decode("utf-8")

    def call_json(self, method: Union[int, str], params_json: str = "{}", pump: Optional[Callable[[], None]] = None, timeout_seconds: Optional[float] = None) -> str:
        request_id = self.send_json(method, params_json)
        if request_id == 0:
            return ""
        deadline = time.monotonic() + (timeout_seconds or self._timeout_seconds)
        while time.monotonic() < deadline:
            if pump is not None:
                pump()
            self.poll()
            response = self.try_take_json_response(request_id)
            if response is not None:
                return response
            time.sleep(0.001)
        self._last_error = ErrorCode.RpcResponseTimeout
        return ""

    def _take_request_id(self) -> int:
        value = self._next_request_id
        self._next_request_id += 1
        if self._next_request_id > 0xFFFFFFFF:
            self._next_request_id = 1
        return value


class AxtpServer:
    def __init__(self) -> None:
        self._broker = BasicBroker()
        self._endpoint = AxtpEndpoint(self._broker)
        self._transport: Optional[MockTransport] = None

    def attach_transport(self, transport: MockTransport) -> None:
        self._transport = transport
        self._endpoint.attach_transport(transport)
        transport.open()

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()

    def poll(self, max_tasks: int = 8) -> None:
        self._endpoint.poll(max_tasks)

    def on_raw(self, method_id: int, handler: RawRpcHandler) -> None:
        self._broker.register_raw_method(method_id, handler)

    def on_json(self, method, handler: JsonRpcHandler) -> None:
        self._broker.register_json_method(method, handler)

    def endpoint(self) -> AxtpEndpoint:
        return self._endpoint
