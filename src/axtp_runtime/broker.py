from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional

from .generated.axtp_ids_generated import ErrorCode, RpcBodyEncoding, RpcEncoding, RpcOp
from .generated.registry import MethodRegistry
from .model import RpcPayload


class BrokerTaskType(str, Enum):
    RpcRequest = "rpcRequest"
    RpcEvent = "rpcEvent"
    StreamData = "streamData"
    ControlNotice = "controlNotice"
    ProtocolError = "protocolError"


class BrokerResultType(str, Enum):
    RpcResponse = "rpcResponse"
    RpcError = "rpcError"
    Event = "event"
    Noop = "noop"


@dataclass
class RpcContext:
    session_id: int
    request_id: int
    method_id: int
    method_name: str
    encoding: RpcEncoding
    source_protocol: int


RawRpcHandler = Callable[[RpcContext, RpcPayload], bytes]
JsonRpcHandler = Callable[[RpcContext, str], str]


@dataclass
class BrokerTask:
    type: BrokerTaskType
    rpc: Optional[RpcPayload] = None


@dataclass
class BrokerResult:
    type: BrokerResultType
    rpc: Optional[RpcPayload] = None


class BusinessRouter:
    def __init__(self) -> None:
        self._method_handlers: Dict[int, RawRpcHandler] = {}
        self._method_registry = MethodRegistry.from_generated_defaults()

    def registry(self) -> MethodRegistry:
        return self._method_registry

    def register_raw_method(self, method_id: int, handler: RawRpcHandler) -> None:
        self._method_handlers[method_id] = handler

    def register_json_method(self, method, handler: JsonRpcHandler) -> None:
        method_id = method if isinstance(method, int) else self._method_registry.find_method_id(method)
        if method_id is None:
            return

        def raw_handler(context: RpcContext, request: RpcPayload) -> bytes:
            return handler(context, request.body.decode("utf-8")).encode("utf-8")

        self.register_raw_method(int(method_id), raw_handler)

    def handle_rpc_request(self, request: RpcPayload) -> RpcPayload:
        response = RpcPayload(
            encoding=request.encoding,
            op=RpcOp.RequestResponse,
            request_id=request.request_id,
            method_or_event_id=request.method_or_event_id,
            status_code=ErrorCode.Success,
            body_encoding=request.body_encoding,
            meta=request.meta,
            body=b"",
        )
        handler = self._method_handlers.get(request.method_or_event_id)
        if handler is None:
            response.status_code = ErrorCode.RpcMethodNotFound
            return response
        method_name = self._method_registry.find_method_name(request.method_or_event_id) or ""
        context = RpcContext(
            session_id=request.meta.session_id,
            request_id=request.request_id,
            method_id=request.method_or_event_id,
            method_name=method_name,
            encoding=request.encoding,
            source_protocol=int(request.meta.source_protocol),
        )
        try:
            body = handler(context, request)
            response.body = body if isinstance(body, bytes) else bytes(body)
            if request.encoding == RpcEncoding.Json:
                response.body_encoding = RpcBodyEncoding.RawBytes
        except Exception:
            response.status_code = ErrorCode.RpcExecutionFailed
        return response


class BasicBroker:
    def __init__(self) -> None:
        self._tasks: List[BrokerTask] = []
        self._results: List[BrokerResult] = []
        self._router = BusinessRouter()

    def registry(self) -> MethodRegistry:
        return self._router.registry()

    def submit(self, task: BrokerTask) -> None:
        self._tasks.append(task)

    def poll(self, max_tasks: int = 8) -> None:
        processed = 0
        while self._tasks and processed < max_tasks:
            processed += 1
            task = self._tasks.pop(0)
            if task.type == BrokerTaskType.RpcRequest and task.rpc is not None:
                response = self._router.handle_rpc_request(task.rpc)
                result_type = BrokerResultType.RpcResponse if response.status_code == ErrorCode.Success else BrokerResultType.RpcError
                self._results.append(BrokerResult(result_type, response))
            elif task.type == BrokerTaskType.RpcEvent and task.rpc is not None:
                self._results.append(BrokerResult(BrokerResultType.Event, task.rpc))

    def poll_result(self) -> Optional[BrokerResult]:
        if not self._results:
            return None
        return self._results.pop(0)

    def register_raw_method(self, method_id: int, handler: RawRpcHandler) -> None:
        self._router.register_raw_method(method_id, handler)

    def register_json_method(self, method, handler: JsonRpcHandler) -> None:
        self._router.register_json_method(method, handler)
