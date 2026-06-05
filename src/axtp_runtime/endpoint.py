from typing import Optional

from .broker import BasicBroker, BrokerTask, BrokerTaskType
from .core import AxtpCore, CoreEventType
from .model import RpcPayload
from .transport import MockTransport


class AxtpEndpoint:
    def __init__(self, broker: BasicBroker) -> None:
        self._broker = broker
        self._core = AxtpCore()
        self._transport: Optional[MockTransport] = None

    def attach_transport(self, transport: MockTransport) -> None:
        self._transport = transport
        self._core.configure(transport.profile())
        transport.bind(self.on_transport_bytes)

    def detach_transport(self) -> None:
        self._transport = None

    def on_transport_bytes(self, data: bytes) -> None:
        self._core.on_bytes(data)

    def poll(self, max_tasks: int = 8) -> None:
        self._drain_core_events()
        self._broker.poll(max_tasks)
        self._drain_broker_results()
        self.flush_outbound()

    def core(self) -> AxtpCore:
        return self._core

    def broker(self) -> BasicBroker:
        return self._broker

    def send_rpc_request(self, payload: RpcPayload) -> None:
        self._core.expect_rpc_response(payload.request_id)
        self._core.send_rpc_request(payload)
        self.flush_outbound()

    def try_take_rpc_response(self, request_id: int):
        return self._core.try_take_rpc_response(request_id)

    def flush_outbound(self) -> None:
        if self._transport is None:
            return
        while True:
            data = self._core.try_pop_outbound_bytes()
            if data is None:
                return
            self._transport.send_bytes(data)

    def _drain_core_events(self) -> None:
        while True:
            event = self._core.poll_event()
            if event is None:
                return
            if event.type == CoreEventType.RpcRequest and event.rpc is not None:
                self._broker.submit(BrokerTask(BrokerTaskType.RpcRequest, rpc=event.rpc))
            elif event.type == CoreEventType.RpcEvent and event.rpc is not None:
                self._broker.submit(BrokerTask(BrokerTaskType.RpcEvent, rpc=event.rpc))

    def _drain_broker_results(self) -> None:
        while True:
            result = self._broker.poll_result()
            if result is None:
                return
            self._core.handle_broker_result(result)
