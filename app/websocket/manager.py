import asyncio
from dataclasses import dataclass
from typing import Dict, List

from fastapi import WebSocket


@dataclass
class ManagedConnection:
    """
    Tracks a WebSocket and its per-client queue so one slow consumer can be
    isolated instead of blocking all broadcast recipients.
    """

    websocket: WebSocket
    queue: asyncio.Queue[str]


class WebSocketManager:
    """
    Maintains live WebSocket connections with bounded queues so broadcasts stay
    non-blocking and slow clients can be handled explicitly.
    """

    def __init__(self, queue_size: int):
        """
        Stores the queue size because per-client buffering needs a hard ceiling
        to make backpressure visible instead of silently growing in memory.
        """

        self._queue_size = queue_size
        self._connections: Dict[str, ManagedConnection] = {}
        self._lock = asyncio.Lock()

    async def connect(self, client_id: str, websocket: WebSocket) -> ManagedConnection:
        """
        Accepts the socket and registers a bounded queue so application services
        can broadcast without writing directly to each transport.
        """

        await websocket.accept()
        connection = ManagedConnection(
            websocket=websocket,
            queue=asyncio.Queue(maxsize=self._queue_size),
        )
        async with self._lock:
            self._connections[client_id] = connection
        return connection

    async def disconnect(self, client_id: str) -> None:
        """
        Removes the connection during disconnect or failure so dead sockets do
        not remain in the broadcast registry.
        """

        async with self._lock:
            self._connections.pop(client_id, None)

    async def broadcast(self, message: str) -> None:
        """
        Queues one message per client instead of writing inline so one slow peer
        cannot stall the entire application broadcast path.
        """

        async with self._lock:
            connections = list(self._connections.items())

        for client_id, connection in connections:
            if connection.queue.full():
                await self.disconnect(client_id=client_id)
                await connection.websocket.close(code=1013)
                continue
            await connection.queue.put(message)

    async def sender_loop(self, client_id: str, connection: ManagedConnection) -> None:
        """
        Drains the per-client queue in its own task so broadcasts and socket I/O
        are decoupled and slow consumers remain isolated.
        """

        try:
            while True:
                message = await connection.queue.get()
                await connection.websocket.send_text(message)
        finally:
            await self.disconnect(client_id=client_id)

    async def receive_loop(self, websocket: WebSocket) -> None:
        """
        Keeps the socket alive by consuming inbound frames because many browsers
        and proxies expect read activity to continue during long-lived sessions.
        """

        while True:
            await websocket.receive_text()

    async def active_client_ids(self) -> List[str]:
        """
        Returns registered client identifiers so tests can verify connection
        cleanup without reaching into private manager internals.
        """

        async with self._lock:
            return list(self._connections.keys())
