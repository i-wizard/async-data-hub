import asyncio
import uuid

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from app.config.dependencies import websocket_manager
from app.utils.logger import CustomLogger
from app.websocket.manager import WebSocketManager

router = APIRouter()


@router.websocket("/ws/updates")
async def websocket_updates(
    websocket: WebSocket,
    manager: WebSocketManager = Depends(websocket_manager),
) -> None:
    """
    Hosts the live update channel so aggregate and webhook workflows can publish
    progress without coupling themselves to connection lifecycle details.
    """

    client_id = str(uuid.uuid4())
    connection = await manager.connect(client_id=client_id, websocket=websocket)

    try:
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(manager.sender_loop(client_id=client_id, connection=connection))
            task_group.create_task(manager.receive_loop(websocket=websocket))
    # TaskGroup re-raises child exceptions wrapped in ExceptionGroup, so a plain
    # `except WebSocketDisconnect` would not match — use `except*` to unwrap.
    except* WebSocketDisconnect:
        CustomLogger.info(f"WebSocket client {client_id} disconnected")
        await manager.disconnect(client_id=client_id)
    except* asyncio.CancelledError:
        CustomLogger.info(f"WebSocket client {client_id} cancelled")
        await manager.disconnect(client_id=client_id)
        raise
