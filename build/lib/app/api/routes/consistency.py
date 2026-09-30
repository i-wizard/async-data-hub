from fastapi import APIRouter, Depends, Query

from app.config.dependencies import consistency_service
from app.schemas.consistency import (
    ReplicationStatusResponse,
    WriteResult,
    WriteRequest,
    Durability,
    ReadResult,
    ReadNode,
)
from app.services.consistency import ConsistencyService

router = APIRouter(prefix="/consistency")


@router.post(
    "/documents",
    response_model=WriteResult,
)
async def write_document(
    data: WriteRequest,
    durability: Durability = Query(Durability.QUORUM),
    _service: ConsistencyService = Depends(consistency_service),
):
    return await _service.write(
        doc_id=data.id, content=data.content, durability=durability
    )


@router.get(
    "/documents/{document_id}",
    response_model=ReadResult,
)
async def read_document(
    document_id: str,
    source: ReadNode = Query(ReadNode.REPLICA1),
    wait_for_lsn: str | None = Query(
        None,
        description="Wait for this LSN to be replayed on the replica before returning",
    ),
    wait_timeout: float = Query(
        10.0, description="Timeout in seconds to wait for the LSN to be replayed"
    ),
    _service: ConsistencyService = Depends(consistency_service),
):
    return await _service.read(
        node=source,
        doc_id=document_id,
        wait_for_lsn=wait_for_lsn,
        wait_timeout=wait_timeout,
    )


@router.get(
    "/replication-status",
    response_model=ReplicationStatusResponse,
    response_description="The primary's view of it's standby's (sync state + replay lag",
)
async def replication_status(
    _service: ConsistencyService = Depends(consistency_service),
):
    return await _service.replication_status()
