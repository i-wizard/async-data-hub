import asyncio
import time

import httpx
from httpx import AsyncClient

BASE_URL = "http://localhost:8000/api/v1"


async def write(client: AsyncClient, doc_id: str, content: str, durability: str):
    response = await client.post(
        f"{BASE_URL}/consistency/documents",
        params={"durability": durability},
        json={"id": doc_id, "content": content},
    )
    response.raise_for_status()
    return response.json()


async def read(client: AsyncClient, doc_id: str, source: str, wait_for_lsn: str | None):
    params = {"source": source}
    if wait_for_lsn is not None:
        params["wait_for_lsn"] = wait_for_lsn
    response = await client.get(
        f"{BASE_URL}/consistency/documents/{doc_id}", params=params
    )
    response.raise_for_status()
    return response.json()


async def replication_status(client: AsyncClient):
    response = await client.get(f"{BASE_URL}/consistency/replication-status")
    response.raise_for_status()
    return response.json()


def new_id() -> str:
    import uuid

    return str(uuid.uuid4())


async def replication_lag():
    """
    Demo 01 — REPLICATION LAG (eventual consistency).

    A write committed on the primary is not instantly visible on the replicas — the
    WAL has to stream over and be replayed. `replica2` is deliberately delayed, so
    a read from it right after an async write misses the value, then "catches up" a
    few seconds later. The primary, by contrast, is always immediately current.

    Run (inside the api container):  python demos/01_replication_lag.py
    """
    async with httpx.AsyncClient(timeout=30) as client:
        doc_id = new_id()
        content = "Hello, world!"
        await write(client, doc_id, content, durability="async")

        # The primary is authoritative and immediately consistent
        primary = await read(client, doc_id, source="primary", wait_for_lsn=None)
        print(
            f"primary   immediately after write : found={primary['found']} content={primary['content']!r}"
        )
        assert primary["found"] and primary["content"] == content

        # the delayed replica has not applied the write yet
        first = await read(client, doc_id, source="replica2", wait_for_lsn=None)
        print(
            f"replica2  immediately after write : found={first['found']}  (stale — WAL not applied yet)"
        )

        # Poll the replica until it converges, and measure how long it took
        start = time.monotonic()
        result = await read(client, doc_id, source="replica2", wait_for_lsn=None)
        while not result["found"]:
            if time.monotonic() - start > 30:
                raise TimeoutError("replica2 did not converge within 30 seconds")
            await asyncio.sleep(0.5)
            result = await read(client, doc_id, source="replica2", wait_for_lsn=None)
        lag = time.monotonic() - start
        print(f"replica2  converged after         : {lag:.2f}s")
        assert lag >= 0
        print(
            "\n✅ Eventual consistency: the replica converged to the primary's value after a lag."
        )


if __name__ == "__main__":
    asyncio.run(replication_lag())
