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


async def read_your_writes():
    """
    Demo 02 — READ-AFTER-WRITE (a.k.a. read-your-writes) and two fixes.

    A user updates a value, then immediately reads it back. If the read is served by
    a lagging replica, they see their OLD value — a confusing "did my save work?"
    bug. Two standard fixes:

      Fix A — route the read to the PRIMARY (always current).
      Fix B — pin the read to the write's WAL LSN: wait until the replica has
              replayed up to that LSN, then read (read-your-writes from a replica).

    Run (inside the api container):  python demos/02_read_after_write.py
    """
    async with httpx.AsyncClient(timeout=30) as client:
        doc_id = new_id()
        # Seed v1 and make sure the delayed replica has it (so the anomaly below
        # is a STALE VALUE, not just a missing row).
        first_content = "v1"
        first = await write(client, doc_id, first_content, durability="async")
        await read(client, doc_id, source="replica2", wait_for_lsn=first["write_lsn"])

        # Update to v2, then immediately read the delayed replica
        second_content = "v2"
        second = await write(client, doc_id, second_content, durability="async")
        anomaly = await read(client, doc_id, source="replica2", wait_for_lsn=None)
        print(f"just wrote v2; replica2 now returns : {anomaly['content']!r}")
        if anomaly["content"] == first_content:
            print("  -> ❌ read-after-write anomaly: the user sees their OLD value")

        # Fix A: read the primary.
        fix_primary = await read(client, doc_id, source="primary", wait_for_lsn=None)
        print(f"fix A  read primary                 : {fix_primary['content']!r}")
        assert fix_primary["content"] == second_content

        # Fix B: wait for the replica to reach the write's LSN, then read it.
        fix_lsn = await read(
            client, doc_id, source="replica2", wait_for_lsn=second["write_lsn"]
        )
        print(
            f"fix B  replica2 wait_for_lsn        : {fix_lsn['content']!r}  (up_to_date={fix_lsn['up_to_date']})"
        )

        assert fix_lsn["content"] == second_content
        assert fix_lsn["up_to_date"] is True

        print(
            "\n✅ Read-after-write guaranteed by routing to the primary, or by waiting for the LSN."
        )


async def quorum_and_status():
    """
    Demo 03 — QUORUM (synchronous) writes and the replication overview.

    The primary is configured to wait for one standby to acknowledge each commit
    (a quorum/synchronous write). This trades a little latency for the guarantee
    that an acknowledged write survives a primary failure and is already on a
    replica. The app can opt out per request (async) for lower latency.

    This demo shows the cluster's replication status and that both durability levels
    succeed while the replicas are healthy. The CAP behaviour under a PARTITION
    (quorum writes block; async writes still succeed) is a manual exercise — see the
    README's "CAP under a partition" section (it requires pausing containers).

    Run (inside the api container):  python demos/03_quorum_and_status.py
    """
    async with httpx.AsyncClient(timeout=30) as client:
        status = await replication_status(client)
        print(f"primary LSN: {status['primary_lsn']}")
        print("standbys (from pg_stat_replication):")
        for standby in status["standbys"]:
            print(
                f"  - {standby['application_name']:<16} state={standby['state']:<10} "
                f"sync_state={standby['sync_state']:<10} lag={standby['replay_lag_seconds']}"
            )
        # At least one standby should be streaming; typically both.
        assert len(status["standbys"]) >= 1

        # With NUM_SYNCHRONOUS_REPLICAS=1, at least one standby is sync/quorum.
        sync_states = {s["sync_state"] for s in status["standbys"]}
        print(f"\nsync_state values present: {sync_states}")

        # Both durability levels succeed while replicas are healthy.
        q = await write(client, new_id(), "quorum-write", durability="quorum")
        a = await write(client, new_id(), "async-write", durability="async")
        print(
            f"quorum write committed at LSN {q['write_lsn']} (waited for a standby ack)"
        )
        print(f"async  write committed at LSN {a['write_lsn']} (no wait)")

        assert any(
            state in {"sync", "quorum"} for state in sync_states
        ), "expected at least one synchronous standby"
        print(
            "\n✅ A synchronous standby backs quorum writes; async writes skip the wait."
        )
        print(
            "   (Partition behaviour — quorum blocks, async proceeds — is in the README.)"
        )


async def run_with_partition():
    """
    Demo 04 — QUORUM (synchronous) writes under a PARTITION.

    This demo is a manual exercise — it requires pausing containers to simulate a
    network partition. >>> make pause-replicas

    A quorum write timeout while the replicas are unreachable, but async writes still succeed.
    This is a classic CAP tradeoff: the system is available (async writes succeed) but not consistent (quorum writes fail).
    But for Quorum writes, the system is consistent (they fail) but not available (they block).
    """
    async with httpx.AsyncClient(timeout=15) as client:
        q = write(client, new_id(), "quorum-write", durability="quorum")
        a = write(client, new_id(), "async-write", durability="async")
        results = await asyncio.gather(q, a, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                print(f"quorum write failed: {result}")
            else:
                print(
                    f"async write succeeded: {result['durability']} write committed at LSN {result['write_lsn']}"
                )


if __name__ == "__main__":
    asyncio.run(run_with_partition())
