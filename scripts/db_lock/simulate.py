import asyncio
import time
from typing import List

import asyncpg

from scripts.payment.load_customers import settings


def _dsn():
    return settings.database_url.replace("postgresql+asyncpg", "postgresql")


async def _connect() -> asyncpg.Connection:
    """Open a fresh asyncpg connection (its own backend / transaction scope)."""
    return await asyncpg.connect(_dsn())


async def _reset_jobs(count: int) -> List[str]:
    """Clear the queue and insert `count` QUEUED jobs; return their ids in order."""
    conn = await _connect()
    try:
        await conn.execute("DELETE FROM db_queue_jobs")
        ids = []
        for i in range(count):
            insert_statement = (
                "INSERT INTO db_queue_jobs (id, payload, status, created_at, updated_at)"
                "VALUES ($1, $2, 'QUEUED', now(), now()) RETURNING id"
            )
            job_id = await conn.fetchval(
                insert_statement, f"{i}-job_id", f"payload-{i}"
            )
            ids.append(job_id)
    finally:
        await conn.close()
    return ids


def _is_deadlock(exc: BaseException) -> bool:
    """True if the asyncpg error is a deadlock (SQLSTATE 40P01)."""
    return (
        isinstance(exc, asyncpg.PostgresError)
        and getattr(exc, "sqlstate", None) == "40P01"
    )


def _is_lock_not_available(exc: BaseException) -> bool:
    """True if the asyncpg error is lock_not_available (SQLSTATE 55P03)."""
    return (
        isinstance(exc, asyncpg.PostgresError)
        and getattr(exc, "sqlstate", None) == "55P03"
    )


async def row_lock_blocks():
    """
        Demo 01 — A row lock is exclusive: FOR UPDATE blocks another FOR UPDATE.

    `SELECT ... FOR UPDATE` takes an EXCLUSIVE lock on the matched row and holds it
    until the transaction ends. A second transaction that tries to lock the same row
    BLOCKS — indefinitely by default — until the first commits. (Demo 02 shows how
    to bound or avoid that wait.)
    """
    (job_id,) = await _reset_jobs(1)
    holder_conn = await _connect()
    waiter_conn = await _connect()
    select_for_update_statement = (
        "SELECT id FROM db_queue_jobs WHERE id = $1 FOR UPDATE"
    )
    try:
        holder_txn = holder_conn.transaction()
        await holder_txn.start()
        await holder_conn.execute(select_for_update_statement, job_id)
        waiter_txn = waiter_conn.transaction()
        await waiter_txn.start()
        task = asyncio.create_task(
            waiter_conn.fetchval(select_for_update_statement, job_id)
        )
        await asyncio.sleep(0.3)
        blocked = not task.done()
        print(f"waiter blocked while holder keeps the lock: {blocked}")
        assert blocked
        await holder_txn.commit()
        done = await task
        await waiter_txn.commit()
        print(f"after holder commit, waiter acquired the row: {done == job_id}")
    finally:
        await holder_conn.close()
        await waiter_conn.close()


async def no_wait_amd_lock_timeout():
    """

        By default a blocked FOR UPDATE waits forever for the holder. Two ways to bound
    it (both raise SQLSTATE 55P03, lock_not_available):
      - FOR UPDATE NOWAIT        : fail IMMEDIATELY if the row is already locked.
      - SET lock_timeout = '..'  : wait at most that long, then fail.

    This is the concrete answer to "how long does a waiter wait?": by default,
    indefinitely — so you opt into NOWAIT or lock_timeout to fail fast.
    """
    (job_id,) = await _reset_jobs(1)
    holder_conn = await _connect()
    waiter_conn = await _connect()
    select_for_update_statement = (
        "SELECT id FROM db_queue_jobs WHERE id = $1 FOR UPDATE"
    )
    try:
        holder_txn = holder_conn.transaction()
        await holder_txn.start()
        await holder_conn.execute(select_for_update_statement, job_id)
        nowait_failed = False
        start_time = time.monotonic()
        try:
            await waiter_conn.execute(
                "SELECT id from db_queue_jobs WHERE id = $1 FOR UPDATE NOWAIT", job_id
            )
        except Exception as exc:
            nowait_failed = _is_lock_not_available(exc)
        time_elapsed = time.monotonic() - start_time
        print(
            f"NOWAIT failed immediately: {nowait_failed} (after {time_elapsed * 1000:.0f} ms)"
        )
        assert time_elapsed < 0.2
        assert nowait_failed

        timeout_failed = False
        await waiter_conn.execute("SET lock_timeout = '300ms'")
        start_time = time.monotonic()
        try:
            await waiter_conn.execute(select_for_update_statement, job_id)
        except Exception as exc:
            timeout_failed = _is_lock_not_available(exc)
            time_elapsed = time.monotonic() - start_time
        assert timeout_failed
        assert (
            time_elapsed >= 0.30
        )  # lock_timeout should wait at least 300ms before failing
        print(
            f"lock_timeout failed after ~300ms: {timeout_failed} (after {time_elapsed * 1000:.0f} ms)"
        )
        await holder_txn.commit()

    finally:
        await holder_conn.close()
        await waiter_conn.close()


async def for_share_vs_for_update():
    """

        FOR SHARE takes a SHARED row lock: several transactions can hold it at once
    (readers coexist). FOR UPDATE takes an EXCLUSIVE lock: it conflicts with any
    other lock. So two FOR SHARE locks are granted concurrently, but a FOR UPDATE
    must wait while any FOR SHARE is held.
    """
    (job_id,) = await _reset_jobs(1)
    share_a = await _connect()
    share_b = await _connect()
    exclusive = await _connect()
    select_for_share = "SELECT id FROM db_queue_jobs WHERE id = $1 FOR SHARE"
    try:
        share_a_txn = share_a.transaction()
        await share_a_txn.start()
        await share_a.execute(select_for_share, job_id)
        share_b_txn = share_b.transaction()
        await share_b_txn.start()
        second_share_task = asyncio.create_task(
            share_b.fetchval(select_for_share, job_id)
        )
        await asyncio.sleep(0.2)
        assert second_share_task.done()
        print(f"second FOR SHARE granted concurrently: {second_share_task.done()}")

        exclusive_txn = exclusive.transaction()
        await exclusive_txn.start()
        exclusive_task = asyncio.create_task(
            exclusive.fetchval(
                "SELECT id FROM db_queue_jobs WHERE id = $1 FOR UPDATE", job_id
            )
        )
        await asyncio.sleep(0.3)
        assert not exclusive_task.done()
        print(
            f"FOR UPDATE blocked while shared locks held: {not exclusive_task.done()}"
        )
        await share_a_txn.commit()
        await share_b_txn.commit()
        exclusive_res = await exclusive_task
        assert exclusive_task.done()
        assert exclusive_res == job_id
        print(
            f"after shared locks released, FOR UPDATE acquired the row: {exclusive_res == job_id}"
        )
    finally:
        await share_a.close()
        await share_b.close()
        await exclusive.close()


async def skip_locked_queue():
    """
        Multiple workers pull from the same queue. With SKIP LOCKED, a worker's claim
    IGNORES rows already locked by other workers and grabs the next free one — so N
    workers claim N DIFFERENT jobs with no blocking and no double-processing. This
    is the reason SKIP LOCKED exists.

    """
    claim = (
        "SELECT id FROM db_queue_jobs WHERE status = 'QUEUED' ORDER BY position "
        "FOR UPDATE SKIP LOCKED LIMIT 1"
    )
    num_workers = 3
    workers = [await _connect() for _ in range(num_workers)]
    try:
        await _reset_jobs(num_workers)

        txns = []
        job_ids = []
        for worker in workers:
            txn = worker.transaction()
            await txn.start()
            job_id = await worker.fetchval(claim)
            txns.append(txn)
            job_ids.append(job_id)
        assert all(job for job in job_ids if job is not None), "every worker got a job"
        print(f"jobs claimed by the {num_workers} workers: {job_ids}")
        assert len(job_ids) == len(set(job_ids)), "each worker claimed a DIFFERENT job"

        extra = await _connect()
        try:
            extra_txn = extra.transaction()
            await extra_txn.start()
            job_id = await extra.fetchval(claim)
            assert job_id is None, f"4th worker (queue exhausted) claimed: {job_id}"
            print(f"4th worker (queue exhausted) claimed: {job_id}")
            await extra_txn.commit()
        finally:
            await extra.close()
        for txn in txns:
            await txn.commit()
    finally:

        for worker in workers:
            await worker.close()


async def deadlock():
    """
        (deadlock): T1 locks job A then reaches for B; T2 locks job B then reaches
    for A. Each waits for a row the other holds — a cycle. Postgres's deadlock
    detector notices and ABORTS one transaction with SQLSTATE 40P01; the survivor
    proceeds.

    """
    job_1, job_2 = await _reset_jobs(2)
    con1 = await _connect()
    con2 = await _connect()
    txn1 = con1.transaction()
    txn2 = con2.transaction()
    claim = "SELECT id from db_queue_jobs WHERE id = $1 FOR UPDATE"
    try:
        await txn1.start()
        await txn2.start()
        await con1.execute(claim, job_1)
        await con2.execute(claim, job_2)
        results = await asyncio.gather(
            con1.execute(claim, job_2),
            con2.execute(claim, job_1),
            return_exceptions=True,
        )
        errors = [
            res for res in results if isinstance(res, Exception) and _is_deadlock(res)
        ]
        assert (
            len(errors) == 1
        ), "Postgres should break the cycle by aborting exactly one txn"
        print(f"part 1 — transactions aborted by the deadlock detector: {errors}")

    finally:
        await con1.close()
        await con2.close()


if __name__ == "__main__":
    asyncio.run(deadlock())
