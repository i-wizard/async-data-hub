import asyncio
from typing import List, Tuple

import asyncpg

from app.config.settings import get_settings


def dsn() -> str:
    url = get_settings().database_url
    return url.replace("postgresql+asyncpg", "postgresql")


async def connect() -> asyncpg.Connection:
    """Open a fresh asyncpg connection (its own backend / transaction scope)."""
    return await asyncpg.connect(dsn())


async def reset_accounts(accounts: List[Tuple[str, int]]) -> None:
    """(Re)create the accounts table and seed it with (id, balance) rows."""
    conn = await connect()
    try:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS accounts "
            "(id text PRIMARY KEY, owner test NOT NULL, balance bigint NOT NULL)"
        )
        await conn.execute("DELETE FROM accounts")
        for account_id, balance in accounts:
            await conn.execute(
                "INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)",
                account_id,
                account_id,
                balance,
            )
    finally:
        await conn.close()


async def balance(conn: asyncpg.Connection, account_id: str) -> int:
    """Read one account's balance on the given connection."""
    return await conn.fetchval("SELECT balance FROM accounts WHERE id = $1", account_id)


def is_serialization_failure(exc: BaseException) -> bool:
    """True if the asyncpg error is a serialization failure (SQLSTATE 40001)."""
    return (
        isinstance(exc, asyncpg.PostgresError)
        and getattr(exc, "sqlstate", None) == "40001"
    )


async def mvcc_no_dirty_reads() -> None:
    """
    mvcc means "multi-version concurrency control".
    Demo 01 — MVCC: readers don't block writers, and there are NO dirty reads.

    Postgres keeps multiple versions of a row. A transaction reads from a consistent
    SNAPSHOT of committed data, so:
      - a reader never sees another transaction's UNCOMMITTED change (no dirty read),
      - and a reader is never blocked by a writer holding an uncommitted row.

    """
    await reset_accounts([("alice", 100)])
    writer = await connect()
    reader = await connect()
    try:
        # Writer updates alice to 500 but does not commit yet
        wtx = writer.transaction(isolation="read_committed")
        await wtx.start()
        await writer.execute("UPDATE accounts SET balance = 500 WHERE id = 'alice'")
        # Reader concurrently reads alice, it returns immediately (not blocked)
        # and sees the last COMMITTED value (100), not the writer's uncommitted 500
        rtx = reader.transaction(isolation="read_committed")
        await rtx.start()
        seen_while_uncommited = await balance(reader, "alice")
        await rtx.commit()
        print(
            f"reader sees while writer uncommitted : {seen_while_uncommited}  (committed snapshot)"
        )
        assert (
            seen_while_uncommited == 100
        ), "dirty read! (should never happen in Postgres)"

        # Once writer commits, a fresh read sees the new value
        await wtx.commit()
        seen_after_commit = await balance(reader, "alice")
        print(f"reader sees after writer commits     : {seen_after_commit}")
        assert seen_after_commit == 500
        print(
            "\n Readers don't block writers, and uncommitted data is never visible (no dirty reads)."
        )
    finally:
        await reader.close()
        await writer.close()


async def scenario(isolation: str):
    """Read alice, ler another txn set it to 200, read again; return (first, second)"""
    await reset_accounts([("alice", 100)])
    t1 = await connect()
    t2 = await connect()
    try:
        tx1 = t1.transaction(isolation=isolation)
        await tx1.start()
        first = await balance(t1, "alice")  # fixes the snapshot for RR
        await t2.execute(
            "UPDATE accounts SET balance = 200 WHERE id = 'alice'"
        )  # autocommit
        second = await balance(t1, "alice")
        await tx1.commit()
        return first, second
    finally:
        await t1.close()
        await t2.close()


async def non_repeatable_read() -> None:
    """
        Demo 02 — NON-REPEATABLE READ: Read Committed vs Repeatable Read.

    A transaction reads a row twice. Between the reads, another transaction commits
    a change to that row.
      - READ COMMITTED : each statement sees the latest committed data -> the second
                         read differs (non-repeatable).
      - REPEATABLE READ: the transaction's snapshot is fixed at its first statement
                         -> both reads are identical.
    """
    rc = await scenario("read_committed")
    rr = await scenario("repeatable_read")
    print(f"read_committed : first={rc[0]} second={rc[1]}  -> changed (non-repeatable)")
    print(f"repeatable_read: first={rr[0]} second={rr[1]}  -> stable (repeatable)")

    assert rc == (100, 200)
    assert rr == (100, 100)
    print(
        "\n  Read Committed sees the concurrent commit; Repeatable Read is pinned to its snapshot."
    )


async def phantom_scenario(isolation: str):
    """Count rich accounts, let another txn insert one, count again."""
    _count_rich = "SELECT COUNT(*) from accounts WHERE balance >= 1000"
    await reset_accounts([("a1", 100), ("a2", 200)])  # none qualify yet
    t1 = await connect()
    t2 = await connect()
    try:
        tx1 = t1.transaction(isolation=isolation)
        await tx1.start()
        first = (await t1.fetchrow(_count_rich))["count"]
        await t2.execute(
            "INSERT INTO accounts (id, owner, balance) VALUES ('Whale', 'Whale', 5000)"
        )  # autocommit
        second = await t1.fetchval(_count_rich)
        await tx1.commit()
        return first, second
    finally:
        await t1.close()
        await t2.close()


async def phantom_read() -> None:
    """
        Demo 03 — PHANTOM READ: Read Committed vs Repeatable Read.

    A transaction runs the same RANGE query twice (count accounts with balance >=
    1000). Between the runs, another transaction inserts a qualifying row.
      - READ COMMITTED : the second count includes the new row (a "phantom").
      - REPEATABLE READ: Postgres's snapshot isolation hides it -> counts match.
        (This is stricter than the SQL standard, which permits phantoms at RR.)

    """
    rc = await phantom_scenario("read_committed")
    rr = await phantom_scenario("repeatable_read")
    print(f"read_committed : first={rc[0]} second={rc[1]}  -> phantom appeared")
    print(f"repeatable_read: first={rr[0]} second={rr[1]}  -> no phantom")
    assert rc == (0, 1)
    assert rr == (0, 0)
    print("\n  Read Committed sees the inserted phantom; Repeatable Read does not.")


async def _read_commited_case() -> int:
    """Return alice's final balance after two concurrent +50 updates at RC."""
    await reset_accounts([("alice", 100)])
    t1 = await connect()
    t2 = await connect()
    try:
        tx1 = t1.transaction(isolation="read_committed")
        tx2 = t2.transaction(isolation="read_committed")
        await tx1.start()
        await tx2.start()
        b1 = await balance(t1, "alice")  # 100
        b2 = await balance(t2, "alice")  # 100
        await t1.execute(
            "UPDATE accounts SET balance = $1 WHERE id = 'alice'", b1 + 50
        )  # 150
        # t2's UPDATE blocks on the row lock -> run it as a task.
        task2 = asyncio.create_task(
            t2.execute("UPDATE accounts SET balance = $1 WHERE id = 'alice'", b2 + 50)
        )
        await asyncio.sleep(0.3)  # Let task2 reach the lock wait
        await tx1.commit()  # release the lock; task2 unblocks
        # await task2  # succeeds at RC (no serialization error) but writes stale 150
        await tx2.commit()
        return await balance(t1, "alice")  # final value after both commits
    finally:
        await t1.close()
        await t2.close()


async def _repeatable_read_case() -> tuple:
    """Return (serialization_error_raised, final_balance) at RR with a retry."""
    await reset_accounts([("alice", 100)])
    t1 = await connect()
    t2 = await connect()
    try:
        tx1 = t1.transaction(isolation="repeatable_read")
        tx2 = t2.transaction(isolation="repeatable_read")
        await tx1.start()
        await tx2.start()
        b1 = await balance(t1, "alice")
        b2 = await balance(t2, "alice")
        await t1.execute(
            "UPDATE accounts SET balance = $1 WHERE id = 'alice'", b1 + 50
        )  # 150
        task2 = asyncio.create_task(
            t2.execute("UPDATE accounts SET balance = $1 WHERE id = 'alice'", b2 + 50)
        )
        await asyncio.sleep(0.3)
        await tx1.commit()
        raised = False
        try:
            await task2
            await tx2.commit()
        except Exception as exc:
            if not is_serialization_failure(exc):
                raise
            raised = True
            await tx2.rollback()
            # Retry against fresh data: read 150, add 50 -> 200
            retry = t2.transaction(isolation="repeatable_read")
            await retry.start()
            b2 = await balance(t2, "alice")
            await t2.execute(
                "UPDATE accounts SET balance = $1 WHERE id = 'alice'", b2 + 50
            )
            retry.commit()
        return raised, await balance(t2, "alice")
    finally:
        await t1.close()
        await t2.close()


async def lost_update() -> None:
    """
    Demo 04 — LOST UPDATE: Read Committed loses it; Repeatable Read blocks it.

    Two transactions each read alice's balance (100), add 50, and write it back.
    Serialized, the result should be 200.
      - READ COMMITTED : the second writer's blocked UPDATE proceeds after the first
                         commits, but writes 150 from its STALE read -> final 150
                         (one update lost).
      - REPEATABLE READ: the second writer's UPDATE fails with a serialization error
                         (40001). We retry it against fresh data -> final 200.
    """
    rc_final = await _read_commited_case()
    rr_raised, rr_final = await _repeatable_read_case()
    print(
        f"read_committed : final balance = {rc_final}  (expected 200 if safe)  -> LOST UPDATE"
    )
    print(
        f"repeatable_read: serialization_error={rr_raised}, final balance after retry = {rr_final}"
    )
    assert rc_final == 150
    assert rr_raised is True
    assert rr_final == 200
    print(
        "\n✅ Read Committed silently lost an update; Repeatable Read forced a retry to stay correct."
    )


if __name__ == "__main__":
    asyncio.run(lost_update())
