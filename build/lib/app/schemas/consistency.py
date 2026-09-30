from datetime import datetime
from enum import Enum
from typing import Literal, List

from pydantic import BaseModel


class Durability(str, Enum):
    """How durable a write must be before the primary reports success.

    QUORUM - synchronous_commit=on: commit waits for a standby to acknowledge
             (stronger durability; a stale read from that standby is impossible).
    ASYNC  - synchronous_commit=local: commit returns after the primary's local
             flush only (faster; replicas may lag -> stale reads possible).
    """

    QUORUM = "quorum"
    ASYNC = "async"


class ReadNode(str, Enum):
    """Which node a read is routed to."""

    PRIMARY = "primary"  # strong: always the latest committed value
    REPLICA1 = "replica1"  # eventual: fast async standby
    REPLICA2 = "replica2"  # eventual: deliberately delayed standby


class WriteRequest(BaseModel):
    """Upsert a document's content (always executed on the primary)."""

    id: str
    content: str


class WriteResult(BaseModel):
    id: str
    content: str
    updated_at: datetime
    durability: Durability

    # The primary's WAL position at write time. A replica has our write once its
    # replay LSN reaches this value (used by the wait-for-LSN read).
    write_lsn: str


class ReadResult(BaseModel):
    """Result of a read from a specific node."""

    id: str
    found: bool
    content: str | None = None
    updated_at: datetime | None = None
    served_by: ReadNode

    # The node's current WAL position (replay LSN on a replica), for visibility.
    node_lsn: str

    # Set only when the read used wait_for_lsn: did the node reach that LSN?
    up_to_date: bool | None = None


# LSN means => Log Sequence Number. It is a unique identifier that represents a specific point in the write-ahead log (WAL) of a database. Each time a change is made to the database, it is recorded in the WAL, and each entry is assigned an LSN. The LSN can be used to track the order of changes and to determine whether a replica has caught up with the primary database by comparing its replay LSN with the primary's write LSN.


class ReplicaStatus(BaseModel):
    """The primary's view of one standby (from pg_stat_replication)."""

    application_name: str
    state: str
    sync_state: Literal["async", "sync", "quorum", "potential"]
    replay_lag_seconds: float | None = None


class ReplicationStatusResponse(BaseModel):
    """Cluster replication overview, as seen from the primary."""

    primary_lsn: str
    standbys: List[ReplicaStatus]