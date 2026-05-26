# Async FastAPI Deep Dive — Context & Engineering Direction

## Objective

Build a production-grade async backend project called:

# Async Data Hub

The goal is NOT CRUD practice.

The goal is to deeply understand:

- FastAPI async internals
- ASGI architecture
- asyncio scheduling
- structured concurrency
- cancellation propagation
- realtime systems
- streaming
- backpressure
- concurrency limits
- worker/process architecture
- performance/scaling tradeoffs

This project should intentionally exercise the most important async patterns used in real production systems.

---

# Core Tech Stack

## API Layer
- FastAPI
- Starlette
- Uvicorn
- uvloop

## Async/Concurrency
- asyncio
- asyncio.TaskGroup (Python 3.11+)

## HTTP
- httpx.AsyncClient

## Database
- PostgreSQL
- asyncpg OR SQLAlchemy async

## Cache
- Redis
- redis.asyncio (preferred modern library)

## Realtime
- WebSockets
- Server Sent Events (SSE)

## Optional Later
- background workers
- process pools
- task queues

---

# Key Architectural Principles

The project MUST follow these principles.

## 1. Requests should stay lightweight

Requests should:
- validate input
- write minimal durable state
- enqueue work
- stream status
- return quickly

Avoid:
- long blocking workflows
- giant in-request orchestration
- CPU-heavy operations inside request handlers

---

## 2. Async is for I/O concurrency, NOT CPU acceleration

Async improves:
- waiting efficiency
- socket scalability
- concurrent I/O

Async does NOT improve:
- CPU-heavy computations

CPU-heavy work must eventually use:
- thread pools
- process pools
- external workers

---

## 3. Unlimited concurrency is a bug

All downstream systems require limits.

Use:
- semaphores
- bounded queues
- connection pools
- rate limiting
- backpressure

Never spawn unbounded tasks.

---

## 4. Cancellation must propagate correctly

If:
- client disconnects
- request times out
- server shuts down

Then:
- child tasks should cancel
- outbound HTTP calls should stop
- DB queries should stop
- resources should be released

Do NOT swallow `asyncio.CancelledError`.

Always re-raise it.

---

## 5. Long workflows must be failure-safe

Never assume workflows fully complete.

Use patterns like:
- DB transactions
- outbox pattern
- retries
- idempotency

Do NOT rely on request lifetime for critical workflows.

---

# Deep Async Concepts Already Covered

Codex should assume the following concepts are already understood and should guide implementation choices.

---

# 1. ASGI Internals

ASGI applications fundamentally look like:

```python
async def app(scope, receive, send):
    ...
```

ASGI revolves around:
- `scope`
- `receive`
- `send`

## Important ASGI Concepts

### HTTP scope
```python
scope["type"] == "http"
```

### WebSocket scope
```python
scope["type"] == "websocket"
```

### receive()
Receives:
- request body chunks
- disconnect events
- websocket messages

### send()
Sends:
- response start events
- response body chunks
- websocket frames

---

# 2. FastAPI Request Lifecycle

High-level flow:

```text
Client
→ Socket
→ Uvicorn
→ Event Loop
→ ASGI
→ Starlette
→ FastAPI
→ Endpoint
```

Each incoming request becomes:
- one asyncio Task

---

# 3. Event Loop Scheduling

The event loop:
- schedules Tasks
- resumes paused coroutines
- handles I/O readiness

Async uses:
- cooperative multitasking

Tasks yield control at:
```python
await ...
```

`await` effectively pauses execution and returns control to the event loop.

---

# 4. Coroutines / Tasks / Futures

## Coroutine
A pausable async function.

## Task
A scheduled coroutine managed by the event loop.

## Future
Represents a result that will become available later.

---

# 5. Structured Concurrency

Use:
```python
asyncio.TaskGroup
```

instead of unmanaged `create_task()` patterns.

Benefits:
- scoped task lifetimes
- automatic cancellation propagation
- cleaner error handling
- no orphaned tasks

---

# 6. Cancellation Propagation

Client disconnects trigger:
```python
asyncio.CancelledError
```

Cancellation propagates:
```text
Request Task
→ TaskGroup
→ child tasks
```

Cancellation only occurs at await points.

Blocking code breaks cancellation responsiveness.

---

# 7. Blocking vs Async Code

Blocking code:
```python
time.sleep()
```

blocks the event loop.

Async-friendly code:
```python
await asyncio.sleep()
```

yields control cooperatively.

---

# 8. Threadpools vs Process Pools

## Threadpool
Good for:
- blocking I/O
- legacy sync libraries

Use:
```python
await asyncio.to_thread(...)
```

## Process Pool
Good for:
- CPU-heavy work
- true parallelism

Avoid CPU-heavy work directly in async endpoints.

---

# 9. Uvicorn Workers

Uvicorn workers are:
- separate OS processes
- separate event loops
- separate GILs

Workers improve multicore scaling.

Too many workers can:
- exhaust DB pools
- explode memory usage
- overwhelm downstream systems

---

# 10. Backpressure & Concurrency Limits

Use:
```python
asyncio.Semaphore
```

to limit concurrency.

Examples:
- outbound API limits
- webhook limits
- DB access limits

All queues should eventually be bounded.

---

# 11. Streaming Responses (SSE)

Streaming responses should use:
- async generators
- StreamingResponse

Pattern:

```python
async def generator():
    while True:
        yield ...
        await asyncio.sleep(...)
```

Streaming should:
- stop cleanly on disconnect
- support cancellation properly

---

# 12. WebSockets

WebSockets are:
- long-lived ASGI connections
- bidirectional
- async-native

Important considerations:
- connection management
- slow consumers
- bounded per-client queues
- broadcast architecture

---

# 13. Lifespan Events

Shared async resources should initialize once during startup.

Examples:
- AsyncClient
- Redis pool
- Postgres pool

Use FastAPI lifespan context instead of per-request initialization.

---

# Project Goal

Build a realistic async architecture that demonstrates:

- concurrent API aggregation
- realtime streaming
- websocket broadcasting
- cancellation propagation
- controlled concurrency
- async DB/cache access
- proper lifecycle management
- structured concurrency

---

# Required Project Features

The project should eventually include ALL of these.

---

# Feature 1 — Aggregation Endpoint

Example:

```http
GET /aggregate?q=tesla
```

Behavior:
- concurrently call multiple external APIs
- use TaskGroup
- use httpx.AsyncClient
- apply concurrency limits
- support cancellation properly

Potential APIs:
- weather
- news
- finance
- mock slow APIs

---

# Feature 2 — Streaming Aggregation (SSE)

Stream partial results as they arrive.

Example stream chunks:

```text
data: {"source":"weather","data":...}

data: {"source":"news","data":...}
```

Must demonstrate:
- async generators
- streaming responses
- disconnect cancellation
- backpressure awareness

---

# Feature 3 — WebSocket Live Updates

Example:
```text
/ws/updates
```

Requirements:
- connection registry
- broadcast manager
- proper cleanup on disconnect
- slow consumer isolation
- bounded queues

---

# Feature 4 — Redis Caching

Requirements:
- async Redis usage
- shared connection pool
- request deduplication
- cache-first strategies
- potential request coalescing

---

# Feature 5 — PostgreSQL Async Access

Requirements:
- async DB driver
- connection pooling
- transaction support
- proper lifecycle management

---

# Feature 6 — Webhook Fan-out

Example:
```http
POST /trigger-event
```

Behavior:
- concurrently call many webhooks
- retry failures
- apply timeouts
- use semaphores
- support cancellation

---

# Feature 7 — CPU-heavy Endpoint

Example:
```http
POST /heavy-analysis
```

Purpose:
Demonstrate why async does not solve CPU workloads.

Should compare:
- bad blocking async endpoint
- threadpool offloading
- process pool offloading

---

# Performance & Benchmarking Goals

Eventually benchmark:
- sync vs async
- blocking vs nonblocking
- streaming scalability
- worker count effects

Potential tools:
- wrk
- locust

---

# Preferred Folder Structure (Initial Direction)

Suggested starting structure:

```text
app/
├── api/
├── core/
├── services/
├── websocket/
├── streaming/
├── db/
├── cache/
├── models/
├── schemas/
├── workers/
├── utils/
└── main.py
```

---

# Important Engineering Rules

## DO:
- prefer async-native libraries
- use lifespan-managed shared clients
- use TaskGroup
- use semaphores
- propagate cancellation
- design for failures
- keep requests lightweight
- think in terms of event-driven systems

## DO NOT:
- create AsyncClient per request
- spawn unlimited tasks
- block the event loop
- swallow CancelledError
- assume workflows always complete
- treat async as CPU acceleration

---

# Immediate Next Step

The next task is:

# Design and scaffold the initial Async Data Hub architecture.

This should include:
- project structure
- FastAPI lifespan setup
- shared async clients
- Redis initialization
- Postgres initialization
- reusable HTTP client layer
- concurrency limiter utilities
- websocket connection manager
- SSE streaming utilities
- structured configuration management

The implementation should emphasize:
- correctness
- async architecture quality
- observability
- scalability
- cancellation safety
- production-grade patterns

Do NOT overfocus on CRUD or business logic.
The project is primarily an async systems engineering exercise.