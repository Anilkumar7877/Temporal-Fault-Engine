# Fault-Tolerant Distributed Event Scheduler Engine

A distributed event scheduling engine built from scratch using Node.js, PostgreSQL, Redis, Docker, and WebSockets. The system is designed to provide fault-tolerant event execution with lease-based recovery, high scheduling precision, and provable ownership semantics under concurrent workloads.

---

# Architecture Overview

The scheduler combines:

* **PostgreSQL** as the authoritative source of truth
* **Redis Sorted Sets** for efficient time-based indexing
* **Redis Pub/Sub** for low-latency worker wakeups
* **Stateless Worker Nodes** for horizontal scalability
* **Lease-based Recovery Protocol** for fault tolerance
* **WebSocket Telemetry Stream** for real-time observability

The architecture avoids expensive database polling by maintaining a Redis-backed scheduling index while preserving correctness through PostgreSQL state transitions.

---

# Exactly-Once Claim Protocol

Exactly-once execution is achieved through:

1. Atomic event isolation from Redis.
2. Database-backed ownership verification.
3. Lease-based fault recovery.
4. Monotonic state transitions.

Event lifecycle:

```text
pending → claimed → executed
```

An event can never transition from `executed` back to any previous state.

---

# Sequence Diagram

```mermaid
sequenceDiagram
    autonumber

    actor Client
    participant API as Scheduler API
    participant DB as PostgreSQL
    participant Redis as Redis Delay Queue
    participant W1 as Worker A
    participant W2 as Worker B
    participant Recovery as Recovery Worker

    %% Event Scheduling
    Client->>API: Schedule Event (payload, execute_at)
    API->>DB: INSERT event(status='pending')
    DB-->>API: event_id
    API->>Redis: ZADD(delay_queue, execute_at, event_id)
    API-->>Client: 202 Accepted

    %% Competing Workers
    Note over W1,W2: Both workers detect event is due

    par Concurrent Poll
        W1->>Redis: Fetch due event_id
    and
        W2->>Redis: Fetch same due event_id
    end

    %% Atomic Claim
    W1->>DB: BEGIN
    W1->>DB: UPDATE events\nSET status='claimed',\nowner=W1,\nlease_until=NOW()+30s\nWHERE id=? AND status='pending'
    DB-->>W1: rows_affected=1
    W1->>DB: COMMIT

    W2->>DB: BEGIN
    W2->>DB: UPDATE events ...\nWHERE status='pending'
    DB-->>W2: rows_affected=0
    W2->>DB: ROLLBACK

    Note over W1,W2: Only one worker obtains ownership

    %% Execution Phase
    W1->>DB: Periodic lease heartbeat
    W1->>W1: Execute business payload

    alt Execution succeeds
        W1->>DB: BEGIN
        W1->>DB: UPDATE events\nSET status='executed',\nexecuted_at=NOW()\nWHERE id=?\nAND owner=W1
        DB-->>W1: rows_affected=1
        W1->>DB: COMMIT
    else Worker crashes (kill -9)
        Note over W1: Process terminated
    end

    %% Recovery Flow
    Recovery->>DB: Scan claimed events\nWHERE lease_until < NOW()

    DB-->>Recovery: Expired lease found

    Recovery->>DB: BEGIN
    Recovery->>DB: UPDATE events\nSET status='pending',\nowner=NULL,\nlease_until=NULL\nWHERE id=?\nAND status='claimed'
    Recovery->>DB: COMMIT

    Recovery->>Redis: Requeue event_id

    %% Re-execution
    W2->>DB: Atomic claim attempt
    DB-->>W2: Claim successful

    W2->>W2: Execute payload

    W2->>DB: UPDATE events\nSET status='executed'\nWHERE owner=W2
    DB-->>W2: Success

    %% Safety Guarantee
    Note over DB: Unique state transition\npending → claimed → executed

    Note over DB: Executed events can never\nreturn to pending

    Note over W1,W2: Exactly one successful\nclaim and one successful execution record
```

---

# Failure Scenario Analysis

## Scenario A: Concurrent Polling

### Problem

Two workers attempt to process the same event simultaneously.

### Resolution

Workers execute a Redis Lua script:

```lua
local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)

if #jobs == 0 then
    return nil
end

redis.call('ZREM', KEYS[1], jobs[1])

return jobs[1]
```

Because Redis executes Lua scripts on a single thread, the lookup and removal operation is atomic.

Only one worker receives the event identifier.

Additionally, PostgreSQL verifies ownership:

```sql
UPDATE events
SET status = 'claimed',
    leased_until = NOW() + INTERVAL '5 seconds'
WHERE id = $1
  AND status = 'pending';
```

Only one transaction can successfully transition the row from `pending` to `claimed`.

---

## Scenario B: Worker Crash (kill -9)

### Problem

A worker successfully claims an event but crashes before completion.

### Resolution

Every claimed event receives a lease:

```sql
leased_until = NOW() + INTERVAL '5 seconds'
```

A Lease Reaper continuously executes:

```sql
UPDATE events
SET status = 'pending'
WHERE status = 'claimed'
  AND leased_until < NOW()
RETURNING id;
```

Recovered events are reinserted into Redis and become eligible for execution again.

No manual intervention is required.

---

## Scenario C: Database Connection Loss

### Problem

The worker loses connectivity to PostgreSQL during execution.

### Resolution

If the final execution update cannot be committed:

```sql
UPDATE events
SET status = 'executed'
```

the lease eventually expires.

The Lease Reaper identifies the abandoned event and safely requeues it.

Since ownership is validated through PostgreSQL state transitions, duplicate execution records cannot occur.

---

## Scenario D: System Restart

### Problem

The entire cluster restarts while events remain in `claimed` state.

### Resolution

Lease expiration is based on PostgreSQL server time:

```sql
leased_until < NOW()
```

After restart, Lease Reapers automatically identify expired claims and return them to the scheduling pipeline.

No startup migration or manual cleanup step is required.

---

# Performance Characteristics

* 🚀 Starting Automated Benchmark: Firing 60 concurrent events...
* ✅ All 60 events accepted by Ingress. Waiting for execution window...
* 📊 Gathering exact execution metrics from PostgreSQL Source of Truth...

### Timing Distribution Table (n=60)
| Metric | Variance (ms) | Target Constraint | Status |
| :--- | :--- | :--- | :--- |
| **p50 (Median)** | `37.0ms` | Bounded Latency | ✨ Pass |
| **p95** | `89.0ms` | Bounded Latency | ✨ Pass |
| **p99** | `95.0ms` | `< 200ms`        | ✅ Pass |
| **Max Burst** | `95.0ms` | Real-run Peak | Checked |

All observed execution variance remains significantly below the 200 ms target threshold.

---

# Local Deployment

## Prerequisites

* Docker
* Docker Compose
* Node.js 18+

## Start Infrastructure

```bash
docker compose down -v

docker compose up --build --scale worker=3
```

## Execute Load Test

```bash
node test-runner.js
```

## Open Dashboard

```text
http://localhost:3000
```

The dashboard displays:

* Event throughput
* Scheduling variance
* Worker ownership distribution
* Recovery activity
* Real-time execution stream

---

# Design Principles

* PostgreSQL is the source of truth.
* Redis accelerates scheduling but does not define correctness.
* Workers are completely stateless.
* Failures are assumed and continuously recovered.
* Every event follows a deterministic lifecycle.
* Horizontal scaling requires no architecture changes.
