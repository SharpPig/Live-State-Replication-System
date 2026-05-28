# Live State Replication System

### You can open example_run.pdf to see it in action

A single writer, multi reader distributed state machine built for live score and odds propagation. The core service owns all writes and persists them to a dual **Write-Ahead Log (WAL)** before applying to memory. Replicas subscribe over WebSocket, maintain their own in-memory copies, and serve read traffic. A separate dashboard polls different replicas and renders live scoreboards.

I built this because one thing I notice constantly on both Kalshi and Polymarket US is that the app experience for score and odds synchronization can be laggy or outdated. Scores and odds need to flow fast, and the read path needs to scale independently of the write path. This system separates those concerns cleanly.

```
                        writes (HTTP POST)
                              |
                              v
                      ┌──────────────────┐
                      │      core        │
                      │  localhost:8001  │
                      │                  │
                      │  StateMachine    │
                      │   (in-memory)    │
                      │       |          │
                      │       v          │
                      │  ┌──────────┐    │
                      │  │ DualWAL  │    │
                      │  │  (disk)  │    │
                      │  └┬─────┬───┘    │
                      │   |     |        │
                      └───┼─|───┼────────┘
                          | |   |
               ┌──────────┘ |   └────────────┐
               v            |                 v
      wal_primary.jsonl     |      wal_secondary.jsonl
                            |
                     WebSocket feed
                  (snapshot + live ops)
                            |
              ┌─────────────┼─────────────┐
              v             v             v
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │replica-1 │  │replica-2 │  │replica-3 │
        │  :8002   │  │  :8003   │  │  :8004   │
        │ (in-mem) │  │ (in-mem) │  │ (in-mem) │
        └────┬─────┘  └─────┬────┘  └──────┬───┘
             │              │              │
             └──────────┬───┘──────────────┘
                        │
                   reads (HTTP GET)
                        │
                        v
               ┌─────────────────┐
               │    dashboard    │
               │     :8080       │
               │  (auto-discover │
               │   replicas via  │
               │   port scan)    │
               └─────────────────┘
```

---

## What the State Machine Models

A versioned key value store where each key represents a game. Every change to a game bumps the version and records a clock timestamp, so replicas can detect ordering and staleness. 

Each entry also tracks:
- **version** -- key, incremented on every write
- **ts** -- unix epoch, when this version was written
- **op_id** -- unique 12-char hex id per mutation, can be used for dedup
- **ended_at** -- unix epoch when the game ended (0.0 if game is live)

So the complete state entry looks like:

```json
{
  "key": "warriors_vs_heat_2024_03_08",
  "value": {
    "home": "GSW",
    "away": "MIA", 
    "score": [118, 110],
    "home_odds": "-98",
    "away_odds": "+98"
  },
  "version": 6,
  "ts": 1773016488.01,
  "op_id": "7746257bf506",
  "ended_at": 1773016488.01
}
```

The core doesn't care what's inside the value. It just versions it, persists it to disk, and pushes it out. This means the same system could model moneyline ticks, game boards, stock price feeds, match scores, or anything else that changes over time.

---

## How Replication Works

### Data Flow

1. Client writes to core via `POST /state` with an admin key
2. Core appends the operation to both WAL files on disk (`fsync` after each write)
3. Core applies the operation to its in-memory state
4. Core pushes the JSON over WebSocket to every connected replic
5. Replica applies the operation to its own in-memory state
6. Dashboard reads from a randomly-selected replica

The WAL write happens **before** the in-memory apply. This is was on purpose. If the process crashes between the WAL write and the memory apply, we wont lose data and the WAL replays on restart and rebuilds state. If we did it the other way around, a crash after memory apply but before WAL write would mean the in-memory state existed briefly but was never persisted. In a system where score accuracy matters, that's not acceptable.

### Replica Connection Lifecycle

1. Replica opens a WebSocket to `core:8001/replicate?token=replica-secret`
2. Core sends a **full snapshot** of current state (action=`snapshot`) - every key, every value, one message per key
3. Core then streams **live ops** (action=`put`/`delete`) in real-time as they happen
4. If the connection drops, replica reconnects with exponential back offs (1s --> 2s --> 4s --> ... capped at 30s)

I thought about whether the core should send recovered states to replicas all at once or stream them. My implementation streams the snapshot one WebSocket message per key. This means replicas receive the state incrementally but don't serve reads until the snapshot is fully applied. The trade off is that this approach uses less memory than buffering everything, but takes longer for large states.

### Wire Format

Every WebSocket message is a single JSON object:

```json
{
  "action": "put",
  "key": "warriors_vs_heat_2024_03_08",
  "value": {"home": "GSW", "away": "MIA", "score": [118, 110], "home_odds": "-98", "away_odds": "+98"},
  "version": 6,
  "ts": 1773016488.01,
  "op_id": "7746257bf506",
  "ended_at": 1773016488.01
}
```

Actions: `snapshot` (initial sync), `put` (create/update), `delete` (remove).

### Consistency

**Eventual consistency.** Replicas may lag behind core by the network round-trip time. In practice on localhost this is <1ms, but may be longer on production networks. Each entry carries a timestamp so the caller knows exactly how stale the data is.

There is no read-writes guarantee across services, but for this application I found it to be suitable. If you write to core and immediately read from a replica (within the network round-trip time), you might miss the core write. This is the right trade-off for this read system like a sbook where:
- Odds and scores update frequently but millisecond staleness is acceptable
- Read throughput matters far more than perfect latency consistency

---

## Persistence: The Write-Ahead Log (WAL)

### What is the WAL?

The WAL is a JSON file (`*.jsonl`) where every state mutation is appended before it's applied in memory. On startup, the core replays the WAL line-by-line to rebuild its in-memory state. This gives durability without the complexity of a full database.

I really considered the ordering here and landed on: **append to disk first, then apply to memory**. If the process dies mid-write, we have the WAL. If the WAL write fails, the in-memory state was never updated, so we stay consistent. **The `fsync` sys call after every write ensures the OS actually flushes to disk rather than buffering in kernel space.**

### Dual WAL (Redundant Persistence)

The system writes to two WAL files simultaneously:

- **`logs/wal_primary.jsonl`** -- the primary WAL, read first on replay
- **`logs/wal_secondary.jsonl`**-- the secondary WAL, used as fallback (can be stored offsite)

On startup, the `DualWAL` checks:
- If primary is missing but secondary exists → recovers primary from secondary
- If secondary is missing but primary exists → recovers secondary from primary
- If both exist → replays from primary, falls back to secondary if primary is corrupt

By using separate WALs we get some persistence redundancy. Ideally in production I'd follow the **3-2-1 backup rule**: 3 total copies of the state, across 2 media types, with 1 copy stored off-site. The dual WAL is a step in that direction.

### WAL Sync Detection

If one WAL gets out of sync with the other (e.g., a partial write failure, manual editing, disk error), the **admin** can manually repair it:

```bash
# Check if WALs are in sync
curl -H "X-Admin-Key: admin-secret" http://localhost:8001/wal/sync

# Force sync secondary from primary (primary is source of truth)
curl -X POST -H "X-Admin-Key: admin-secret" http://localhost:8001/wal/sync/primary

# Force sync primary from secondary (if primary is corrupted)
curl -X POST -H "X-Admin-Key: admin-secret" http://localhost:8001/wal/sync/secondary
```

The sync check compares **operation count** and the **timestamp of the last operation** in each file.

---

## Log Files

All services write structured logs to both stdout and `logs/`. Future functionality would be to make a script that combines all of these into a single `TIMELINE` for easier analysis. Each log file corresponds to a module:

| File | What it captures |
|---|---|
| `logs/core_server.log` | HTTP requests, auth rejections, WebSocket connections |
| `logs/core_state.log` | Every put/delete, version bumps, replica fan-out counts |
| `logs/core_wal.log` | WAL opens, replays, recovery events, sync checks |
| `logs/replica_server.log` | Replica HTTP server lifecycle |
| `logs/replica_replicator.log` | WebSocket connects/disconnects, ops applied, lag measurements |
| `logs/dashboard_app.log` | Replica discovery, cache refreshes, fetch failures |
| `logs/wal_primary.jsonl` | Primary WAL -- every mutation as newline-delimited JSON |
| `logs/wal_secondary.jsonl` | Secondary WAL -- mirror of primary for redundancy |

Log format is timestamp first and structured for easy greping:

```
2026-03-08 19:22:40.183  INFO   core.wal  opened  path=logs/wal_primary.jsonl  size=0 bytes
2026-03-08 19:22:40.183  INFO   core.state  state recovered  keys=2  ops_replayed=12
2026-03-08 19:26:15.795  INFO   replica.replicator  connecting  attempt=1  url=ws://localhost:8001/replicate
```

---

## Running the System

This is a **multi-terminal** system. Each service runs in its own terminal window. You need at minimum 3 terminals (core + 1 replica + dashboard), but the system supports up to 1,000 replicas, it could theoretically support up to the port limit of your OS.

### Prerequisites

```bash
pip install -r requirements.txt
```

Python 3.8+, I used python 3.8.10.

### Startup Sequence

**Terminal 1 -- Core (single writer, port 8001):**
```bash
python3 -m core.server
```

**Terminal 2 -- Replica 1 (auto-assigns port 8002):**
```bash
python3 -m replica.server
```

**Terminal 3 -- Replica 2 (auto-assigns port 8003):**
```bash
python3 -m replica.server
```

Each replica automatically finds the next available port starting from 8002. You can also specify a port manually:
```bash
PORT=8005 python3 -m replica.server
```

**Terminal 4 -- Dashboard (port 8080):**
```bash
python3 -m dashboard.app
```

The dashboard auto-discovers running replicas by scanning ports 8002-9002. You can add or remove replicas while the dashboard is running -- it picks them up automatically on the next cache refresh (every 1 second).

### Admin API Endpoints

All admin endpoints require the `X-Admin-Key` header.

**To create or update a game:**
```bash
curl -X POST http://localhost:8001/state \
  -H "X-Admin-Key: admin-secret" \
  -H "Content-Type: application/json" \
  -d '{
    "key": "warriors_vs_heat_2024_03_08",
    "value": {
      "home": "GSW",
      "away": "MIA",
      "score": [52, 45],
      "home_odds": "-105",
      "away_odds": "+105"
    },
    "end_game": false
  }'
```

**To end a game set end_game to true which sets `ended_at` timestamp:**
```bash
curl -X POST http://localhost:8001/state \
  -H "X-Admin-Key: admin-secret" \
  -H "Content-Type: application/json" \
  -d '{
    "key": "warriors_vs_heat_2024_03_08",
    "value": {
      "home": "GSW",
      "away": "MIA",
      "score": [118, 110],
      "home_odds": "-98",
      "away_odds": "+98"
    },
    "end_game": true
  }'
```

**Read a specific game from a replica:**
```bash
curl http://localhost:8002/state/warriors_vs_heat_2024_03_08
```

**Read all states from a replica:**
```bash
curl http://localhost:8002/state
```

**Delete a game:**
```bash
curl -X DELETE http://localhost:8001/state/warriors_vs_heat_2024_03_08 \
  -H "X-Admin-Key: admin-secret"
```

**Core metrics:**
```bash
curl -H "X-Admin-Key: admin-secret" http://localhost:8001/metrics
```

**WAL sync check:**
```bash
curl -H "X-Admin-Key: admin-secret" http://localhost:8001/wal/sync
```

**Health check (no auth):**
```bash
curl http://localhost:8001/
```

### Example Game Simulations

Two example scripts simulate full game lifecycles with 10 score updates each. Be sure to run the **dashboard** and core servers before running these scripts:

```bash
# LOY vs BYU -- 10 score updates + game end
python3 loy_byu_example.py

# Warriors vs Heat -- 10 score updates + game end
python3 warriors_vs_heat_example.py
```

These write to the core, which replicates to all connected replicas, which the dashboard picks up automatically.

---

## Project Structure

```
novig/
├── core/
│   ├── __init__.py
│   ├── server.py          # HTTP + WebSocket endpoints, auth middleware
│   ├── state.py           # StateMachine -- single-writer KV store with fan-out
│   ├── wal.py             # WAL and DualWAL -- disk persistence and recovery
│   └── log.py             # Structured logging config (console + file)
├── replica/
│   ├── __init__.py
│   ├── server.py          # Read-only HTTP endpoints, auto port finding
│   ├── replicator.py      # WebSocket consumer, reconnect with backoff
│   └── log.py             # Logging config
├── dashboard/
│   ├── __init__.py
│   ├── app.py             # Score dashboard, replica discovery, caching
│   ├── templates/
│   │   └── dashboard.html # Jinja2 template with live-update JS
│   └── log.py             # Logging config
├── logs/
│   ├── wal_primary.jsonl   # Primary WAL
│   ├── wal_secondary.jsonl # Secondary WAL
│   ├── core_*.log          # Core service logs
│   ├── replica_*.log       # Replica service logs
│   └── dashboard_*.log     # Dashboard logs
├── loy_byu_example.py      # Game simulation script
├── warriors_vs_heat_example.py  # Game simulation script
├── run_dashboard.py        # Dashboard launcher
├── requirements.txt        # Python dependencies
└── README.md
```

---

## Key Trade-offs and My Reasoning

| Decision | Why |
|---|---|
| **WebSocket over polling** | Lowest latency for replication. No wasted requests when state is idle. I considered UDP for broadcast -- it would be faster for score/odds updates across many replicas in production, but since this is a state machine I cannot have packet loss for replicas. UDP would break these guarantees. |
| **Snapshot then send to replicas** | New replicas get consistent state immediately, then stay current. No need for a separate catch-up protocol. |
| **Single writer (core)** | Eliminates write conflicts entirely.|
| **WAL before memory** | The WAL appends to disk before applying to the in-memory state. We always keep a persistent snapshot of score state. If the process crashes, we lose nothing. |
| **Dual WAL** | Redundant persistence. If one storage volume gets corrupted or deleted, the system recovers from the other on startup. Inspired by the **3-2-1 data backup principle.** |
| **Dashboard separated from replicas** | I thought about having each node serve its own dashboard, but API services shouldn't be serving frontend UIs. Separation of concepts keeps the read path clean and the dashboard independently deployable. |
| **Dynamic replica discovery** | The dashboard scans ports to find replicas instead of using a hardcoded list. You can spin up or tear down replicas without touching config. |
| **Conditional page refresh** | The dashboard JS checks for score changes via `/api/games` every second. It only reloads the page if something actually changed. This minimizes unnecessary network traffic. |

---
