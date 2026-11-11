# Antithesis workloads for toyDB

## `toydb/`: a 5-node cluster under faults

**What it tests.** Five toyDB nodes (Raft + Bitcask, fsync on) and one driver
container. Antithesis kills, stops and partitions the nodes
(`--fault-tolerance high_availability`); the driver is excluded from faults.

**The driver** (`toydb/driver/`, Rust) is a [hegel](https://hegel.dev)
concurrent state machine that uses toyDB's own `Client` in-process. Each rule
draws a node and sends SQL to it; outcomes are recorded in a model:

| Rule | SQL | Model / check |
| --- | --- | --- |
| `put` / `get` | `UPDATE kv …` / `SELECT value FROM kv …` (implicit txns) | per-key write history; reads must be linearizable |
| `transfer` | `BEGIN; SELECT; UPDATE; UPDATE; COMMIT` | bank: money is conserved, never overdrawn |
| `read_bank`, `read_bank_as_of` | `BEGIN READ ONLY [AS OF SYSTEM TIME v]` + aggregate and scan | snapshot consistency, time travel |
| `insert_batch` | 1–3 `INSERT`s in one txn; 10% abandoned mid-txn | committed ⇒ durable & visible; never-committed ⇒ never visible; all-or-nothing |
| `delete_batch` | `DELETE FROM item WHERE id = … OR …` | deleted rows never reappear |
| `read_group` | `SELECT id FROM item WHERE grp = g` (secondary index) | all of the item checks above |
| `eventually` | (rare) stops faults, waits for recovery | every node serves; acked writes survive; bank consistent; Raft logs converge; then exits 0 |

Requests that time out or are aborted by a leader change are recorded as
*unknown* outcomes and may or may not take effect.

**Properties** (each SDK assertion is one property): guarantee assertions are
`assert_always` / `assert_unreachable` in `driver/src/main.rs`, plus
`a toyDB node never panics` (a panic hook in `src/bin/toydb.rs` that turns
toyDB's many internal Raft/storage `assert!`s into a property). Exploration
assertions live in the driver and in toyDB itself (`src/raft/`, `src/storage/`,
`src/server.rs`), marking elections, step-downs, log truncation, follower lag,
Bitcask recovery/compaction and MVCC conflicts.

**toyDB source changes** (for the Antithesis build): the `antithesis_sdk`
dependency and assertions, an optional `antithesis-instrumentation` feature
(coverage instrumentation, enabled only by `toydb/toydb/Dockerfile`), and
`Client::connect_timeout` so the driver can bound every request. Outside
Antithesis the SDK is a no-op.

### Layout

```text
toydb/config/       docker-compose.yaml (+ coverage override for the local loop)
toydb/driver/       driver crate, Dockerfile, first_start_workload test command
toydb/toydb/        instrumented toyDB image + entrypoint (writes per-node config)
toydb/scripts/      launch.sh, local-run.sh, coverage-report.sh
```

### Build, run locally, launch

```sh
cd antithesis/toydb
(cd config && docker compose build && snouty validate .)    # build + validate
scripts/local-run.sh 300                    # local loop, evidence in runs/<ts>/
EVENTUALLY_AFTER=120 scripts/local-run.sh 200   # also exercise the eventually
scripts/coverage-report.sh                  # LLVM coverage of the last local run
scripts/launch.sh 12                        # Antithesis run with 12 compute hours
```

Driver knobs: `TOYDB_NODES`, `TOYDB_EVENTUALLY=off`, `TOYDB_EVENTUALLY_AFTER=<secs>`.

### Known findings / workarounds

See [`FINDINGS.md`](FINDINGS.md).
