# Findings and open questions

## Blocked: Antithesis credentials (needs you)

`snouty` 0.9.1 is installed in `~/.local/bin`, but has no tenant, registry or
credentials, so no Antithesis run has been launched yet. To unblock, run one of:

```sh
snouty login        # interactive
# or export ANTITHESIS_TENANT, ANTITHESIS_API_KEY and ANTITHESIS_REPOSITORY
```

Then `antithesis/toydb/scripts/launch.sh 12` starts the first short run
(budget for the whole task: 60 compute hours).

## Local observations (not yet confirmed in Antithesis)

### Raft peer connections have no write timeout (low, local Docker artifact)

During a manual local test (`docker network disconnect` + `connect` on a
node), Docker gave the node a different IP when it came back. The other nodes'
outbound Raft connections to the old IP stayed `ESTABLISHED` with a full send
queue: `Server::raft_send_peer` writes with no timeout and only reconnects when
a write fails, so the node stayed cut off (campaigning with ever-higher terms)
until TCP's retransmission timeout (~15 min by default). Antithesis partitions
don't change IPs, so this shouldn't affect runs, but a write timeout or TCP
keepalive on peer sockets would let toyDB recover from a blackholed peer
connection promptly. toyDB's docs already scope reliability to crash failures.

### Orphaned transactions after a node crash (expected limitation)

From reading the code: a client's open transaction is recorded in MVCC's active set via Raft. If the
node holding the session crashes, nothing rolls it back, so its writes stay
invisible and every later write to the same keys fails with a serialization
error. The workload doesn't assert that writes eventually succeed, so this is
not reported as a failure; it may show up as `Sometimes`/`Reachable`
assertions about successful writes being hit less often. The eventually's
`transactions from crashed sessions remain active once faults stop` assertion
shows whether runs reach this state.
