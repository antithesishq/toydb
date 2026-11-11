# Findings and open questions

## Run 1: `40d6c82e6acbe14b6ea1dc1f84d7158a-67-0` (12 compute hours)

### BUG: restarted Raft leader panics on requests forwarded before the crash

`a toyDB node never panics` fails (10 examples; nodes exit 101 via the panic
hook). Example moment `-1606378430519816350` @ vtime 26.63:

```
26.6317 [toydb1] FAIL Unreachable "a toyDB node never panics"
        details {"location":"src/raft/node.rs:470:17",
                 "message":"assertion `left == right` failed: client request from other node\n  left: 4\n right: 1"}
```

Timeline: toydb1 is leader (it runs leader-only `maybe_send_append` until
23.40) → killed at 23.43 → restarted at 26.25. On restart it reloads its
persisted term but becomes a follower. toydb4 had queued forwarded
`ClientRequest`s for toydb1 in its outbound peer channel (stamped with that
term) and delivers them once it reconnects. toydb1, now a *follower in the same
term*, hits `assert_eq!(msg.from, self.id, "client request from other node")`
in `RawNode<Follower>::step` and panics.

Suggested fix: a follower receiving a `ClientRequest` from another node should
reply `ClientResponse { response: Err(Error::Abort) }` to the sender (as a
candidate already does) instead of asserting.

### BUG: followers can't reach a restarted peer for ~15 min (stale Raft sockets)

`every node's Raft log catches up with the leader once faults stop` fails (15
examples). Example moment `-8949651209012332786` @ vtime 109.47: faults stopped
at 21.97, toydb4 won term 7 at 28.58, and 80 s later the leader still reports
`match={1: 80, 2: 0, 3: 0, 4: 80, 5: 80}`; every request through toydb2 times
out.

`snouty runs exec` at vtime 49.44 shows why. toydb4 and toydb5 were restarted
during the faults and came back with **new IPs** (`.9`, `.8`, previously `.2`,
`.3`). toydb2's and toydb3's outbound Raft connections still point at the old
IPs, stuck in `CLOSE-WAIT` with unsent data, and neither has any connection to
the new leader:

```
toydb2: CLOSE-WAIT 1 689  10.89.0.5:37406 10.89.0.3:9701
toydb2: CLOSE-WAIT 1 10   10.89.0.5:39204 10.89.0.2:9701
toydb4 (leader, .9): inbound Raft only from .8 and .4
toysql via toydb2: hangs;  via toydb1: returns 8
```

`Server::raft_send_peer` only writes; it never reads the socket (so it misses
the peer's FIN), sets no write timeout or TCP keepalive, and resolves the peer
address only when a write fails. Writes to a vanished IP get no RST, so they
"succeed" into the send buffer until TCP gives up (~15 min). Until then the
follower's responses to the new leader are lost: it never catches up, and
client requests through it hang. (I first saw this locally and wrongly assumed
only Docker changes IPs on restart; Antithesis does too.)

Suggested fix: on each send failure *or* periodically (e.g. when no ack/heartbeat
traffic arrives from that peer for a few election timeouts), drop and re-dial
the connection; also set a socket write timeout and TCP keepalive, and/or have
the sender detect EOF on its socket.

### Under investigation: committed inserts missing

`acknowledged inserts are visible to every later read` and `a delete of a
committed batch removes exactly its rows` fail together: a batch whose COMMIT
was acknowledged is missing from later index reads, and a primary-key DELETE of
it removes 0 rows (e.g. ids `[17,18,19]` at moment `13304024868958580` @ 18.51,
with no leader change; and `[146]` at moment `3297190298469261901` @ 21.13).
Root cause not yet determined (SUT vs workload model).

### Workload fixes after run 1

- The final-phase reads (`a kv/bank/item read fails once faults stop`) asserted
  on the first failed request. toyDB doesn't retry forwarded requests and
  expects clients to time out, so one timeout right after recovery isn't a
  violation. They now retry for up to 30 s and report the errors seen.

## Local observations (not yet confirmed in Antithesis)

### Raft peer connections have no write timeout (confirmed in Antithesis, see run 1)

During a manual local test (`docker network disconnect` + `connect` on a
node), Docker gave the node a different IP when it came back. The other nodes'
outbound Raft connections to the old IP stayed `ESTABLISHED` with a full send
queue: `Server::raft_send_peer` writes with no timeout and only reconnects when
a write fails, so the node stayed cut off (campaigning with ever-higher terms)
until TCP's retransmission timeout (~15 min by default). Antithesis restarts
containers with new IPs too, so this does affect runs (see run 1).

### Orphaned transactions after a node crash (expected limitation)

From reading the code: a client's open transaction is recorded in MVCC's active set via Raft. If the
node holding the session crashes, nothing rolls it back, so its writes stay
invisible and every later write to the same keys fails with a serialization
error. The workload doesn't assert that writes eventually succeed, so this is
not reported as a failure; it may show up as `Sometimes`/`Reachable`
assertions about successful writes being hit less often. The eventually's
`transactions from crashed sessions remain active once faults stop` assertion
shows whether runs reach this state.
