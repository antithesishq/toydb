//! Antithesis workload driver for a 5-node toyDB cluster.
//!
//! A hegel concurrent state machine. Each rule sends SQL to a drawn node and
//! records the outcome in a model, against which assertions judge responses:
//!
//! * kv: single-row registers, checked for linearizable reads.
//! * account: a bank, whose transfers must preserve the total balance and
//!   never overdraw, also under time-travel (AS OF) reads.
//! * item: rows inserted in transactional batches and read back through a
//!   secondary index: committed batches are durable, uncommitted ones are
//!   never visible, and each batch is visible all together or not at all.
//!
//! An eventually stops faults, waits for recovery, checks durability and
//! convergence, and ends the timeline.

mod cluster;
mod model;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use antithesis_sdk::{
    assert_always, assert_reachable, assert_sometimes, assert_sometimes_less_than_or_equal_to,
    assert_unreachable,
};
use antithesis_instrumentation as _;
use hegel::TestCase;
use hegel::generators as gs;
use serde_json::json;
use toydb::StatementResult;
use toydb::error::{Error, Result};

use cluster::{Cluster, int_rows, is_ambiguous};
use model::{BatchState, DeleteState, Items, KeyHistory};

const KV_KEYS: i64 = 8;
const ACCOUNTS: i64 = 8;
const INITIAL_BALANCE: i64 = 100;
const TOTAL_BALANCE: i64 = ACCOUNTS * INITIAL_BALANCE;
const GROUPS: i64 = 8;
/// Commit versions kept for time-travel reads.
const VERSIONS_KEPT: usize = 64;
/// Chance per call that the eventually rule fires.
const EVENTUALLY_P: f64 = 0.001;
/// How long each read in the eventually retries before it counts as failed.
const FINAL_READ_DEADLINE: Duration = Duration::from_secs(30);
/// How long the eventually waits for the cluster to recover.
const RECOVERY_DEADLINE: Duration = Duration::from_secs(300);

fn log(msg: impl AsRef<str>) {
    println!("[driver] {}", msg.as_ref());
}

struct Shared {
    cluster: Cluster,
    origin: Instant,
    next_value: AtomicI64,
    next_item: AtomicI64,
    kv: Vec<Mutex<KeyHistory>>,
    versions: Mutex<VecDeque<u64>>,
    items: Mutex<Items>,
    /// Set once an eventually starts: other rules stop sending requests.
    quiescing: AtomicBool,
    eventually_lock: Mutex<()>,
    eventually_enabled: bool,
}

struct Workload {
    s: Arc<Shared>,
}

/// Records an unexpected error's kind for exploration, and a Raft abort.
fn note_error(error: &Error, op: &str) {
    if matches!(error, Error::Abort) {
        assert_reachable!("a request is aborted by a Raft leader change", &json!({"op": op}));
    }
    if matches!(error, Error::InvalidInput(_)) {
        // The driver only sends valid SQL, so this is unexpected; surface it.
        assert_unreachable!(
            "the driver's SQL is never rejected as invalid input",
            &json!({"op": op, "error": error.to_string()})
        );
    }
}

impl Shared {
    fn ms(&self, t: Instant) -> u64 {
        t.saturating_duration_since(self.origin).as_millis() as u64
    }

    // ---- kv ---------------------------------------------------------------

    fn put(&self, node: usize, key: i64) {
        let value = self.next_value.fetch_add(1, Ordering::SeqCst);
        self.kv[key as usize].lock().unwrap_or_else(|e| e.into_inner()).record_sent(value, Instant::now());
        let result =
            self.cluster.execute(node, &format!("UPDATE kv SET value = {value} WHERE id = {key}"));
        let now = Instant::now();
        let mut history = self.kv[key as usize].lock().unwrap_or_else(|e| e.into_inner());
        match result {
            Ok(StatementResult::Update { count }) => {
                assert_always!(
                    count == 1,
                    "an update of an existing kv row updates exactly one row",
                    &json!({"node": node, "key": key, "count": count})
                );
                history.record_acked(value, now);
            }
            Ok(other) => assert_unreachable!(
                "an UPDATE returns an update result",
                &json!({"node": node, "result": format!("{other:?}")})
            ),
            Err(error) if is_ambiguous(&error) => {
                note_error(&error, "put");
                assert_reachable!("a kv put has an unknown outcome", &json!({"node": node, "error": error.to_string()}));
            }
            Err(error) => {
                note_error(&error, "put");
                history.record_failed(value);
            }
        }
        history.prune(now);
    }

    /// Reads a key and checks the value against its history. Returns the
    /// error if the read failed.
    fn get(&self, node: usize, key: i64, message_final: bool) -> std::result::Result<(), String> {
        let start = Instant::now();
        let result = self
            .cluster
            .execute(node, &format!("SELECT value FROM kv WHERE id = {key}"))
            .and_then(int_rows);
        let end = Instant::now();
        let rows = match result {
            Ok(rows) => rows,
            Err(error) => {
                note_error(&error, "get");
                return Err(error.to_string());
            }
        };
        let got = match rows.as_slice() {
            [row] => row.first().copied().flatten(),
            _ => None,
        };
        let history = self.kv[key as usize].lock().unwrap_or_else(|e| e.into_inner());
        let ok = got.is_some_and(|v| history.may_return(v, start, end));
        let details = json!({
            "node": node, "key": key, "rows": format!("{rows:?}"),
            "start_ms": self.ms(start), "end_ms": self.ms(end),
            "history": history.describe(self.origin),
        });
        if message_final {
            assert_always!(ok, "acknowledged kv writes survive once faults stop", &details);
        } else {
            assert_always!(
                ok,
                "a kv read returns the latest acknowledged write or one that overlapped it",
                &details
            );
        }
        if let Some(v) = got {
            assert_sometimes!(
                history.is_unacked(v),
                "a kv read observes a write whose client never got an acknowledgement",
                &json!({"node": node, "key": key, "value": v})
            );
        }
        Ok(())
    }

    // ---- bank -------------------------------------------------------------

    fn transfer(&self, node: usize, from: i64, to: i64, amount: i64) {
        let mut client = match self.cluster.client(node) {
            Ok(client) => client,
            Err(_) => return,
        };
        let mut commit_sent = false;
        let result: Result<Option<u64>> = (|| {
            client.execute("BEGIN")?;
            let balance = int_rows(
                client.execute(&format!("SELECT balance FROM account WHERE id = {from}"))?,
            )?;
            let balance = balance.first().and_then(|r| r.first().copied().flatten());
            let Some(balance) = balance else {
                return Err(Error::InvalidData(format!("account {from} not found")));
            };
            if balance < amount {
                client.execute("ROLLBACK")?;
                return Ok(None);
            }
            client.execute(&format!(
                "UPDATE account SET balance = balance - {amount} WHERE id = {from}"
            ))?;
            client.execute(&format!(
                "UPDATE account SET balance = balance + {amount} WHERE id = {to}"
            ))?;
            commit_sent = true;
            match client.execute("COMMIT")? {
                StatementResult::Commit { version } => Ok(Some(version)),
                other => Err(Error::InvalidData(format!("unexpected commit result {other:?}"))),
            }
        })();
        let details = json!({"node": node, "from": from, "to": to, "amount": amount});
        match &result {
            Ok(Some(version)) => {
                assert_reachable!("a bank transfer commits", &details);
                let mut versions = self.versions.lock().unwrap_or_else(|e| e.into_inner());
                versions.push_back(*version);
                if versions.len() > VERSIONS_KEPT {
                    versions.pop_front();
                }
            }
            Ok(None) => assert_reachable!("a bank transfer is rejected for insufficient funds", &details),
            Err(Error::Serialization) => {
                assert_reachable!("a bank transfer fails with a serialization conflict", &details)
            }
            Err(error) => {
                note_error(error, "transfer");
                if commit_sent && is_ambiguous(error) {
                    assert_reachable!("a bank transfer's commit has an unknown outcome", &details);
                }
            }
        }
        if result.is_err() && client.txn().is_some() && !commit_sent {
            client.execute("ROLLBACK").ok();
        }
        match &result {
            Err(Error::IO(_) | Error::InvalidData(_)) => {}
            _ => self.cluster.release(node, client),
        }
    }

    /// Reads the whole bank in one snapshot (optionally as of a past version)
    /// and checks that no money was created, destroyed or overdrawn.
    fn read_bank(&self, node: usize, as_of: Option<u64>, message_final: bool) -> std::result::Result<(), String> {
        let begin = match as_of {
            Some(v) => format!("BEGIN READ ONLY AS OF SYSTEM TIME {v}"),
            None => "BEGIN READ ONLY".to_string(),
        };
        let result = self.cluster.with_client(node, |client| {
            client.execute(&begin)?;
            let agg = int_rows(
                client.execute("SELECT SUM(balance), MIN(balance), COUNT(*) FROM account")?,
            );
            let rows = int_rows(client.execute("SELECT id, balance FROM account")?);
            // Roll back read-only transactions whatever happened above.
            client.execute("COMMIT")?;
            Ok((agg?, rows?))
        });
        let (agg, rows) = match result {
            Ok(r) => r,
            Err(error) => {
                note_error(&error, "read_bank");
                return Err(error.to_string());
            }
        };
        let (sum, min, count) = match agg.as_slice() {
            [row] if row.len() == 3 => (row[0], row[1], row[2]),
            _ => (None, None, None),
        };
        let scan_sum: i64 = rows.iter().filter_map(|r| r.get(1).copied().flatten()).sum();
        let details = json!({
            "node": node, "as_of": as_of, "sum": sum, "min": min, "count": count,
            "scan_sum": scan_sum, "rows": format!("{rows:?}"),
        });
        let consistent = sum == Some(TOTAL_BALANCE)
            && count == Some(ACCOUNTS)
            && scan_sum == TOTAL_BALANCE
            && rows.len() as i64 == ACCOUNTS;
        let solvent = min.is_some_and(|m| m >= 0);
        match (message_final, as_of.is_some()) {
            (true, _) => {
                assert_always!(consistent && solvent, "the bank is consistent once faults stop", &details)
            }
            (false, false) => {
                assert_always!(consistent, "bank transfers never create or destroy money", &details);
                assert_always!(solvent, "no bank account is ever overdrawn", &details);
            }
            (false, true) => assert_always!(
                consistent && solvent,
                "a time-travel read sees a consistent bank",
                &details
            ),
        }
        if let Some(min) = min {
            assert_sometimes_less_than_or_equal_to!(
                min,
                0,
                "a bank account is drained to zero",
                &json!({"node": node})
            );
        }
        Ok(())
    }

    // ---- items ------------------------------------------------------------

    fn insert_batch(&self, node: usize, grp: i64, size: i64, abandon: bool) {
        let ids: Vec<i64> =
            (0..size).map(|_| self.next_item.fetch_add(1, Ordering::SeqCst)).collect();
        let batch = self.items.lock().unwrap_or_else(|e| e.into_inner()).open(grp, ids.clone());
        let set_state = |state| {
            self.items.lock().unwrap_or_else(|e| e.into_inner()).batches[batch].state = state;
        };
        let mut client = match self.cluster.client(node) {
            Ok(client) => client,
            Err(_) => return set_state(BatchState::Aborted),
        };
        let result: Result<()> = (|| {
            client.execute("BEGIN")?;
            for id in &ids {
                client.execute(&format!("INSERT INTO item VALUES ({id}, {grp})"))?;
            }
            Ok(())
        })();
        let details = json!({"node": node, "grp": grp, "ids": ids});
        if let Err(error) = result {
            note_error(&error, "insert");
            set_state(BatchState::Aborted);
            if !matches!(error, Error::IO(_) | Error::InvalidData(_)) {
                client.execute("ROLLBACK").ok();
                self.cluster.release(node, client);
            }
            return;
        }
        if abandon {
            // Close the connection mid-transaction: the server must roll it back.
            assert_reachable!("an insert transaction is abandoned without commit", &details);
            drop(client);
            return set_state(BatchState::Aborted);
        }
        let sent = Instant::now();
        set_state(BatchState::CommitSent(sent));
        match client.execute("COMMIT") {
            Ok(_) => {
                set_state(BatchState::Committed(sent, Instant::now()));
                self.cluster.release(node, client);
            }
            Err(error) => {
                note_error(&error, "insert_commit");
                if !is_ambiguous(&error) {
                    // A definite commit failure: the transaction didn't commit.
                    set_state(BatchState::Aborted);
                }
                assert_reachable!("an insert batch's commit fails or has an unknown outcome", &details);
            }
        }
    }

    /// Deletes the rows of one committed, not yet deleted batch in the group.
    fn delete_batch(&self, node: usize, grp: i64, pick: usize) {
        let (batch, ids) = {
            let items = self.items.lock().unwrap_or_else(|e| e.into_inner());
            let candidates: Vec<usize> = items
                .batches
                .iter()
                .enumerate()
                .filter(|(_, b)| {
                    b.grp == grp && b.delete.is_none() && matches!(b.state, BatchState::Committed(..))
                })
                .map(|(i, _)| i)
                .collect();
            if candidates.is_empty() {
                return;
            }
            let batch = candidates[pick % candidates.len()];
            (batch, items.batches[batch].ids.clone())
        };
        let set_delete = |state| {
            self.items.lock().unwrap_or_else(|e| e.into_inner()).batches[batch].delete = state;
        };
        let sent = Instant::now();
        set_delete(Some(DeleteState::Sent(sent)));
        let predicate: Vec<String> = ids.iter().map(|id| format!("id = {id}")).collect();
        let result =
            self.cluster.execute(node, &format!("DELETE FROM item WHERE {}", predicate.join(" OR ")));
        let details = json!({"node": node, "grp": grp, "ids": ids, "result": format!("{result:?}")});
        match result {
            Ok(StatementResult::Delete { count }) => {
                assert_always!(
                    count == ids.len() as u64,
                    "a delete of a committed batch removes exactly its rows",
                    &details
                );
                set_delete(Some(DeleteState::Acked(sent, Instant::now())));
            }
            Ok(_) => assert_unreachable!("a DELETE returns a delete result", &details),
            Err(error) if is_ambiguous(&error) => {
                note_error(&error, "delete");
                assert_reachable!("a delete has an unknown outcome", &details);
            }
            Err(error) => {
                note_error(&error, "delete");
                set_delete(None);
            }
        }
    }

    fn read_group(&self, node: usize, grp: i64, message_final: bool) -> std::result::Result<(), String> {
        let start = Instant::now();
        let result = self
            .cluster
            .execute(node, &format!("SELECT id FROM item WHERE grp = {grp}"))
            .and_then(int_rows);
        let end = Instant::now();
        let ids: Vec<i64> = match result {
            Ok(rows) => rows.iter().filter_map(|r| r.first().copied().flatten()).collect(),
            Err(error) => {
                note_error(&error, "read_group");
                return Err(error.to_string());
            }
        };
        let items = self.items.lock().unwrap_or_else(|e| e.into_inner());
        let check = items.check_group(grp, &ids, start, end);
        drop(items);
        let details = json!({"node": node, "grp": grp, "check": format!("{check:?}"), "rows": ids.len()});
        assert_always!(
            check.unknown.is_empty(),
            "every item row was written by the workload",
            &details
        );
        assert_always!(
            check.uncommitted.is_empty(),
            "rows of transactions that never committed are never visible",
            &details
        );
        assert_always!(
            check.partial.is_empty(),
            "a transaction's inserts are visible all together or not at all",
            &details
        );
        assert_always!(
            check.deleted.is_empty(),
            "rows never reappear after an acknowledged delete",
            &details
        );
        if message_final {
            assert_always!(
                check.missing.is_empty(),
                "acknowledged inserts survive once faults stop",
                &details
            );
        } else {
            assert_always!(
                check.missing.is_empty(),
                "acknowledged inserts are visible to every later read",
                &details
            );
        }
        assert_sometimes!(
            check.unknown_seen > 0,
            "a read finds an insert batch whose commit outcome was unknown",
            &json!({"node": node, "grp": grp})
        );
        assert_sometimes!(ids.len() > 50, "an item group grows past 50 rows", &json!({"grp": grp}));
        Ok(())
    }

    // ---- eventually -------------------------------------------------------

    /// Waits until the node answers reads, up to the deadline.
    fn wait_serving(&self, node: usize, deadline: Instant) -> bool {
        let mut last = String::new();
        loop {
            let result = self.cluster.execute(node, "SELECT COUNT(*) FROM kv");
            match result {
                Ok(_) => return true,
                Err(error) => {
                    let msg = format!("waiting for {} to serve reads: {error}", self.cluster.addrs[node]);
                    if msg != last {
                        log(&msg);
                        last = msg;
                    }
                }
            }
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    fn eventually(&self) {
        let Ok(_guard) = self.eventually_lock.try_lock() else { return };
        self.quiescing.store(true, Ordering::SeqCst);
        log("eventually: stopping faults");
        if let Ok(stop) = std::env::var("ANTITHESIS_STOP_FAULTS") {
            match std::process::Command::new(&stop).arg("3600").status() {
                Ok(status) => log(format!("eventually: {stop} exited with {status}")),
                Err(error) => log(format!("eventually: failed to run {stop}: {error}")),
            }
        }
        // Let in-flight rules finish, and forget connections from before.
        std::thread::sleep(Duration::from_secs(5));
        self.cluster.reset();

        let deadline = Instant::now() + RECOVERY_DEADLINE;
        let serving: Vec<bool> = (0..self.cluster.len()).map(|n| self.wait_serving(n, deadline)).collect();
        log("eventually: assertions start");
        assert_always!(
            serving.iter().all(|s| *s),
            "every toyDB node serves requests again once faults stop",
            &json!({"serving": serving})
        );
        if serving.iter().all(|s| *s) {
            let nodes = self.cluster.len();
            // A single request can time out right after recovery (toyDB doesn't
            // retry forwarded requests; clients are expected to), so each read
            // retries until a deadline before it counts as failed.
            for key in 0..KV_KEYS {
                let node = key as usize % nodes;
                if let Err(e) = self.retry_final(|| self.get(node, key, true)) {
                    assert_unreachable!("a kv read fails once faults stop", &json!({"node": node, "key": key, "error": e}));
                }
            }
            for node in 0..nodes {
                if let Err(e) = self.retry_final(|| self.read_bank(node, None, true)) {
                    assert_unreachable!("a bank read fails once faults stop", &json!({"node": node, "error": e}));
                }
            }
            for grp in 0..GROUPS {
                let node = grp as usize % nodes;
                if let Err(e) = self.retry_final(|| self.read_group(node, grp, true)) {
                    assert_unreachable!("an item read fails once faults stop", &json!({"node": node, "grp": grp, "error": e}));
                }
            }
            self.check_converged();
        }
        log("eventually: ending the timeline");
        std::process::exit(0);
    }

    /// Retries a final-phase read until it succeeds or FINAL_READ_DEADLINE
    /// passes, returning the errors seen.
    fn retry_final(
        &self,
        mut read: impl FnMut() -> std::result::Result<(), String>,
    ) -> std::result::Result<(), String> {
        let deadline = Instant::now() + FINAL_READ_DEADLINE;
        let mut errors: Vec<String> = Vec::new();
        loop {
            match read() {
                Ok(()) => {
                    if !errors.is_empty() {
                        assert_reachable!(
                            "a read succeeds on retry once faults stop",
                            &json!({"errors": errors})
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    if errors.last() != Some(&e) {
                        errors.push(e);
                    }
                }
            }
            if Instant::now() > deadline {
                return Err(errors.join("; "));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Waits for every node's log to match the leader's and every committed
    /// entry to be applied, then asserts that it happened.
    fn check_converged(&self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut last = String::new();
        loop {
            let status = self.cluster.with_client(0, |c| c.status());
            let converged = match &status {
                Ok(s) => {
                    s.raft.match_index.len() == self.cluster.len()
                        && s.raft.match_index.values().all(|&m| m == s.raft.commit_index)
                        && s.raft.applied_index == s.raft.commit_index
                }
                Err(_) => false,
            };
            let msg = match &status {
                Ok(s) => format!("raft status: leader={} term={} commit={} applied={} match={:?}", s.raft.leader, s.raft.term, s.raft.commit_index, s.raft.applied_index, s.raft.match_index),
                Err(e) => format!("raft status failed: {e}"),
            };
            if converged && let Ok(s) = &status {
                // Sessions on crashed nodes leave their transactions in the
                // MVCC active set forever (nothing rolls them back).
                assert_sometimes!(
                    s.mvcc.active_txns > 0,
                    "transactions from crashed sessions remain active once faults stop",
                    &json!({"active_txns": s.mvcc.active_txns})
                );
            }
            if converged || Instant::now() > deadline {
                assert_always!(
                    converged,
                    "every node's Raft log catches up with the leader once faults stop",
                    &json!({"status": msg})
                );
                return;
            }
            if msg != last {
                log(format!("eventually: waiting for convergence: {msg}"));
                last = msg;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

#[hegel::concurrent_state_machine]
impl Workload {
    #[rule(weight = 4)]
    fn put(&self, tc: TestCase) {
        let node = tc.draw(gs::integers::<usize>().max_value(self.s.cluster.len() - 1));
        let key = tc.draw(gs::integers::<i64>().min_value(0).max_value(KV_KEYS - 1));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        self.s.put(node, key);
    }

    #[rule(weight = 4)]
    fn get(&self, tc: TestCase) {
        let node = tc.draw(gs::integers::<usize>().max_value(self.s.cluster.len() - 1));
        let key = tc.draw(gs::integers::<i64>().min_value(0).max_value(KV_KEYS - 1));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        let _ = self.s.get(node, key, false);
    }

    #[rule(weight = 4)]
    fn transfer(&self, tc: TestCase) {
        let node = tc.draw(gs::integers::<usize>().max_value(self.s.cluster.len() - 1));
        let from = tc.draw(gs::integers::<i64>().min_value(0).max_value(ACCOUNTS - 1));
        let to = tc.draw(gs::integers::<i64>().min_value(0).max_value(ACCOUNTS - 1));
        let amount = tc.draw(gs::integers::<i64>().min_value(0).max_value(2 * INITIAL_BALANCE));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        self.s.transfer(node, from, to, amount);
    }

    #[rule(weight = 2)]
    fn read_bank(&self, tc: TestCase) {
        let node = tc.draw(gs::integers::<usize>().max_value(self.s.cluster.len() - 1));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        let _ = self.s.read_bank(node, None, false);
    }

    #[rule(weight = 1)]
    fn read_bank_as_of(&self, tc: TestCase) {
        let node = tc.draw(gs::integers::<usize>().max_value(self.s.cluster.len() - 1));
        let pick = tc.draw(gs::integers::<usize>().max_value(VERSIONS_KEPT - 1));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        let version = {
            let versions = self.s.versions.lock().unwrap_or_else(|e| e.into_inner());
            if versions.is_empty() {
                return;
            }
            versions[pick % versions.len()]
        };
        let _ = self.s.read_bank(node, Some(version), false);
    }

    #[rule(weight = 3)]
    fn insert_batch(&self, tc: TestCase) {
        let node = tc.draw(gs::integers::<usize>().max_value(self.s.cluster.len() - 1));
        let grp = tc.draw(gs::integers::<i64>().min_value(0).max_value(GROUPS - 1));
        let size = tc.draw(gs::integers::<i64>().min_value(1).max_value(3));
        let abandon = tc.draw(gs::weighted_booleans(0.1));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        self.s.insert_batch(node, grp, size, abandon);
    }

    #[rule(weight = 1)]
    fn delete_batch(&self, tc: TestCase) {
        let node = tc.draw(gs::integers::<usize>().max_value(self.s.cluster.len() - 1));
        let grp = tc.draw(gs::integers::<i64>().min_value(0).max_value(GROUPS - 1));
        let pick = tc.draw(gs::integers::<usize>().max_value(1 << 20));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        self.s.delete_batch(node, grp, pick);
    }

    #[rule(weight = 3)]
    fn read_group(&self, tc: TestCase) {
        let node = tc.draw(gs::integers::<usize>().max_value(self.s.cluster.len() - 1));
        let grp = tc.draw(gs::integers::<i64>().min_value(0).max_value(GROUPS - 1));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        let _ = self.s.read_group(node, grp, false);
    }

    #[rule]
    fn eventually(&self, tc: TestCase) {
        let fire = tc.draw(gs::weighted_booleans(EVENTUALLY_P));
        if fire && self.s.eventually_enabled {
            self.s.eventually();
        }
    }
}

// ---- startup ----------------------------------------------------------------

/// Retries `f` until it succeeds, logging each new failure.
fn retry<T>(what: &str, mut f: impl FnMut() -> Result<T>) -> T {
    let mut last = String::new();
    loop {
        match f() {
            Ok(v) => return v,
            Err(error) => {
                let msg = format!("{what}: {error}");
                if msg != last {
                    log(&msg);
                    last = msg;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Creates the schema and initial rows in one transaction, unless a previous
/// (possibly ambiguous) attempt already did.
fn setup_schema(cluster: &Cluster) {
    retry("creating the schema", || {
        cluster.with_client(0, |c| {
            let tables = c.list_tables()?;
            if ["account", "item", "kv"].iter().all(|t| tables.iter().any(|x| x == t)) {
                return Ok(());
            }
            if !tables.is_empty() {
                return Err(Error::InvalidData(format!("partial schema {tables:?}")));
            }
            c.execute("BEGIN")?;
            let result = (|| {
                c.execute("CREATE TABLE kv (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")?;
                c.execute("CREATE TABLE account (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)")?;
                c.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL INDEX)")?;
                let kv: Vec<String> = (0..KV_KEYS).map(|k| format!("({k}, 0)")).collect();
                c.execute(&format!("INSERT INTO kv VALUES {}", kv.join(", ")))?;
                let acc: Vec<String> =
                    (0..ACCOUNTS).map(|a| format!("({a}, {INITIAL_BALANCE})")).collect();
                c.execute(&format!("INSERT INTO account VALUES {}", acc.join(", ")))?;
                c.execute("COMMIT")?;
                Ok(())
            })();
            if result.is_err() && c.txn().is_some() {
                c.execute("ROLLBACK").ok();
            }
            result
        })
    });
}

fn main() {
    antithesis_sdk::antithesis_init();
    run();
}

#[hegel::main(phases = [hegel::Phase::Generate])]
fn run(tc: TestCase) {
    let addrs: Vec<String> = std::env::var("TOYDB_NODES")
        .unwrap_or_else(|_| "toydb1:9601,toydb2:9601,toydb3:9601,toydb4:9601,toydb5:9601".into())
        .split(',')
        .map(String::from)
        .collect();
    let cluster = Cluster::new(addrs);
    log(format!("nodes: {:?}", cluster.addrs));

    // Every node is up: it answers a status request, even with an error.
    for node in 0..cluster.len() {
        retry(&format!("waiting for {} to answer", cluster.addrs[node]), || {
            match cluster.with_client(node, |c| c.status()) {
                Ok(_) | Err(Error::Abort) => Ok(()),
                Err(e) => Err(e),
            }
        });
    }
    setup_schema(&cluster);
    // Every node serves the workload's reads.
    for node in 0..cluster.len() {
        retry(&format!("waiting for {} to serve reads", cluster.addrs[node]), || {
            cluster.execute(node, "SELECT COUNT(*) FROM kv").map(|_| ())
        });
    }
    log("setup complete");
    antithesis_sdk::lifecycle::setup_complete(&json!({"nodes": cluster.addrs}));

    while !std::path::Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("the workload starts", &json!({}));
    log("workload active");

    let origin = Instant::now();
    let eventually_enabled = std::env::var("TOYDB_EVENTUALLY").map(|v| v != "off").unwrap_or(true);
    let shared = Arc::new(Shared {
        cluster,
        origin,
        next_value: AtomicI64::new(1),
        next_item: AtomicI64::new(1),
        kv: (0..KV_KEYS).map(|_| Mutex::new(KeyHistory::new(0, origin))).collect(),
        versions: Mutex::new(VecDeque::new()),
        items: Mutex::new(Items::default()),
        quiescing: AtomicBool::new(false),
        eventually_lock: Mutex::new(()),
        eventually_enabled,
    });

    // Local loop: run the eventually after a fixed time.
    if let Some(secs) = std::env::var("TOYDB_EVENTUALLY_AFTER").ok().and_then(|v| v.parse().ok()) {
        let shared = shared.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(secs));
            shared.eventually();
        });
    }

    hegel::stateful::machine(Workload { s: shared })
        .steps(i64::MAX)
        .min_concurrency(1)
        .max_concurrency(6)
        .run_concurrent(tc);
}
