//! What the driver expects the database to contain.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use crate::cluster::TIMEOUT;

/// How long a write with an unknown outcome stays possible. Raft can commit an
/// entry long after its client gave up, once a partition heals and a leader
/// that holds it is elected, so this is generous.
pub const AMBIGUITY_WINDOW: Duration = Duration::from_secs(300);

/// No read lasts longer than this (a connect plus one request, with margin).
const MAX_READ: Duration = Duration::from_secs(4 * TIMEOUT.as_secs());

// ---------------------------------------------------------------------------
// kv: one integer register per key.

#[derive(Debug)]
pub struct KvWrite {
    pub value: i64,
    pub sent: Instant,
    pub acked: Option<Instant>,
}

/// Every write still relevant to a key, with when it was sent and, if the
/// system acknowledged it, when.
#[derive(Debug)]
pub struct KeyHistory {
    pub writes: Vec<KvWrite>,
}

impl KeyHistory {
    /// A key whose row was inserted with `value` during setup.
    pub fn new(value: i64, at: Instant) -> Self {
        Self { writes: vec![KvWrite { value, sent: at, acked: Some(at) }] }
    }

    /// w was acknowledged, and some write sent after w's acknowledgement was
    /// itself acknowledged before t.
    fn overwritten_before(&self, w: &KvWrite, t: Instant) -> bool {
        let Some(acked) = w.acked else { return false };
        self.writes.iter().any(|o| o.sent > acked && o.acked.is_some_and(|a| a < t))
    }

    /// Whether a linearizable read that ran from `start` to `end` may return
    /// `got`: its write was sent before `end` and not overwritten before
    /// `start`.
    pub fn may_return(&self, got: i64, start: Instant, end: Instant) -> bool {
        self.writes
            .iter()
            .find(|w| w.value == got)
            .is_some_and(|w| w.sent < end && !self.overwritten_before(w, start))
    }

    pub fn is_unacked(&self, value: i64) -> bool {
        self.writes.iter().any(|w| w.value == value && w.acked.is_none())
    }

    pub fn record_sent(&mut self, value: i64, sent: Instant) {
        self.writes.push(KvWrite { value, sent, acked: None });
    }

    pub fn record_acked(&mut self, value: i64, at: Instant) {
        if let Some(w) = self.writes.iter_mut().find(|w| w.value == value) {
            w.acked = Some(at);
        }
    }

    /// Forgets a write that definitely didn't take effect.
    pub fn record_failed(&mut self, value: i64) {
        self.writes.retain(|w| w.value != value);
    }

    /// Keeps the history small: drops acknowledged writes overwritten before
    /// any read still in flight started, and unacknowledged writes older than
    /// the ambiguity window.
    pub fn prune(&mut self, now: Instant) {
        let (Some(read_cutoff), ambiguity_cutoff) =
            (now.checked_sub(MAX_READ), now.checked_sub(AMBIGUITY_WINDOW))
        else {
            return;
        };
        let drop: BTreeSet<i64> = self
            .writes
            .iter()
            .filter(|w| match w.acked {
                Some(_) => self.overwritten_before(w, read_cutoff),
                None => ambiguity_cutoff.is_some_and(|c| w.sent < c),
            })
            .map(|w| w.value)
            .collect();
        self.writes.retain(|w| !drop.contains(&w.value));
    }

    pub fn describe(&self, origin: Instant) -> serde_json::Value {
        let ms = |t: Instant| t.saturating_duration_since(origin).as_millis() as u64;
        serde_json::Value::Array(
            self.writes
                .iter()
                .map(|w| {
                    serde_json::json!({"value": w.value, "sent_ms": ms(w.sent), "acked_ms": w.acked.map(ms)})
                })
                .collect(),
        )
    }
}

// ---------------------------------------------------------------------------
// item: rows inserted in transactional batches, read back through an index.

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BatchState {
    /// Inserting; COMMIT not sent yet.
    Open,
    /// The transaction ended without a COMMIT being sent: rolled back, failed
    /// or abandoned. Its rows must never be visible.
    Aborted,
    /// COMMIT was sent at this time, and its outcome is not known (yet).
    CommitSent(Instant),
    /// COMMIT was sent, then acknowledged at the second time.
    Committed(Instant, Instant),
}

/// A DELETE of a committed batch's rows.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteState {
    /// Sent at this time; the outcome is not known (yet).
    Sent(Instant),
    /// Sent, then acknowledged at the second time.
    Acked(Instant, Instant),
}

#[derive(Debug)]
pub struct Batch {
    pub grp: i64,
    pub ids: Vec<i64>,
    pub state: BatchState,
    pub delete: Option<DeleteState>,
}

#[derive(Debug, Default)]
pub struct Items {
    pub batches: Vec<Batch>,
    pub batch_of: HashMap<i64, usize>,
}

/// What a read of one group found wrong, if anything.
#[derive(Debug, Default)]
pub struct GroupCheck {
    /// Rows that no batch the driver started could have written.
    pub unknown: Vec<i64>,
    /// Rows of batches whose COMMIT was never sent (before the read ended).
    pub uncommitted: Vec<i64>,
    /// Rows of batches acknowledged before the read started, but missing.
    pub missing: Vec<i64>,
    /// Batches only partly visible.
    pub partial: Vec<Vec<i64>>,
    /// Rows of batches whose DELETE was acknowledged before the read started.
    pub deleted: Vec<i64>,
    /// Batches with unknown outcomes that the read found (or didn't).
    pub unknown_seen: usize,
    pub unknown_unseen: usize,
}


impl Items {
    pub fn open(&mut self, grp: i64, ids: Vec<i64>) -> usize {
        let index = self.batches.len();
        for id in &ids {
            self.batch_of.insert(*id, index);
        }
        self.batches.push(Batch { grp, ids, state: BatchState::Open, delete: None });
        index
    }

    /// Judges the ids a read of group `grp`, from `start` to `end`, returned.
    pub fn check_group(&self, grp: i64, got: &[i64], start: Instant, end: Instant) -> GroupCheck {
        let mut check = GroupCheck::default();
        let got: BTreeSet<i64> = got.iter().copied().collect();
        for id in &got {
            match self.batch_of.get(id).map(|&b| &self.batches[b]) {
                None => check.unknown.push(*id),
                Some(batch) if batch.grp != grp => check.unknown.push(*id),
                Some(batch) => match batch.state {
                    BatchState::Open | BatchState::Aborted => check.uncommitted.push(*id),
                    BatchState::CommitSent(sent) | BatchState::Committed(sent, _) if sent >= end => {
                        check.uncommitted.push(*id)
                    }
                    _ => {}
                },
            }
        }
        for batch in self.batches.iter().filter(|b| b.grp == grp) {
            let seen = batch.ids.iter().filter(|id| got.contains(id)).count();
            if seen != 0 && seen != batch.ids.len() {
                check.partial.push(batch.ids.clone());
            }
            let delete_sent_before_end = match batch.delete {
                Some(DeleteState::Sent(sent) | DeleteState::Acked(sent, _)) => sent < end,
                None => false,
            };
            if let Some(DeleteState::Acked(_, acked)) = batch.delete
                && acked < start
                && seen > 0
            {
                check.deleted.extend(batch.ids.iter().filter(|id| got.contains(id)));
            }
            match batch.state {
                BatchState::Committed(_, acked)
                    if acked < start && seen < batch.ids.len() && !delete_sent_before_end =>
                {
                    check.missing.extend(batch.ids.iter().filter(|id| !got.contains(id)));
                }
                BatchState::CommitSent(_) if seen > 0 => check.unknown_seen += 1,
                BatchState::CommitSent(_) => check.unknown_unseen += 1,
                _ => {}
            }
        }
        check
    }
}
