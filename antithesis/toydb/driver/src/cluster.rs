//! Connections to the toyDB nodes, and how the driver reads request outcomes.

use std::sync::Mutex;
use std::time::Duration;

use toydb::error::{Error, Result};
use toydb::{Client, StatementResult};

/// Timeout for connecting and for each request. Short, so a request to an
/// unreachable node costs a moment rather than a stalled round.
pub const TIMEOUT: Duration = Duration::from_secs(2);

/// Idle connections kept per node.
const POOL_SIZE: usize = 8;

/// Whether an error means the request may or may not have taken effect.
///
/// * `Abort`: Raft aborted the request on a leader change, but a write may
///   already be in the log and commit later.
/// * `IO` / `InvalidData`: the connection timed out or broke, possibly after
///   the server executed the request.
pub fn is_ambiguous(error: &Error) -> bool {
    matches!(error, Error::Abort | Error::IO(_) | Error::InvalidData(_))
}

/// Whether the connection is unusable after this error.
fn breaks_connection(error: &Error) -> bool {
    matches!(error, Error::IO(_) | Error::InvalidData(_))
}

pub struct Cluster {
    pub addrs: Vec<String>,
    pools: Vec<Mutex<Vec<Client>>>,
}

impl Cluster {
    pub fn new(addrs: Vec<String>) -> Self {
        let pools = addrs.iter().map(|_| Mutex::new(Vec::new())).collect();
        Self { addrs, pools }
    }

    pub fn len(&self) -> usize {
        self.addrs.len()
    }

    /// Takes an idle connection to the node, or opens a new one.
    pub fn client(&self, node: usize) -> Result<Client> {
        let pooled = self.pools[node].lock().unwrap_or_else(|e| e.into_inner()).pop();
        match pooled {
            Some(client) => Ok(client),
            None => Client::connect_timeout(self.addrs[node].as_str(), TIMEOUT),
        }
    }

    /// Returns a connection after use. Connections in a transaction are
    /// dropped (closing them rolls back the transaction server-side).
    pub fn release(&self, node: usize, client: Client) {
        if client.txn().is_some() {
            return;
        }
        let mut pool = self.pools[node].lock().unwrap_or_else(|e| e.into_inner());
        if pool.len() < POOL_SIZE {
            pool.push(client);
        }
    }

    /// Runs `f` on a connection to the node, keeping the connection unless the
    /// result shows it broke.
    pub fn with_client<T>(
        &self,
        node: usize,
        f: impl FnOnce(&mut Client) -> Result<T>,
    ) -> Result<T> {
        let mut client = self.client(node)?;
        let result = f(&mut client);
        match &result {
            Err(error) if breaks_connection(error) => {}
            _ => self.release(node, client),
        }
        result
    }

    /// Executes one statement on the node.
    pub fn execute(&self, node: usize, statement: &str) -> Result<StatementResult> {
        self.with_client(node, |client| client.execute(statement))
    }

    /// Drops every idle connection, e.g. once faults stop, so later requests
    /// don't reuse a connection to a node that restarted.
    pub fn reset(&self) {
        for pool in &self.pools {
            pool.lock().unwrap_or_else(|e| e.into_inner()).clear();
        }
    }
}

/// Reads the integer columns of every row of a SELECT result.
pub fn int_rows(result: StatementResult) -> Result<Vec<Vec<Option<i64>>>> {
    use toydb::sql::types::Value;
    let StatementResult::Select { rows, .. } = result else {
        return Err(Error::InvalidData(format!("expected a SELECT result, got {result:?}")));
    };
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    Value::Integer(i) => Ok(Some(i)),
                    Value::Null => Ok(None),
                    value => Err(Error::InvalidData(format!("expected an integer, got {value}"))),
                })
                .collect()
        })
        .collect()
}
