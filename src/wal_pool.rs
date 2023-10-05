/// A handle to a pool of connections, designed for a database in WAL mode.
#[derive(Debug, Clone)]
pub struct WalPool {}

impl WalPool {
    /// Get a builder for a [`WalPool`].
    pub fn builder() -> WalPoolBuilder {
        WalPoolBuilder::new()
    }
}

/// A builder for a [`WalPool`].
pub struct WalPoolBuilder {
    /// The maximum number of reads that may be queued.
    pub max_queued_reads: Option<usize>,

    /// The maximum number of writes that may be queued.
    pub max_queued_writes: Option<usize>,

    /// The number of read connections
    pub num_read_connections: usize,
}

impl WalPoolBuilder {
    /// Make a new [`WalPoolBuilder`].
    pub fn new() -> Self {
        // TODO: Try to find some sane defaults experimentally.
        Self {
            max_queued_reads: Some(128),
            max_queued_writes: Some(32),

            num_read_connections: 4,
        }
    }

    /// Set the maximum number of queued reads.
    pub fn max_queued_reads(&mut self, max_queued_reads: Option<usize>) -> &mut Self {
        self.max_queued_reads = max_queued_reads;
        self
    }

    /// Set the maximum number of queued writes.
    pub fn max_queued_writes(&mut self, max_queued_writes: Option<usize>) -> &mut Self {
        self.max_queued_writes = max_queued_writes;
        self
    }

    /// Set the number of read connections.
    pub fn num_read_connections(&mut self, num_read_connections: usize) -> &mut Self {
        self.num_read_connections = num_read_connections;
        self
    }
}

impl Default for WalPoolBuilder {
    fn default() -> Self {
        Self::new()
    }
}
