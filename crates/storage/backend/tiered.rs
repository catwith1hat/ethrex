//! Tiered storage backend: hot RocksDB + cold RocksDB.
//!
//! The hot instance holds all column families and recent block data.
//! The cold instance holds only the four warm CFs ([`WARM_TABLES`]) for
//! blocks older than the configured hot window.
//!
//! Writes always target the hot instance.  Reads for warm tables fall
//! through from hot to cold.  A background migration worker periodically
//! moves aged-out data from hot to cold.

use crate::api::tables::{
    BODIES, CANONICAL_BLOCK_HASHES, HEADERS, MISC_VALUES, RECEIPTS, WARM_TABLES, is_warm_table,
};
use crate::api::{
    PrefixResult, StorageBackend, StorageLockedView, StorageReadView, StorageWriteBatch,
};
use crate::error::StoreError;

use super::rocksdb::RocksDBBackend;

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use tracing::{debug, error, info};

// ── Configuration defaults ───────────────────────────────────────────

/// Default number of recent blocks to keep in the hot tier.
pub const DEFAULT_HOT_BLOCKS: u64 = 100_000;

/// Minimum allowed value for hot_blocks (must exceed the 128-block diff-layer window).
const MIN_HOT_BLOCKS: u64 = 1_024;

/// Number of blocks to migrate in a single batch before yielding.
const MIGRATION_BATCH_SIZE: u64 = 512;

/// Key in MISC_VALUES that tracks how far migration has progressed.
const COLD_MIGRATION_CURSOR_KEY: &[u8] = b"cold_migration_cursor";

// ── Cold-only RocksDB opener ─────────────────────────────────────────

/// Opens a RocksDB instance at `path` containing **only** the warm column
/// families.  Uses moderate resource settings since cold reads are rare.
fn open_cold_db(path: impl AsRef<Path>) -> Result<RocksDBBackend, StoreError> {
    RocksDBBackend::open_with_tables(path, &WARM_TABLES)
}

// ── TieredBackend ────────────────────────────────────────────────────

/// Two-tier storage backend that keeps recent data in a fast (hot) RocksDB
/// and moves historical block data into a separate (cold) RocksDB directory.
#[derive(Debug)]
pub struct TieredBackend {
    hot: Arc<RocksDBBackend>,
    cold: Arc<RocksDBBackend>,
    /// Number of recent blocks to keep hot.
    hot_blocks: u64,
    /// Signals the migration worker to stop.
    shutdown: Arc<AtomicBool>,
    /// The latest head block number known to the migration worker.
    head_block: Arc<AtomicU64>,
    /// Handle for the migration background thread.
    migration_thread: Option<JoinHandle<()>>,
}

impl TieredBackend {
    /// Open a tiered backend.
    ///
    /// - `hot_path`: path to the primary data directory (existing RocksDB).
    /// - `cold_path`: path to the cold data directory (created if absent).
    /// - `hot_blocks`: number of recent blocks to retain in the hot tier.
    pub fn open(
        hot_path: impl AsRef<Path>,
        cold_path: impl AsRef<Path>,
        hot_blocks: u64,
    ) -> Result<Self, StoreError> {
        let hot_blocks = hot_blocks.max(MIN_HOT_BLOCKS);

        let hot = Arc::new(RocksDBBackend::open(&hot_path)?);
        let cold = Arc::new(open_cold_db(&cold_path)?);

        let shutdown = Arc::new(AtomicBool::new(false));
        let head_block = Arc::new(AtomicU64::new(0));

        let worker = MigrationWorker {
            hot: hot.clone(),
            cold: cold.clone(),
            hot_blocks,
            shutdown: shutdown.clone(),
            head_block: head_block.clone(),
        };

        let migration_thread = std::thread::Builder::new()
            .name("cold-migration".into())
            .spawn(move || worker.run())
            .map_err(|e| StoreError::Custom(format!("Failed to spawn migration thread: {e}")))?;

        Ok(Self {
            hot,
            cold,
            hot_blocks,
            shutdown,
            head_block,
            migration_thread: Some(migration_thread),
        })
    }

    /// Inform the migration worker about the current chain head.
    /// This should be called after fork-choice updates.
    pub fn notify_head(&self, block_number: u64) {
        self.head_block.store(block_number, Ordering::Release);
    }

    /// Returns the configured hot-blocks window.
    pub fn hot_blocks(&self) -> u64 {
        self.hot_blocks
    }
}

impl Drop for TieredBackend {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.migration_thread.take() {
            let _ = handle.join();
        }
    }
}

impl StorageBackend for TieredBackend {
    fn clear_table(&self, table: &'static str) -> Result<(), StoreError> {
        self.hot.clear_table(table)?;
        if is_warm_table(table) {
            self.cold.clear_table(table)?;
        }
        Ok(())
    }

    fn begin_read(&self) -> Result<Box<dyn StorageReadView + '_>, StoreError> {
        Ok(Box::new(TieredReadView {
            hot: self.hot.begin_read()?,
            cold: self.cold.begin_read()?,
        }))
    }

    fn begin_write(&self) -> Result<Box<dyn StorageWriteBatch + 'static>, StoreError> {
        // Writes always go to the hot instance.
        self.hot.begin_write()
    }

    fn begin_locked(
        &self,
        table_name: &'static str,
    ) -> Result<Box<dyn StorageLockedView + 'static>, StoreError> {
        // Locked views are only used for snap-sync on trie tables, which are always hot.
        self.hot.begin_locked(table_name)
    }

    fn create_checkpoint(&self, path: &Path) -> Result<(), StoreError> {
        self.hot.create_checkpoint(path)
    }

    fn notify_head(&self, block_number: u64) {
        self.head_block.store(block_number, Ordering::Release);
    }
}

// ── TieredReadView ───────────────────────────────────────────────────

/// Read view that checks the hot tier first, falling through to cold for
/// warm tables.
struct TieredReadView<'a> {
    hot: Box<dyn StorageReadView + 'a>,
    cold: Box<dyn StorageReadView + 'a>,
}

impl StorageReadView for TieredReadView<'_> {
    fn get(&self, table: &'static str, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        if let Some(value) = self.hot.get(table, key)? {
            return Ok(Some(value));
        }
        if is_warm_table(table) {
            return self.cold.get(table, key);
        }
        Ok(None)
    }

    fn prefix_iterator(
        &self,
        table: &'static str,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = PrefixResult> + '_>, StoreError> {
        if is_warm_table(table) {
            let hot_iter = self.hot.prefix_iterator(table, prefix)?;
            let cold_iter = self.cold.prefix_iterator(table, prefix)?;
            return Ok(Box::new(ChainedIterator {
                hot: hot_iter,
                cold: Some(cold_iter),
                seen_keys: HashSet::new(),
            }));
        }
        self.hot.prefix_iterator(table, prefix)
    }
}

// ── ChainedIterator ──────────────────────────────────────────────────

/// Yields all entries from the hot iterator first, then entries from the
/// cold iterator whose keys were not already seen in the hot tier.
struct ChainedIterator<'a> {
    hot: Box<dyn Iterator<Item = PrefixResult> + 'a>,
    cold: Option<Box<dyn Iterator<Item = PrefixResult> + 'a>>,
    seen_keys: HashSet<Box<[u8]>>,
}

impl Iterator for ChainedIterator<'_> {
    type Item = PrefixResult;

    fn next(&mut self) -> Option<Self::Item> {
        // Drain hot first.
        if let Some(item) = self.hot.next() {
            match &item {
                Ok((key, _)) => {
                    self.seen_keys.insert(key.clone());
                }
                Err(_) => {}
            }
            return Some(item);
        }
        // Then cold, skipping duplicates.
        let cold = self.cold.as_mut()?;
        loop {
            match cold.next() {
                Some(Ok((key, value))) => {
                    if self.seen_keys.contains(&key) {
                        continue;
                    }
                    self.seen_keys.insert(key.clone());
                    return Some(Ok((key, value)));
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

// ── Migration Worker ─────────────────────────────────────────────────

struct MigrationWorker {
    hot: Arc<RocksDBBackend>,
    cold: Arc<RocksDBBackend>,
    hot_blocks: u64,
    shutdown: Arc<AtomicBool>,
    head_block: Arc<AtomicU64>,
}

impl MigrationWorker {
    fn run(&self) {
        info!(
            hot_blocks = self.hot_blocks,
            "Cold-migration worker started"
        );

        // Wait for the head to be set before starting.
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return;
            }
            if self.head_block.load(Ordering::Acquire) > 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(5));
        }

        loop {
            if self.shutdown.load(Ordering::Acquire) {
                debug!("Cold-migration worker shutting down");
                return;
            }

            match self.run_migration_pass() {
                Ok((migrated, cursor, cutoff)) => {
                    if migrated == 0 {
                        // Nothing to do — sleep longer.
                        std::thread::sleep(std::time::Duration::from_secs(30));
                    } else {
                        let remaining = cutoff.saturating_sub(cursor);
                        info!(
                            blocks = migrated,
                            cursor,
                            cutoff,
                            remaining,
                            "Migrated blocks to cold storage"
                        );
                        // Yield briefly then check for more.
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
                Err(e) => {
                    error!("Cold-migration error: {e}");
                    std::thread::sleep(std::time::Duration::from_secs(60));
                }
            }
        }
    }

    /// Runs one migration pass, returning (migrated_count, new_cursor, cutoff).
    fn run_migration_pass(&self) -> Result<(u64, u64, u64), StoreError> {
        let head = self.head_block.load(Ordering::Acquire);
        if head <= self.hot_blocks {
            return Ok((0, 0, 0));
        }
        let cutoff = head - self.hot_blocks;

        let cursor = self.read_cursor()?;
        if cursor >= cutoff {
            return Ok((0, cursor, cutoff));
        }

        let start = cursor;
        let end = (cursor + MIGRATION_BATCH_SIZE).min(cutoff);

        let mut migrated: u64 = 0;

        for block_number in start..end {
            if self.shutdown.load(Ordering::Acquire) {
                break;
            }
            self.migrate_block(block_number)?;
            migrated += 1;
        }

        let new_cursor = start + migrated;
        if migrated > 0 {
            self.write_cursor(new_cursor)?;
        }

        Ok((migrated, new_cursor, cutoff))
    }

    /// Migrate a single block's warm data from hot to cold.
    fn migrate_block(&self, block_number: u64) -> Result<(), StoreError> {
        let hot_read = self.hot.begin_read()?;

        // 1. Look up the canonical block hash for this number.
        let block_hash_bytes = match hot_read
            .get(CANONICAL_BLOCK_HASHES, &block_number.to_le_bytes())?
        {
            Some(b) => b,
            None => return Ok(()), // Block doesn't exist or was already removed.
        };

        // 2. Copy HEADERS entry (keyed by block_hash RLP).
        self.copy_entry(&hot_read, HEADERS, &block_hash_bytes)?;

        // 3. Read BODIES entry — we need it to find transaction hashes.
        let body_bytes = hot_read.get(BODIES, &block_hash_bytes)?;
        self.copy_entry_raw(BODIES, &block_hash_bytes, &body_bytes)?;

        // 4. Copy RECEIPTS entries (keyed by block_hash prefix).
        //    Receipt keys are (block_hash_rlp, index) — block_hash is the prefix.
        self.copy_prefixed_entries(&hot_read, RECEIPTS, &block_hash_bytes)?;

        // 5. Copy TRANSACTION_LOCATIONS.
        //    These are keyed by (tx_hash ++ block_hash).  We need to iterate the
        //    block body to extract tx hashes.  However the body is RLP-encoded and
        //    decoding it here would pull in heavy dependencies.  Instead we scan
        //    the TRANSACTION_LOCATIONS CF using a reverse approach: for each entry
        //    whose key ends with block_hash_bytes (32 bytes), move it.
        //    Since there is no efficient suffix index, we use the body's tx count
        //    heuristic — but for simplicity and correctness we skip tx_locations
        //    migration in this initial implementation.  The TieredReadView will
        //    still find them in hot.  A future optimization can migrate these.
        //
        //    NOTE: We intentionally leave TRANSACTION_LOCATIONS in hot for now.
        //    They are small (64-byte keys, ~40-byte values) and the prefix scan
        //    in get_transaction_location already handles them.

        // 6. Delete copied entries from hot.
        let mut hot_write = self.hot.begin_write()?;
        hot_write.delete(HEADERS, &block_hash_bytes)?;
        if body_bytes.is_some() {
            hot_write.delete(BODIES, &block_hash_bytes)?;
        }
        // Delete receipt entries.
        self.delete_prefixed_entries(&mut hot_write, &hot_read, RECEIPTS, &block_hash_bytes)?;
        hot_write.commit()?;

        Ok(())
    }

    /// Copy a single key-value entry from hot to cold.
    fn copy_entry(
        &self,
        hot_read: &Box<dyn StorageReadView + '_>,
        table: &'static str,
        key: &[u8],
    ) -> Result<(), StoreError> {
        if let Some(value) = hot_read.get(table, key)? {
            let mut cold_write = self.cold.begin_write()?;
            cold_write.put(table, key, &value)?;
            cold_write.commit()?;
        }
        Ok(())
    }

    /// Copy a raw value (already read) to cold.
    fn copy_entry_raw(
        &self,
        table: &'static str,
        key: &[u8],
        value: &Option<Vec<u8>>,
    ) -> Result<(), StoreError> {
        if let Some(v) = value {
            let mut cold_write = self.cold.begin_write()?;
            cold_write.put(table, key, v)?;
            cold_write.commit()?;
        }
        Ok(())
    }

    /// Copy all entries with a given prefix from hot to cold.
    fn copy_prefixed_entries(
        &self,
        hot_read: &Box<dyn StorageReadView + '_>,
        table: &'static str,
        prefix: &[u8],
    ) -> Result<(), StoreError> {
        let iter = hot_read.prefix_iterator(table, prefix)?;
        let mut batch = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            batch.push((key.to_vec(), value.to_vec()));
        }
        if !batch.is_empty() {
            let mut cold_write = self.cold.begin_write()?;
            cold_write.put_batch(table, batch)?;
            cold_write.commit()?;
        }
        Ok(())
    }

    /// Delete all entries with a given prefix from hot.
    fn delete_prefixed_entries(
        &self,
        hot_write: &mut Box<dyn StorageWriteBatch + 'static>,
        hot_read: &Box<dyn StorageReadView + '_>,
        table: &'static str,
        prefix: &[u8],
    ) -> Result<(), StoreError> {
        let iter = hot_read.prefix_iterator(table, prefix)?;
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            hot_write.delete(table, &key)?;
        }
        Ok(())
    }

    /// Read the migration cursor from hot MISC_VALUES.
    fn read_cursor(&self) -> Result<u64, StoreError> {
        let read = self.hot.begin_read()?;
        match read.get(MISC_VALUES, COLD_MIGRATION_CURSOR_KEY)? {
            Some(bytes) if bytes.len() == 8 => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes);
                Ok(u64::from_le_bytes(arr))
            }
            _ => Ok(0),
        }
    }

    /// Write the migration cursor to hot MISC_VALUES.
    fn write_cursor(&self, value: u64) -> Result<(), StoreError> {
        let mut write = self.hot.begin_write()?;
        write.put(
            MISC_VALUES,
            COLD_MIGRATION_CURSOR_KEY,
            &value.to_le_bytes(),
        )?;
        write.commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tables::{
        ACCOUNT_TRIE_NODES, BODIES, CANONICAL_BLOCK_HASHES, HEADERS, RECEIPTS,
    };

    fn make_tiered(hot_blocks: u64) -> (TieredBackend, tempfile::TempDir, tempfile::TempDir) {
        let hot_dir = tempfile::tempdir().expect("tempdir");
        let cold_dir = tempfile::tempdir().expect("tempdir");
        let backend = TieredBackend::open(hot_dir.path(), cold_dir.path(), hot_blocks).expect("open tiered");
        (backend, hot_dir, cold_dir)
    }

    #[test]
    fn read_falls_through_for_warm_tables() {
        let (backend, _hot, _cold) = make_tiered(100);

        // Write a value directly to the cold DB for a warm table.
        {
            let mut w = backend.cold.begin_write().expect("cold write");
            w.put(HEADERS, b"hash1", b"header_data").expect("put");
            w.commit().expect("commit");
        }

        // Read through the tiered view — should find it via cold fallback.
        let read = backend.begin_read().expect("read");
        let val = read.get(HEADERS, b"hash1").expect("get");
        assert_eq!(val, Some(b"header_data".to_vec()));
    }

    #[test]
    fn read_does_not_fall_through_for_hot_only_tables() {
        let (backend, _hot, _cold) = make_tiered(100);

        // Write a value to a warm table in cold (this is what the cold DB supports).
        {
            let mut w = backend.cold.begin_write().expect("cold write");
            w.put(HEADERS, b"hash_cold_only", b"header_data").expect("put");
            w.commit().expect("commit");
        }

        // A hot-only table key that only exists nowhere — read should return None
        // without attempting cold (ACCOUNT_TRIE_NODES doesn't exist in cold DB).
        let read = backend.begin_read().expect("read");
        let val = read.get(ACCOUNT_TRIE_NODES, b"nonexistent").expect("get");
        assert_eq!(val, None);

        // Verify the warm table DOES fall through (control).
        let val = read.get(HEADERS, b"hash_cold_only").expect("get");
        assert_eq!(val, Some(b"header_data".to_vec()));
    }

    #[test]
    fn hot_takes_precedence_over_cold() {
        let (backend, _hot, _cold) = make_tiered(100);

        // Write different values in hot and cold for the same key.
        {
            let mut w = backend.hot.begin_write().expect("hot write");
            w.put(BODIES, b"hash2", b"hot_body").expect("put");
            w.commit().expect("commit");
        }
        {
            let mut w = backend.cold.begin_write().expect("cold write");
            w.put(BODIES, b"hash2", b"cold_body").expect("put");
            w.commit().expect("commit");
        }

        let read = backend.begin_read().expect("read");
        let val = read.get(BODIES, b"hash2").expect("get");
        assert_eq!(val, Some(b"hot_body".to_vec()));
    }

    #[test]
    fn prefix_iterator_merges_hot_and_cold() {
        let (backend, _hot, _cold) = make_tiered(100);

        let prefix = b"block_";

        // Put entries in hot and cold with the same prefix.
        {
            let mut w = backend.hot.begin_write().expect("hot write");
            w.put(RECEIPTS, b"block_1_r0", b"receipt_hot").expect("put");
            w.commit().expect("commit");
        }
        {
            let mut w = backend.cold.begin_write().expect("cold write");
            w.put(RECEIPTS, b"block_1_r1", b"receipt_cold").expect("put");
            w.commit().expect("commit");
        }

        let read = backend.begin_read().expect("read");
        let iter = read.prefix_iterator(RECEIPTS, prefix).expect("iter");
        let results: Vec<_> = iter
            .map(|r| {
                let (k, v) = r.expect("item");
                (k.to_vec(), v.to_vec())
            })
            .collect();

        assert_eq!(results.len(), 2);
        // Both entries should be present.
        assert!(results.iter().any(|(_, v)| v == b"receipt_hot"));
        assert!(results.iter().any(|(_, v)| v == b"receipt_cold"));
    }

    #[test]
    fn prefix_iterator_deduplicates_keys() {
        let (backend, _hot, _cold) = make_tiered(100);

        let key = b"block_dup";

        // Same key in both hot and cold.
        {
            let mut w = backend.hot.begin_write().expect("hot write");
            w.put(HEADERS, key, b"hot_version").expect("put");
            w.commit().expect("commit");
        }
        {
            let mut w = backend.cold.begin_write().expect("cold write");
            w.put(HEADERS, key, b"cold_version").expect("put");
            w.commit().expect("commit");
        }

        let read = backend.begin_read().expect("read");
        let iter = read.prefix_iterator(HEADERS, b"block_").expect("iter");
        let results: Vec<_> = iter.map(|r| r.expect("item")).collect();

        // Should only have one entry (hot wins).
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.to_vec(), b"hot_version".to_vec());
    }

    #[test]
    fn migration_worker_moves_data_to_cold() {
        let (backend, _hot, _cold) = make_tiered(5);

        // Simulate 10 blocks: write canonical hashes + headers + bodies.
        for block_num in 0u64..10 {
            let hash_key = format!("hash_{block_num}");
            let hash_bytes = hash_key.as_bytes();
            let header_data = format!("header_{block_num}");
            let body_data = format!("body_{block_num}");

            let mut w = backend.hot.begin_write().expect("write");
            w.put(
                CANONICAL_BLOCK_HASHES,
                &block_num.to_le_bytes(),
                hash_bytes,
            )
            .expect("put canonical");
            w.put(HEADERS, hash_bytes, header_data.as_bytes())
                .expect("put header");
            w.put(BODIES, hash_bytes, body_data.as_bytes())
                .expect("put body");
            w.commit().expect("commit");
        }

        // Notify the worker of head = 10 with hot_blocks = 5, so cutoff = 5.
        // Blocks 0..5 should be migrated.
        backend.notify_head(10);

        // Run migration manually (don't rely on background thread timing).
        let worker = MigrationWorker {
            hot: backend.hot.clone(),
            cold: backend.cold.clone(),
            hot_blocks: 5,
            shutdown: Arc::new(AtomicBool::new(false)),
            head_block: Arc::new(AtomicU64::new(10)),
        };

        let (migrated, _cursor, _cutoff) = worker.run_migration_pass().expect("migration pass");
        assert!(migrated > 0, "should have migrated some blocks");

        // Block 0's header should now be in cold and removed from hot.
        let hash_0 = b"hash_0";
        let cold_read = backend.cold.begin_read().expect("cold read");
        assert!(
            cold_read.get(HEADERS, hash_0).expect("get").is_some(),
            "header should be in cold"
        );

        let hot_read = backend.hot.begin_read().expect("hot read");
        assert!(
            hot_read.get(HEADERS, hash_0).expect("get").is_none(),
            "header should be removed from hot"
        );

        // Block 9's header should still be in hot (within hot window).
        let hash_9 = b"hash_9";
        assert!(
            hot_read.get(HEADERS, hash_9).expect("get").is_some(),
            "recent header should remain in hot"
        );

        // Read through the tiered view should find both.
        let tiered_read = backend.begin_read().expect("tiered read");
        assert!(tiered_read.get(HEADERS, hash_0).expect("get").is_some());
        assert!(tiered_read.get(HEADERS, hash_9).expect("get").is_some());
    }
}
