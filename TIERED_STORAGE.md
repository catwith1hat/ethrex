# Tiered Storage Design

A design for splitting ethrex's storage into a **hot tier** (fast SSD, small) and
a **cold tier** (separate directory, cheap/large disk) so that the working set stays
small while full history remains available on demand.

---

## 1. Goals

1. Keep the hot RocksDB instance small enough to fit on a fast NVMe volume
   (target: latest ~90 days of data, roughly state + recent blocks).
2. Move historical data that is never touched during block execution or Engine API
   processing into a second, independent data directory.
3. Serve cold reads transparently — RPC and P2P callers should not need to know
   which tier holds a piece of data.
4. Allow the cold directory to be a slower medium (HDD, NFS mount, object-store
   adapter) without affecting block-import throughput.
5. Align with the Ethereum roadmap: EIP-4444 (history expiry from P2P) means
   nodes will increasingly stop serving ancient blocks over devp2p and instead
   retrieve them from the Portal Network or external archive services.

---

## 2. Data Classification

Every column family in the current storage layer is classified below.
"Head distance" means the number of blocks between the data's block and the
chain head.

### 2.1 Always Hot (must stay in the primary RocksDB)

| Column Family | Why |
|---|---|
| `ACCOUNT_TRIE_NODES` | Block execution opens the trie at `parent.state_root`. The diff-layer cache covers the last 128 roots, but the on-disk trie is still needed as the base. The entire current-state trie must remain hot. |
| `STORAGE_TRIE_NODES` | Same — per-account storage tries are walked during every `SLOAD`/`SSTORE`. |
| `ACCOUNT_FLATKEYVALUE` | Flat snapshot of account leaves; used by `get_account_info` / `get_storage_at` as a fast path that avoids full trie traversal. |
| `STORAGE_FLATKEYVALUE` | Same, for storage slots. |
| `ACCOUNT_CODES` | Contract bytecode is keyed by code hash, not block number. Any code deployed at any point can be called at head. The 64 MB LRU cache handles most reads, but the underlying CF must remain hot. |
| `ACCOUNT_CODE_METADATA` | Tiny (8 bytes per code hash). Used for `EXTCODESIZE` gas calculation. |
| `CHAIN_DATA` | Chain config, head/safe/finalized pointers. Tiny, always needed. |
| `PENDING_BLOCKS` | Transient (cleared after fork-choice resolution). |
| `INVALID_CHAINS` | Tiny; used by Engine API. |
| `SNAP_STATE` | Sync checkpoints. Tiny and transient. |
| `MISC_VALUES` | FKV cursor, oldest-witness pointer. Tiny. |
| `CANONICAL_BLOCK_HASHES` | Maps `block_number → block_hash`. Small (~40 bytes per block). Required by almost every read path (`get_block_header(number)` calls this first). Keep all of them hot for now; they total < 1 GB at 20 M blocks. |
| `BLOCK_NUMBERS` | Reverse map `block_hash → block_number`. Same size profile. Keep hot. |

### 2.2 Warm (recent blocks hot, older blocks cold)

| Column Family | Hot window | Cold data | Notes |
|---|---|---|---|
| `HEADERS` | Last N blocks (configurable, default 100 000) | Blocks older than N | Engine API only needs parent header. P2P serves headers in response to `GetBlockHeaders` but only needs the hot window when EIP-4444 is active. RPC `eth_getBlockByNumber` can tolerate a cold-tier read for ancient headers. |
| `BODIES` | Last N blocks | Blocks older than N | Same reasoning. Bodies are large (~1-5 KB each). They dominate on-disk size after state. Block execution only needs the *current* block's body (already in memory from P2P/Engine). |
| `RECEIPTS` | Last N blocks | Blocks older than N | `eth_getTransactionReceipt`, `eth_getLogs`, and P2P `GetReceipts` access these. Receipts are comparable in size to bodies. Log filtering (`eth_getLogs`) over ancient ranges will read from cold tier — this is acceptable since such queries are inherently slow. |
| `TRANSACTION_LOCATIONS` | Last N blocks | Blocks older than N | `eth_getTransactionByHash` does a prefix scan. Recent transactions are looked up far more frequently. Old entries are only needed for archive-style queries. |
| `EXECUTION_WITNESSES` | Last 128 blocks (already pruned) | N/A — already auto-pruned. No change needed. |

### 2.3 Exclusively Cold (or deletable)

| Column Family | Disposition |
|---|---|
| `FULLSYNC_HEADERS` | Only used during initial full-sync. Can be cleared after sync completes. Already handled by `clear_fullsync_headers()`. No tiering needed. |

### 2.4 Not Stored (no action)

| Data | Status |
|---|---|
| **Blobs (EIP-4844)** | Blob sidecars are never persisted to RocksDB. They live only transiently in the mempool/payload and are dropped after inclusion. The `BlobsBundle` type exists in memory during payload building. The consensus layer is responsible for blob availability during the ~18-day retention window. **No tiering needed.** |
| **Historical state (old trie nodes)** | ethrex currently only serves state at blocks within the 128-block diff-layer cache. State trie nodes from older blocks are *overwritten in place* (same key = same trie path) — they are not versioned by block number. Old intermediate trie nodes that are no longer reachable from the current state root are dead data inside RocksDB and will be reclaimed by compaction. There is nothing to tier here; the trie CFs already represent current-state only. |

---

## 3. Architecture

```
                        ┌──────────────────────────┐
                        │        Store API          │
                        │  (unchanged public API)   │
                        └────────────┬─────────────┘
                                     │
                        ┌────────────▼─────────────┐
                        │     TieredBackend         │
                        │  impl StorageBackend      │
                        └──┬──────────────────┬────┘
                           │                  │
               ┌───────────▼──────┐  ┌────────▼────────┐
               │   Hot RocksDB    │  │  Cold RocksDB    │
               │  (fast SSD)      │  │  (separate dir)  │
               │                  │  │                  │
               │ state tries      │  │ old HEADERS      │
               │ flat KV          │  │ old BODIES       │
               │ codes            │  │ old RECEIPTS     │
               │ chain metadata   │  │ old TX_LOCATIONS │
               │ recent blocks    │  │                  │
               └──────────────────┘  └─────────────────┘
```

### 3.1 `TieredBackend`

A new `StorageBackend` implementation that wraps two `RocksDBBackend` instances:

- **`hot`**: opened at the existing `--datadir` path. Contains all always-hot CFs
  plus the recent window of warm CFs.
- **`cold`**: opened at a new `--datadir-cold` path. Contains only the four warm
  CFs (`HEADERS`, `BODIES`, `RECEIPTS`, `TRANSACTION_LOCATIONS`), holding data
  older than the hot window.

The `TieredBackend` implements the existing `StorageBackend` trait:

```rust
impl StorageBackend for TieredBackend {
    fn begin_read(&self) -> Result<Box<dyn StorageReadView>> {
        Ok(Box::new(TieredReadView {
            hot: self.hot.begin_read()?,
            cold: self.cold.begin_read()?,
        }))
    }

    fn begin_write(&self) -> Result<Box<dyn StorageWriteBatch>> {
        // Writes always target the hot instance.
        // The migration worker moves data to cold asynchronously.
        self.hot.begin_write()
    }
    // ...
}
```

### 3.2 `TieredReadView`

For the four warm CFs, reads fall through:

```rust
impl StorageReadView for TieredReadView {
    fn get(&self, table: &'static str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Try hot first
        if let Some(value) = self.hot.get(table, key)? {
            return Ok(Some(value));
        }
        // For warm tables, fall through to cold
        if is_warm_table(table) {
            return self.cold.get(table, key);
        }
        Ok(None)
    }

    fn prefix_iterator(&self, table: &'static str, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>>
    {
        if is_warm_table(table) {
            // Merge iterators: hot first, then cold, dedup by key
            return Ok(Box::new(MergedIterator::new(
                self.hot.prefix_iterator(table, prefix)?,
                self.cold.prefix_iterator(table, prefix)?,
            )));
        }
        self.hot.prefix_iterator(table, prefix)
    }
}
```

The `is_warm_table()` check is a simple match on the four warm CF names —
no runtime overhead for hot-only CFs.

### 3.3 Migration Worker

A background thread (similar to the existing FKV generator and trie-update
worker) that periodically moves old data from hot to cold:

```
loop:
    sleep(migration_interval)  // e.g. every 1000 blocks or 30 minutes
    let cutoff = head_number - hot_window
    for each warm CF:
        scan hot CF for keys with block_number < cutoff
        batch-write them to cold CF
        batch-delete them from hot CF
```

Key design decisions:

- **Key encoding**: `HEADERS` and `BODIES` are keyed by `block_hash`, not
  `block_number`. The migration worker must scan `CANONICAL_BLOCK_HASHES` to find
  hashes for blocks below the cutoff, then move those hash-keyed entries.
  `RECEIPTS` use a `(block_hash, index)` composite key — same approach.
  `TRANSACTION_LOCATIONS` use `(tx_hash, block_hash)` — these need a reverse
  lookup from block hashes to find all tx entries belonging to a block.

- **Atomicity**: The migration is not atomic across the two DBs. This is
  acceptable because:
  - Data is *copied* to cold before being deleted from hot.
  - If the process crashes mid-migration, data exists in both tiers
    (duplicated, not lost). The next migration run will reconcile.
  - The `TieredReadView` handles duplicates correctly (hot takes precedence).

- **Tracking progress**: A new key in `MISC_VALUES` tracks the last block number
  that was migrated to cold (`cold_migration_cursor`). On restart, migration
  resumes from this cursor.

### 3.4 Integration with `remove_block`

The existing `remove_block()` only deletes from hot-tier CFs. For tiered storage,
it must also attempt deletion from the cold tier. This matters for reorgs of
very deep blocks (unusual but possible during long-range reorganizations).

---

## 4. Configuration

New CLI flags on the `ethrex` binary:

```
--datadir-cold <PATH>     Path to cold storage directory. If not set, tiered
                          storage is disabled and all data stays in --datadir.

--storage.hot-blocks <N>  Number of recent blocks to keep in hot storage.
                          Default: 100000 (~14 days at 12s blocks).
                          Minimum: 1024 (to stay well above the 128-block
                          diff-layer window).
```

When `--datadir-cold` is not provided, the `Store` uses a plain `RocksDBBackend`
exactly as today — zero behavioral change for existing users.

---

## 5. Migration Path for Existing Nodes

When a node that already has a populated `--datadir` enables `--datadir-cold`
for the first time:

1. On startup, detect that the cold DB is empty while the hot DB has blocks
   below the hot window.
2. Run an initial migration pass (blocking startup or as a background task with
   degraded performance — configurable).
3. After initial migration, switch to the normal periodic migration loop.

This avoids requiring users to re-sync.

---

## 6. Impact on Existing Code

### 6.1 Changes Required

| File | Change |
|---|---|
| `crates/storage/backend/mod.rs` | Add `TieredBackend` module. |
| `crates/storage/backend/tiered.rs` | New file: `TieredBackend`, `TieredReadView`, `TieredWriteBatch`, `MergedIterator`, migration worker. |
| `crates/storage/store.rs` | `Store::new()` accepts optional cold path; constructs `TieredBackend` when provided. Add `cold_migration_cursor` read/write helpers to `MISC_VALUES`. |
| `crates/storage/api/tables.rs` | Add `WARM_TABLES: [&str; 4]` constant. |
| `cmd/ethrex/` | Add `--datadir-cold` and `--storage.hot-blocks` CLI args. Thread them into `Store::new()`. |

### 6.2 No Changes Required

| Component | Why |
|---|---|
| `Store` public API | All existing methods (`get_block_header`, `get_receipt`, etc.) remain unchanged. The `StorageBackend` abstraction hides the tiering. |
| Blockchain / VM / P2P / RPC | These consume the `Store` API, not the backend directly. Transparent. |
| Trie layer cache | Only deals with in-memory diff layers and `ACCOUNT_TRIE_NODES` / `STORAGE_TRIE_NODES`, which are always-hot. |
| FlatKeyValue generator | Only touches `ACCOUNT_FLATKEYVALUE` / `STORAGE_FLATKEYVALUE`, which are always-hot. |
| Snap sync | Only touches `SNAP_STATE`, trie nodes, and `FULLSYNC_HEADERS` — all always-hot. |

---

## 7. Data Size Estimates

Rough per-block sizes on Ethereum mainnet (post-Merge, avg):

| Data | Per-block size | 20M blocks | 100K blocks (hot) |
|---|---|---|---|
| Header | ~550 B | ~11 GB | ~55 MB |
| Body | ~3 KB (varies wildly) | ~60 GB | ~300 MB |
| Receipts | ~2 KB avg | ~40 GB | ~200 MB |
| Tx locations | ~0.5 KB avg | ~10 GB | ~50 MB |
| **Subtotal (warm)** | | **~120 GB** | **~600 MB** |
| State trie nodes | — | ~80-100 GB | (current state, not per-block) |
| Flat KV | — | ~30-40 GB | (current state) |
| Codes | — | ~5 GB | (all time, not per-block) |

With tiered storage at a 100K-block hot window, the hot RocksDB drops from
~350+ GB to ~230 GB (state dominates), while the cold tier absorbs the ~120 GB
of historical block data. As the chain grows, only the cold tier grows
significantly.

---

## 8. Future Extensions

- **EIP-4444 integration**: When EIP-4444 is implemented, the P2P layer stops
  serving blocks older than ~1 year. The cold tier becomes the only local source
  for ancient data. Nodes could choose to not run a cold tier at all and rely on
  Portal Network for archive queries.

- **Cold tier as object storage**: The `StorageBackend` trait could gain a
  third implementation backed by S3/GCS for the cold tier, enabling cloud-native
  deployments where archive data is stored cheaply in object storage.

- **State expiry**: If Ethereum implements state expiry (Verkle + state expiry
  EIPs), the always-hot trie CFs could themselves be tiered. This design
  does not preclude that — the `TieredBackend` pattern generalizes.

- **Compaction of cold data**: The cold RocksDB could use more aggressive
  compression (zstd instead of LZ4) since it is read-rarely. This is a simple
  `Options` change on the cold instance.

---

## 9. Implementation Order

1. **`TieredBackend` skeleton**: Implement `StorageBackend` with two RocksDB
   instances, hot-only writes, fall-through reads. No migration yet — just the
   plumbing.
2. **`TieredReadView` with `MergedIterator`**: Handle reads that may span both
   tiers. Add `is_warm_table()`.
3. **Migration worker**: Background thread that moves data from hot to cold.
   Track cursor in `MISC_VALUES`.
4. **CLI integration**: Wire `--datadir-cold` and `--storage.hot-blocks` through
   the argument parser to `Store::new()`.
5. **Initial migration on first enable**: Detect empty cold DB and run catch-up
   migration.
6. **Tests**: Unit tests for `TieredReadView` fallthrough, `MergedIterator`
   dedup, migration correctness (data appears in cold, disappears from hot).
   Integration test: run a short chain with tiered storage, verify all RPC
   endpoints still work.
