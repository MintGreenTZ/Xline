# Xline TODO

## Urgent

- [ ] **Pass CI in `fix/consistency-problem-when-start-fast-path-too-early` branch**
  - [x] Fixed: cargo audit failures — updated deps (bytes, crossbeam-channel, hashbrown, ring,
    tracing-subscriber) and added `--ignore` for 3 MSRV-blocked advisories (protobuf, time, idna)
  - [x] Fixed: snapshot validation logic (FIXME in raw_curp/mod.rs:1128) with 7 unit tests
  - [x] Fixed: commit messages — squashed 8 commits into 4 well-formed conventional commits
  - [x] Fixed: `cargo sort` — reordered sections in `curp/Cargo.toml` and `xline-client/Cargo.toml`
  - [ ] Remaining: sccache build failure — transient GitHub Actions cache outage (HTTP 400), needs CI re-run

## High Priority

### Correctness & Consistency Fixes

- [ ] **FIXME: Cluster info state inconsistency with log**
  - File: `crates/curp/src/server/raw_curp/mod.rs:1929`
  - The state of `ctx.cluster_info` might be inconsistent with the log. Needs review
    of all usages of `ctx.cluster_info` to ensure correctness.
  - See also: `mod.rs:1956` — review all usages of `ctx.cluster_info`
  - **Analysis:** `ctx.cluster_info` is mutated in `switch_config()` (add/remove/update/promote)
    and `fallback_conf_change()` (undo). If a node crashes mid-recovery or receives a snapshot,
    the reconstructed cluster state may differ from what other nodes see.
  - **Approach:** Enrich each ConfChange log entry with member state fields (`name`,
    `client_urls`, `is_learner`) so `switch_config()` can reconstruct Members deterministically
    from the log alone, without relying on transient in-memory state.
  - **Breakdown:**
    - [x] Phase 1: Enrich ConfChange with member state fields (~200 LOC)
      - Added `name`, `client_urls`, `is_learner` fields to ConfChange proto message
      - Updated all ConfChange constructors in `rpc/mod.rs` with `with_member_state()` method
      - Updated `switch_config()` to use enriched fields when creating Members (instead of
        empty defaults), and returns new `FallbackInfo` struct with `client_urls`
      - Updated `handle_propose_conf_change()` to enrich ConfChange entries with current
        member state from `cluster_info` before logging
      - Updated `fallback_conf_change()` to restore `client_urls` when undoing Remove ops
      - Added `client_urls` field to `FallbackContext` in `log.rs`
      - Added 3 new tests + updated 4 existing tests
    - [x] Phase 2: Audit all `ctx.cluster_info` usages for defensive handling (~80 LOC)
      - Audited every `ctx.cluster_info` read/write across the codebase (25+ call sites)
      - Changed `ClusterInfo::update()` from panicking to returning `Option<Vec<String>>`
      - Made `switch_config()` Update/Promote branches return `None` gracefully when
        the target member is missing (crash recovery can leave cluster_info inconsistent)
      - Made `fallback_conf_change()` Update/Promote branches skip safely when member
        is missing instead of panicking
      - Fixed `lease_server.rs` leader address lookups to return `tonic::Status::unavailable`
        instead of panicking with `unreachable!()`
      - Added 6 new tests: 2 for ClusterInfo::update(), 4 for defensive switch_config
        and fallback behavior with missing members
    - [ ] Phase 3: Update recovery and snapshot installation paths (~40-80 LOC)
    - [ ] Phase 4: Additional integration tests for conf change consistency (~60-100 LOC)

- [x] **FIXME: Snapshot validation logic correctness**
  - File: `crates/curp/src/server/raw_curp/mod.rs:1128`
  - **Fixed:** The old check `last_log_index < last_included_index && last_log_term <= last_included_term`
    was too restrictive — it rejected snapshots at the same (term, index) as the follower's log, and
    used an asymmetric comparison that could reject valid snapshots from higher terms. Replaced with
    standard Raft log ordering: accept if snapshot term > follower term, or same term with index >=.

- [x] **FIXME: Persist other log entries**
  - File: `crates/curp/src/server/raw_curp/log.rs:446`
  - **Resolved (stale FIXME):** All 6 callers of `log.push()` already call `persistent_log_entries()`
    after pushing: `push_logs()` (line 569), `handle_propose_shutdown()` (line 653),
    `handle_propose_conf_change()` (line 697), `handle_publish()` (line 725),
    `become_candidate()` (line 1057), and `recover_from_spec_pools()` (line 1872).
    The FIXME was added during initial persistence refactoring but all entry types have since
    been covered. Removed the stale comment from `log.rs`.

### Engine / Storage Fixes

- [ ] **FIXME: Remove retry logic after curp command execution reimplemented**
  - File: `crates/engine/src/rocksdb_engine/mod.rs:154-155`
  - Also remove the `Clone` impl of `WriteOperation` once retry is gone.

- [ ] **TODO: Refactor engine operation trait to require `&mut self` for writes**
  - File: `crates/engine/src/api/operation.rs:4`
  - Write operations should take `&mut self` for safety.

### CURP Server Improvements

- [x] **TODO: Better dedup mechanism in log entries**
  - File: `crates/curp/src/server/raw_curp/mod.rs:1843`
  - **Fixed:** Replaced O(n) `get_cmd_ids()` (which rebuilt a `HashSet<ProposeId>` from the entire
    log on every call) with an incremental `cmd_ids` cache maintained inside the `Log<C>` struct.
    The cache is updated on `push_back()`, `pop_front()`, `truncate()`, `clear()`, and `restore()`.
    Dedup lookups in `recover_from_spec_pools()` are now O(1) via `contains_cmd_id()`. Added 3 unit
    tests covering lifecycle, truncation, and restore scenarios.

- [x] **TODO: Disable dedup for read-only or commutative commands**
  - File: `crates/curp/src/server/curp_node.rs:267`
  - **Fixed (read-only part):** Read-only commands now skip the duplicate cached-result path in
    `propose_stream()`. Dedup tracking still runs (for GC), but duplicate read-only commands
    proceed to fresh re-execution instead of returning stale cached results. Commutative
    command support is left for a future task (requires adding `is_commutative()` to the
    `Command` trait).

- [ ] **TODO: Buffer local snapshots for long-down followers**
  - File: `crates/curp/src/server/raw_curp/mod.rs:1297`
  - If a follower is down for a long time, a buffered local snapshot could help.

- [x] **TODO: Replace connect list with a proper queue**
  - File: `crates/curp/src/server/raw_curp/mod.rs:351`
  - **Fixed:** Replaced `HashMap<LogIndex, Arc<ResponseSender>>` with `VecDeque<(LogIndex, Arc<ResponseSender>)>`
    (type-aliased as `RespTxQueue`). Insertions use `push_back()` in `push_logs()`, and removals
    use conditional `pop_front()` in `apply()` — matching the sequential log-index access pattern.

## Medium Priority

### Refactoring

- [ ] **TODO: Refactor curp_node to use builder pattern (too many arguments)**
  - File: `crates/curp/src/server/curp_node.rs:839`
  - Also: `crates/curp/src/server/mod.rs:265`

- [ ] **TODO: Split xline_server large function into multiple functions**
  - File: `crates/xline/src/server/xline_server.rs:434`

- [ ] **TODO: Split too-long kv_store test**
  - File: `crates/xline/src/storage/kv_store.rs:1638`

- [ ] **TODO: Tidy up raw_curp handlers**
  - File: `crates/curp/src/server/raw_curp/mod.rs:485`

- [ ] **TODO: Refactor xline module to remove module_name_repetitions**
  - File: `crates/xline/src/lib.rs:150`

- [ ] **TODO: Refine conflict pool code for reusability**
  - File: `crates/xline/src/conflict/mod.rs:21`

- [ ] **TODO: Remove physical process logic from kv_store**
  - File: `crates/xline/src/storage/kv_store.rs:1120`
  - Move compact physical process logic elsewhere.

- [ ] **TODO: Simplify deserialization structure in propose_impl**
  - File: `crates/curp/src/client/unary/propose_impl.rs:301`

### Client Improvements

- [ ] **TODO: Allow external custom interceptors instead of passing token**
  - File: `crates/curp/src/client/mod.rs:75`

- [ ] **TODO: Implement request tracker in unary client**
  - File: `crates/curp/src/client/unary/mod.rs:114`

- [ ] **TODO: Implement batched read index processing**
  - File: `crates/curp/src/client/unary/propose_impl.rs:79`

- [ ] **TODO: Batch accelerate tracker**
  - File: `crates/curp/src/tracker.rs:239`

### Storage / Xline Server

- [x] **TODO: Lease extend should use election timeout, not hardcoded 1s**
  - File: `crates/xline/src/state.rs:29`
  - **Fixed:** Replaced hardcoded `Duration::from_secs(1)` with `heartbeat_interval * follower_timeout_ticks`
    computed from `CurpConfig`. The `State` struct now stores `election_timeout` and passes it to
    `promote()` on election win. Default: 300ms * 5 = 1500ms (was 1000ms).

- [x] **TODO: Return only SyncResponse from lease_store**
  - File: `crates/xline/src/storage/lease_store/mod.rs:117`
  - **Fixed:** Changed `after_sync` return type from `Result<(SyncResponse, Vec<WriteOp>), ExecuteError>`
    to `Result<SyncResponse, ExecuteError>` since lease operations never produce write ops. The caller
    in `command.rs` now wraps the lease branch result with `(asr, vec![])` to match the other backends.

- [ ] **TODO: Some requests allowed without token when auth enabled**
  - File: `crates/xline/src/storage/auth_store/store.rs:948`

- [ ] **TODO: Remove kvwatcher workaround after issue #491 closed**
  - File: `crates/xline/src/storage/kvwatcher.rs:64`

### WAL

- [x] **TODO: Remove `#![allow(unused)]` from WAL module**
  - File: `crates/curp/src/server/storage/wal/mod.rs:1`
  - **Fixed:** Removed blanket `#![allow(unused)]` and cleaned up all 40+ warnings across 8 WAL files:
    removed unused imports, dead code (`new_memory()`, `SegmentRemover::rwal_path` field, `WALSegment::size()`),
    gated test-only items with `#[cfg(test)]` (`get_ref()`, `with_max_segment_size()`), fixed unused mut
    variables, handled `#[must_use]` results properly, and converted a doc comment on `thread_local!` to
    a regular comment.

- [x] **TODO: Fix 8-byte alignment in WAL codec**
  - File: `crates/curp/src/server/storage/wal/codec.rs:105`
  - **Fixed:** Implemented 8-byte alignment for WAL Entry frames to prevent torn writes.
    Entry frame payloads are now padded to the next 8-byte boundary. The padding length
    (1-7 bytes) is encoded into bits 48-55 of the 7-byte header length field (bit 7 = flag,
    bits 0-2 = pad count). Seal frames (8 bytes) and Commit frames (40 bytes) were already
    aligned. Added `decode_frame_size()` to extract padding info during decoding, and updated
    `encode_frame_size()` to use bits 48-55 (not 56-63, since only 7 header bytes are stored).
    Added 7 unit tests covering encode/decode roundtrips, alignment verification, multi-frame
    batches, and corrupted-padding checksum detection.

## Low Priority

### CLI / UX

- [ ] **TODO: Implement interactive mode for user passwd**
  - File: `crates/xlinectl/src/command/user/passwd.rs:10`

- [ ] **TODO: Support reading value from stdin in put command**
  - File: `crates/xlinectl/src/command/put.rs:17`

### Testing

- [ ] **TODO: Rewrite tests for propose_stream**
  - Files:
    - `crates/curp/tests/it/server.rs:121`
    - `crates/curp/tests/it/server.rs:176`
    - `crates/curp/src/client/tests.rs:281`
    - `crates/curp/src/server/raw_curp/tests.rs:148, 161, 184, 200, 214, 621, 926`

- [ ] **TODO: Add more simulation tests**
  - File: `crates/simulation/tests/it/xline.rs:8`

### Miscellaneous

- [ ] **TODO: Return reference type from xlineapi to avoid copying**
  - File: `crates/xlineapi/src/lib.rs:421`

- [ ] **TODO: Use `type_alias_impl_trait` when stabilized**
  - Files: `crates/xlineapi/src/command.rs:20`, `crates/simulation/src/curp_group.rs:43`
  - Blocked on Rust language feature stabilization.

- [ ] **TODO: Clean snapshot after stream generation**
  - File: `crates/curp/src/rpc/connect.rs:873`

- [ ] **TODO: Use `tonic::Status` instead of `CurpError` in reconnect**
  - File: `crates/curp/src/rpc/reconnect.rs:57`

- [ ] **TODO: Implement hash_revision (etcd 3.6 compat)**
  - File: `crates/xline/src/server/maintenance.rs:183`
  - `hash_revision` was introduced in etcd 3.6; xline is currently etcd 3.5 compatible.

- [ ] **FIXME: Nested retry timeout issue with etcd client**
  - File: `crates/utils/src/config.rs:375`
  - etcd client has its own retry mechanism which may lead to nested retry timeouts.

- [ ] **FIXME: madsim single-threaded synchronous wait issue**
  - File: `crates/xline/src/storage/kv_store.rs:1122`
  - Cannot use synchronous wait in madsim's single-threaded environment.

- [ ] **TODO: Add mutex on metrics path**
  - File: `crates/xline/src/metrics.rs:172`

- [x] **TODO: Avoid allocation during locking in log**
  - File: `crates/curp/src/server/raw_curp/log.rs:447`
  - **Fixed:** Split `Log::push()` into `prepare_entry()` (allocates Arc, computes bincode size
    outside the lock) and `push_prepared()` (assigns sequential index and inserts under the lock).
    Updated 4 callers (`push_logs`, `handle_shutdown`, `handle_propose_conf_change`,
    `handle_publish`, `become_candidate`) to prepare entries before acquiring the write lock.
    The convenience `push()` method is retained for callers that already hold the lock.
    Added 2 unit tests verifying index assignment and size consistency.
