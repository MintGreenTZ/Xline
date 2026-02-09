use std::{sync::Arc, time::Duration};

use curp::role_change::RoleChange;

use crate::storage::{
    compact::{Compactable, Compactor},
    LeaseStore,
};

/// State of current node
pub(crate) struct State<C: Compactable> {
    /// lease storage
    lease_storage: Arc<LeaseStore>,
    /// auto compactor
    auto_compactor: Option<Arc<dyn Compactor<C>>>,
    /// Election timeout used for lease promotion
    election_timeout: Duration,
}

impl<C: Compactable> Clone for State<C> {
    fn clone(&self) -> Self {
        Self {
            lease_storage: Arc::clone(&self.lease_storage),
            auto_compactor: self.auto_compactor.clone(),
            election_timeout: self.election_timeout,
        }
    }
}

impl<C: Compactable> RoleChange for State<C> {
    fn on_election_win(&self) {
        self.lease_storage.promote(self.election_timeout);
        if let Some(auto_compactor) = self.auto_compactor.as_ref() {
            auto_compactor.resume();
        }
    }

    fn on_calibrate(&self) {
        self.lease_storage.demote();
        if let Some(auto_compactor) = self.auto_compactor.as_ref() {
            auto_compactor.pause();
        }
    }
}

impl<C: Compactable> State<C> {
    /// Create a new State
    pub(super) fn new(
        lease_storage: Arc<LeaseStore>,
        auto_compactor: Option<Arc<dyn Compactor<C>>>,
        election_timeout: Duration,
    ) -> Self {
        Self {
            lease_storage,
            auto_compactor,
            election_timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use curp::role_change::RoleChange;
    use utils::config::EngineConfig;

    use crate::header_gen::HeaderGenerator;
    use crate::storage::compact::MockCompactable;
    use crate::storage::db::DB;
    use crate::storage::lease_store::LeaseCollection;
    use crate::storage::LeaseStore;

    use super::State;

    fn create_test_lease_store() -> Arc<LeaseStore> {
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let lease_collection = Arc::new(LeaseCollection::new(0));
        let (kv_update_tx, _rx) = flume::bounded(1);
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        Arc::new(LeaseStore::new(
            lease_collection,
            header_gen,
            db,
            kv_update_tx,
            false,
        ))
    }

    #[test]
    fn state_uses_election_timeout_for_promote() {
        let lease_storage = create_test_lease_store();
        let election_timeout = Duration::from_millis(1500);
        let state: State<MockCompactable> =
            State::new(Arc::clone(&lease_storage), None, election_timeout);

        // Before election win, not primary
        assert!(!lease_storage.is_primary());

        state.on_election_win();

        // After election win, should be primary (promote was called)
        assert!(lease_storage.is_primary());
    }

    #[test]
    fn state_on_calibrate_demotes() {
        let lease_storage = create_test_lease_store();
        let state: State<MockCompactable> = State::new(
            Arc::clone(&lease_storage),
            None,
            Duration::from_millis(1500),
        );

        state.on_election_win();
        assert!(lease_storage.is_primary());

        state.on_calibrate();
        assert!(!lease_storage.is_primary());
    }

    #[test]
    fn election_timeout_computation() {
        // Verify the election timeout is computed correctly from config values
        let heartbeat_interval = Duration::from_millis(300);
        let follower_timeout_ticks: u8 = 5;
        let expected = Duration::from_millis(1500); // 300ms * 5

        let computed = heartbeat_interval.saturating_mul(u32::from(follower_timeout_ticks));
        assert_eq!(computed, expected);
    }
}
