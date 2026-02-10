use curp_test_utils::{mock_role_change, test_cmd::TestCommand, TestRoleChange, TEST_CLIENT_ID};
use test_macros::abort_on_panic;
use tokio::time::{sleep, Instant};
use tracing_test::traced_test;
use utils::config::{
    default_candidate_timeout_ticks, default_follower_timeout_ticks, default_heartbeat_interval,
    CurpConfigBuilder,
};

use super::*;
use crate::{
    rpc::{connect::MockInnerConnectApi, Redirect},
    server::{
        cmd_board::CommandBoard,
        conflict::test_pools::{TestSpecPool, TestUncomPool},
        lease_manager::LeaseManager,
    },
    tracker::Tracker,
    LogIndex,
};

// Hooks for tests
impl RawCurp<TestCommand, TestRoleChange> {
    fn role(&self) -> Role {
        self.st.read().role
    }

    fn contains(&self, id: ServerId) -> bool {
        self.cluster().all_members().contains_key(&id)
            && self.ctx.sync_events.contains_key(&id)
            && self.lst.get_all_statuses().contains_key(&id)
            && self.cst.lock().config.contains(id)
    }

    #[allow(clippy::mem_forget)] // we should prevent the channel from being dropped
    pub(crate) fn new_test(
        n: u64,
        role_change: TestRoleChange,
        task_manager: Arc<TaskManager>,
    ) -> Self {
        let all_members: HashMap<_, _> = (0..n)
            .map(|i| (format!("S{i}"), vec![format!("S{i}")]))
            .collect();
        let cluster_info = Arc::new(ClusterInfo::from_members_map(all_members, [], "S0"));
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let lease_manager = Arc::new(RwLock::new(LeaseManager::new()));
        let sync_events = cluster_info
            .peers_ids()
            .into_iter()
            .map(|id| (id, Arc::new(Event::new())))
            .collect();
        let connects = cluster_info
            .peers_ids()
            .into_iter()
            .map(|id| {
                (
                    id,
                    InnerConnectApiWrapper::new_from_arc(Arc::new(MockInnerConnectApi::new())),
                )
            })
            .collect();
        let curp_config = CurpConfigBuilder::default()
            .log_entries_cap(10)
            .build()
            .unwrap();
        let curp_storage = Arc::new(DB::open(&curp_config.engine_cfg).unwrap());
        let _ignore = curp_storage.recover().unwrap();

        // bypass test client id
        lease_manager.write().bypass(TEST_CLIENT_ID);

        let sp = Arc::new(Mutex::new(SpeculativePool::new(vec![Box::new(
            TestSpecPool::default(),
        )])));
        let ucp = Arc::new(Mutex::new(UncommittedPool::new(vec![Box::new(
            TestUncomPool::default(),
        )])));
        let (as_tx, as_rx) = flume::unbounded();
        std::mem::forget(as_rx);
        let resp_txs = Arc::new(Mutex::default());
        let id_barrier = Arc::new(IdBarrier::new());

        Self::builder()
            .cluster_info(cluster_info)
            .is_leader(true)
            .cmd_board(cmd_board)
            .lease_manager(lease_manager)
            .cfg(Arc::new(curp_config))
            .sync_events(sync_events)
            .role_change(role_change)
            .task_manager(task_manager)
            .connects(connects)
            .curp_storage(curp_storage)
            .spec_pool(sp)
            .uncommitted_pool(ucp)
            .as_tx(as_tx)
            .resp_txs(resp_txs)
            .id_barrier(id_barrier)
            .build_raw_curp()
            .unwrap()
    }

    /// Set connect for a server
    pub(crate) fn set_connect(&self, id: ServerId, connect: InnerConnectApiWrapper) {
        self.ctx.connects.entry(id).and_modify(|c| *c = connect);
    }

    pub(crate) fn tracker(&self, client_id: u64) -> Tracker {
        self.ctx
            .cb
            .read()
            .trackers
            .get(&client_id)
            .cloned()
            .unwrap_or_else(|| unreachable!("cannot find {client_id} in result trackers"))
    }

    /// Add a new cmd to the log, will return log entry index
    pub(crate) fn push_cmd(&self, propose_id: ProposeId, cmd: Arc<TestCommand>) -> LogIndex {
        let st_r = self.st.read();
        let mut log_w = self.log.write();
        log_w.push(st_r.term, propose_id, cmd).index
    }

    pub(crate) fn check_learner(&self, node_id: ServerId, is_learner: bool) -> bool {
        self.lst
            .get_all_statuses()
            .get(&node_id)
            .is_some_and(|f| f.is_learner == is_learner)
            && self
                .cluster()
                .all_members()
                .get(&node_id)
                .is_some_and(|m| m.is_learner == is_learner)
            && self.cst.map_lock(|cst_l| {
                cst_l.config.learners.contains(&node_id) == is_learner
                    && cst_l.config.voters().contains(&1) != is_learner
            })
    }

    /// Set term for testing purposes (keeps leader role)
    pub(crate) fn set_term_for_test(&self, term: u64) {
        self.st.write().term = term;
    }
}

/*************** tests for propose **************/
// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
#[traced_test]
#[test]
fn leader_handle_propose_will_succeed() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };
    let cmd = Arc::new(TestCommand::default());
    assert!(curp
        .handle_propose(ProposeId(TEST_CLIENT_ID, 0), cmd, 0)
        .unwrap());
}

// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
#[traced_test]
#[test]
fn leader_handle_propose_will_reject_conflicted() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };

    let cmd1 = Arc::new(TestCommand::new_put(vec![1], 0));
    assert!(curp
        .handle_propose(ProposeId(TEST_CLIENT_ID, 0), cmd1, 0)
        .unwrap());

    let cmd2 = Arc::new(TestCommand::new_put(vec![1, 2], 1));
    let res = curp.handle_propose(ProposeId(TEST_CLIENT_ID, 1), cmd2, 1);
    assert!(matches!(res, Err(CurpError::KeyConflict(()))));

    // leader will also reject cmds that conflict un-synced cmds
    let cmd3 = Arc::new(TestCommand::new_put(vec![2], 1));
    let res = curp.handle_propose(ProposeId(TEST_CLIENT_ID, 2), cmd3, 2);
    assert!(matches!(res, Err(CurpError::KeyConflict(()))));
}

// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
#[traced_test]
#[test]
fn leader_handle_propose_will_reject_duplicated() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };
    let cmd = Arc::new(TestCommand::default());
    assert!(curp
        .handle_propose(ProposeId(TEST_CLIENT_ID, 0), Arc::clone(&cmd), 0)
        .unwrap());

    let res = curp.handle_propose(ProposeId(TEST_CLIENT_ID, 0), cmd, 0);
    assert!(matches!(res, Err(CurpError::Duplicated(()))));
}

// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
#[traced_test]
#[test]
fn follower_handle_propose_will_succeed() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);
    let cmd = Arc::new(TestCommand::new_get(vec![1]));
    assert!(!curp
        .handle_propose(ProposeId(TEST_CLIENT_ID, 0), cmd, 0)
        .unwrap());
}

// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
#[traced_test]
#[test]
fn follower_handle_propose_will_reject_conflicted() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let cmd1 = Arc::new(TestCommand::new_get(vec![1]));
    assert!(!curp
        .handle_propose(ProposeId(TEST_CLIENT_ID, 0), cmd1, 0)
        .unwrap());

    let cmd2 = Arc::new(TestCommand::new_get(vec![1]));
    let res = curp.handle_propose(ProposeId(TEST_CLIENT_ID, 1), cmd2, 1);
    assert!(matches!(res, Err(CurpError::KeyConflict(()))));
}

/*************** tests for append_entries(heartbeat) **************/

#[traced_test]
#[test]
fn heartbeat_will_calibrate_term() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };

    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let result = curp.handle_append_entries_resp(s1_id, None, 2, false, 1);
    assert!(result.is_err());

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 2);
    assert_eq!(st_r.role, Role::Follower);
}

#[traced_test]
#[test]
fn heartbeat_will_calibrate_next_index() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = RawCurp::new_test(3, mock_role_change(), task_manager);

    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let result = curp.handle_append_entries_resp(s1_id, None, 0, false, 1);
    assert_eq!(result, Ok(false));

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 1);
    assert_eq!(curp.lst.get_next_index(s1_id), Some(1));
}

#[traced_test]
#[test]
fn handle_ae_will_calibrate_term() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);
    let s2_id = curp.cluster().get_id_by_name("S2").unwrap();

    let result = curp.handle_append_entries(2, s2_id, 0, 0, vec![], 0);
    assert!(result.is_ok());

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 2);
    assert_eq!(st_r.role, Role::Follower);
    assert_eq!(st_r.leader_id, Some(s2_id));
}

#[traced_test]
#[test]
fn handle_ae_will_set_leader_id() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
    let result = curp.handle_append_entries(1, s2_id, 0, 0, vec![], 0);
    assert!(result.is_ok());

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 1);
    assert_eq!(st_r.role, Role::Follower);
    assert_eq!(st_r.leader_id, Some(s2_id));
}

#[traced_test]
#[test]
fn handle_ae_will_reject_wrong_term() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
    let result = curp.handle_append_entries(0, s2_id, 0, 0, vec![], 0);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().0, 1);
}

#[traced_test]
#[test]
fn handle_ae_will_reject_wrong_log() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
    let result = curp.handle_append_entries(
        1,
        s2_id,
        1,
        1,
        vec![LogEntry::new(
            2,
            1,
            ProposeId(TEST_CLIENT_ID, 0),
            Arc::new(TestCommand::default()),
        )],
        0,
    );
    assert_eq!(result, Err((1, 1)));
}

/*************** tests for election **************/

#[traced_test]
#[tokio::test]
#[abort_on_panic]
async fn follower_will_not_start_election_when_heartbeats_are_received() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let curp_c = Arc::clone(&curp);
    let handle = tokio::spawn(async move {
        loop {
            sleep(default_heartbeat_interval()).await;
            let action = curp_c.tick_election();
            assert!(matches!(action, None));
        }
    });

    for _ in 0..default_follower_timeout_ticks() * 5 {
        sleep(default_heartbeat_interval()).await;
        curp.reset_election_tick();
    }

    assert!(!handle.is_finished());
    handle.abort();
}

#[traced_test]
#[tokio::test]
#[abort_on_panic]
async fn follower_or_pre_candidate_will_start_election_if_timeout() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let start = Instant::now();
    let mut follower_election = None;
    loop {
        sleep(default_heartbeat_interval()).await;
        let role = curp.role();
        let action = curp.tick_election();
        if matches!(action, Some(_)) && role == Role::Follower {
            let now = Instant::now();
            let dur = now - start;
            assert!(dur >= default_heartbeat_interval() * default_follower_timeout_ticks() as u32);
            assert!(
                dur <= default_heartbeat_interval() * default_follower_timeout_ticks() as u32 * 2
                    + default_heartbeat_interval() // plus another interval to tolerate deviation
            );
            follower_election = Some(now);
        }
        if matches!(action, Some(_)) && role == Role::PreCandidate {
            let prev = follower_election.unwrap();
            let now = Instant::now();

            let dur = now - prev;
            assert!(dur >= default_heartbeat_interval() * default_candidate_timeout_ticks() as u32);
            assert!(
                dur <= default_heartbeat_interval() * default_candidate_timeout_ticks() as u32 * 2
                    + default_heartbeat_interval() // plus another interval to tolerate deviation
            );
            return;
        }
    }
}

#[traced_test]
#[test]
fn handle_vote_will_calibrate_term() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.st.write().leader_id = None;

    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let result = curp.handle_vote(2, s1_id, 0, 0).unwrap();
    assert_eq!(result.0, 2);

    assert_eq!(curp.term(), 2);
    assert_eq!(curp.role(), Role::Follower);
}

#[traced_test]
#[test]
fn handle_vote_will_reject_smaller_term() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 2);

    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let result = curp.handle_vote(1, s1_id, 0, 0);
    assert_eq!(result.unwrap_err(), Some(2));
}

// #[traced_test]
#[test]
fn handle_vote_will_reject_outdated_candidate() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
    let result = curp.handle_append_entries(
        2,
        s2_id,
        0,
        0,
        vec![LogEntry::new(
            1,
            1,
            ProposeId(TEST_CLIENT_ID, 0),
            Arc::new(TestCommand::default()),
        )],
        0,
    );
    assert!(result.is_ok());
    curp.st.write().leader_id = None;
    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let result = curp.handle_vote(3, s1_id, 0, 0);
    assert_eq!(result.unwrap_err(), Some(3));
}

#[traced_test]
#[test]
fn pre_candidate_will_become_candidate_then_become_leader_after_election_succeeds() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    // tick till election starts
    while curp.role() != Role::PreCandidate {
        let _ig = curp.tick_election();
    }

    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let result = curp.handle_pre_vote_resp(s1_id, 2, true).unwrap();
    assert!(result.is_some());
    assert_eq!(curp.role(), Role::Candidate);

    let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
    let result = curp.handle_pre_vote_resp(s2_id, 2, true);
    assert!(result.is_err());
    assert_eq!(curp.role(), Role::Candidate);

    let result = curp.handle_vote_resp(s1_id, 2, true, vec![]).unwrap();
    assert!(result);
    assert_eq!(curp.role(), Role::Leader);

    let result = curp.handle_vote_resp(s2_id, 2, true, vec![]);
    assert!(result.is_err());
    assert_eq!(curp.role(), Role::Leader);
}

#[traced_test]
#[test]
fn vote_will_calibrate_pre_candidate_term() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    // tick till election starts
    while curp.role() != Role::PreCandidate {
        let _ig = curp.tick_election();
    }

    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let result = curp.handle_vote_resp(s1_id, 3, false, vec![]);
    assert!(result.is_err());

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 3);
    assert_eq!(st_r.role, Role::Follower);
}

/*************** tests for recovery **************/

#[traced_test]
#[test]
fn recover_from_spec_pools_will_pick_the_correct_cmds() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(5, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    // cmd1 has already been committed
    let cmd0 = Arc::new(TestCommand::new_put(vec![1], 1));
    // cmd2 has been speculatively successfully but not committed yet
    let cmd1 = Arc::new(TestCommand::new_put(vec![2], 1));
    // cmd3 has been speculatively successfully by the leader but not stored by the superquorum of the followers
    let cmd2 = Arc::new(TestCommand::new_put(vec![3], 1));
    curp.push_cmd(ProposeId(TEST_CLIENT_ID, 0), Arc::clone(&cmd0));
    curp.log.map_write(|mut log_w| log_w.commit_index = 1);

    let s0_id = curp.cluster().get_id_by_name("S0").unwrap();
    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
    let s3_id = curp.cluster().get_id_by_name("S3").unwrap();
    let s4_id = curp.cluster().get_id_by_name("S4").unwrap();

    let spec_pools = HashMap::from([
        (
            s0_id,
            vec![
                PoolEntry::new(ProposeId(TEST_CLIENT_ID, 1), Arc::clone(&cmd1)),
                PoolEntry::new(ProposeId(TEST_CLIENT_ID, 2), Arc::clone(&cmd2)),
            ],
        ),
        (
            s1_id,
            vec![PoolEntry::new(
                ProposeId(TEST_CLIENT_ID, 1),
                Arc::clone(&cmd1),
            )],
        ),
        (
            s2_id,
            vec![PoolEntry::new(
                ProposeId(TEST_CLIENT_ID, 1),
                Arc::clone(&cmd1),
            )],
        ),
        (
            s3_id,
            vec![PoolEntry::new(
                ProposeId(TEST_CLIENT_ID, 1),
                Arc::clone(&cmd1),
            )],
        ),
        (s4_id, vec![]),
    ]);

    curp.recover_from_spec_pools(&mut *curp.st.write(), &mut *curp.log.write(), spec_pools);

    curp.log.map_read(|log_r| {
        assert_eq!(log_r[1].propose_id, ProposeId(TEST_CLIENT_ID, 0));
        assert_eq!(log_r[2].propose_id, ProposeId(TEST_CLIENT_ID, 1));
        assert_eq!(log_r.last_log_index(), 2);
    });
}

#[traced_test]
#[test]
fn recover_ucp_from_logs_will_pick_the_correct_cmds() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(5, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let cmd0 = Arc::new(TestCommand::new_put(vec![1], 1));
    let cmd1 = Arc::new(TestCommand::new_put(vec![2], 1));
    let cmd2 = Arc::new(TestCommand::new_put(vec![3], 1));
    curp.push_cmd(ProposeId(TEST_CLIENT_ID, 0), Arc::clone(&cmd0));
    curp.push_cmd(ProposeId(TEST_CLIENT_ID, 1), Arc::clone(&cmd1));
    curp.push_cmd(ProposeId(TEST_CLIENT_ID, 2), Arc::clone(&cmd2));
    curp.log.map_write(|mut log_w| log_w.commit_index = 1);

    curp.recover_ucp_from_log(&mut *curp.log.write());

    curp.ctx.uncommitted_pool.map_lock(|ucp| {
        let mut ids: Vec<_> = ucp.all().into_iter().map(|entry| entry.id).collect();
        assert_eq!(ids.len(), 2);
        ids.sort();
        assert_eq!(ids[0], ProposeId(TEST_CLIENT_ID, 1));
        assert_eq!(ids[1], ProposeId(TEST_CLIENT_ID, 2));
    });
}

/*************** tests for leader retires **************/

/// To ensure #331 is fixed
#[traced_test]
#[test]
fn leader_retires_after_log_compact_will_succeed() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };
    let mut log_w = curp.log.write();
    for i in 1..=20 {
        let cmd = Arc::new(TestCommand::default());
        log_w.push(0, ProposeId(TEST_CLIENT_ID, i), cmd);
    }
    log_w.last_as = 20;
    log_w.last_exe = 20;
    log_w.commit_index = 20;
    log_w.compact();
    drop(log_w);

    curp.leader_retires();
}

// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
#[traced_test]
#[test]
fn leader_retires_should_cleanup() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };

    let _ignore = curp.handle_propose(
        ProposeId(TEST_CLIENT_ID, 0),
        Arc::new(TestCommand::new_put(vec![1], 0)),
        0,
    );
    let _ignore = curp.handle_propose(
        ProposeId(TEST_CLIENT_ID, 1),
        Arc::new(TestCommand::new_get(vec![1])),
        0,
    );

    curp.leader_retires();

    let cb_r = curp.ctx.cb.read();
    assert!(cb_r.er_buffer.is_empty(), "er buffer should be empty");
    assert!(cb_r.asr_buffer.is_empty(), "asr buffer should be empty");
    let ucp_l = curp.ctx.uncommitted_pool.lock();
    assert!(ucp_l.is_empty(), "ucp should be empty");
}

/*************** tests for other small functions **************/

#[traced_test]
#[tokio::test]
async fn leader_handle_shutdown_will_succeed() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };
    assert!(curp.handle_shutdown(ProposeId(TEST_CLIENT_ID, 0)).is_ok());
}

#[traced_test]
#[test]
fn follower_handle_shutdown_will_reject() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);
    let res = curp.handle_shutdown(ProposeId(TEST_CLIENT_ID, 0));
    assert!(matches!(
        res,
        Err(CurpError::Redirect(Redirect {
            leader_id: None,
            term: 1,
        }))
    ));
}

#[traced_test]
#[test]
fn is_synced_should_return_true_when_followers_caught_up_with_leader() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { RawCurp::new_test(3, mock_role_change(), task_manager) };

    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
    curp.log.write().commit_index = 3;
    assert!(!curp.is_synced(s1_id));
    assert!(!curp.is_synced(s2_id));

    curp.lst.update_match_index(s1_id, 3);
    curp.lst.update_match_index(s2_id, 3);
    assert!(curp.is_synced(s1_id));
    assert!(curp.is_synced(s2_id));
}

#[traced_test]
#[test]
fn add_node_should_add_new_node_to_curp() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    let old_cluster = curp.cluster().clone();
    let changes = vec![ConfChange::add(1, vec!["http://127.0.0.1:4567".to_owned()])];
    assert!(curp.check_new_config(&changes).is_ok());
    let info = curp.apply_conf_change(changes.clone()).unwrap();
    assert!(curp.contains(1));
    curp.fallback_conf_change(changes, info);
    let cluster_after_fallback = curp.cluster();
    assert_eq!(
        old_cluster.cluster_id(),
        cluster_after_fallback.cluster_id()
    );
    assert_eq!(old_cluster.self_id(), cluster_after_fallback.self_id());
    assert_eq!(
        old_cluster.all_members(),
        cluster_after_fallback.all_members()
    );
    assert_eq!(
        cluster_after_fallback.cluster_version(),
        old_cluster.cluster_version()
    );
}

#[traced_test]
#[test]
fn add_learner_node_and_promote_should_success() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    let changes = vec![ConfChange::add_learner(
        1,
        vec!["http://127.0.0.1:4567".to_owned()],
    )];
    assert!(curp.check_new_config(&changes).is_ok());
    curp.apply_conf_change(changes);
    assert!(curp.check_learner(1, true));

    let changes = vec![ConfChange::promote(1)];
    assert!(curp.check_new_config(&changes).is_ok());
    let info = curp.apply_conf_change(changes.clone()).unwrap();
    assert!(curp.check_learner(1, false));
    curp.fallback_conf_change(changes, info);
    assert!(curp.check_learner(1, true));
}

#[traced_test]
#[test]
fn add_exists_node_should_return_node_already_exists_error() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    let exists_node_id = curp.cluster().get_id_by_name("S1").unwrap();
    let changes = vec![ConfChange::add(
        exists_node_id,
        vec!["http://127.0.0.1:4567".to_owned()],
    )];
    let resp = curp.check_new_config(&changes);
    let error_match = matches!(resp, Err(CurpError::NodeAlreadyExists(())));
    assert!(error_match);
}

#[traced_test]
#[test]
fn remove_node_should_remove_node_from_curp() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(5, mock_role_change(), task_manager)) };
    let old_cluster = curp.cluster().clone();
    let follower_id = curp.cluster().get_id_by_name("S1").unwrap();
    let changes = vec![ConfChange::remove(follower_id)];
    assert!(curp.check_new_config(&changes).is_ok());
    let info = curp.apply_conf_change(changes.clone()).unwrap();
    assert_eq!(info.addrs, vec!["S1".to_owned()]);
    assert_eq!(info.name, "S1");
    assert!(!info.is_learner);
    assert!(!curp.contains(follower_id));
    curp.fallback_conf_change(changes, info);
    let cluster_after_fallback = curp.cluster();
    assert_eq!(
        old_cluster.cluster_id(),
        cluster_after_fallback.cluster_id()
    );
    assert_eq!(old_cluster.self_id(), cluster_after_fallback.self_id());
    assert_eq!(
        old_cluster.all_members(),
        cluster_after_fallback.all_members()
    );
}

#[traced_test]
#[test]
fn remove_non_exists_node_should_return_node_not_exists_error() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(5, mock_role_change(), task_manager)) };
    let changes = vec![ConfChange::remove(1)];
    let resp = curp.check_new_config(&changes);
    assert!(matches!(resp, Err(CurpError::NodeNotExists(()))));
}

#[traced_test]
#[test]
fn update_node_should_update_the_address_of_node() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    let old_cluster = curp.cluster().clone();
    let follower_id = curp.cluster().get_id_by_name("S1").unwrap();
    let mut mock_connect = MockInnerConnectApi::new();
    mock_connect.expect_update_addrs().returning(|_| Ok(()));
    curp.set_connect(
        follower_id,
        InnerConnectApiWrapper::new_from_arc(Arc::new(mock_connect)),
    );
    assert_eq!(
        curp.cluster().peer_urls(follower_id),
        Some(vec!["S1".to_owned()])
    );
    let changes = vec![ConfChange::update(
        follower_id,
        vec!["http://127.0.0.1:4567".to_owned()],
    )];
    assert!(curp.check_new_config(&changes).is_ok());
    let info = curp.apply_conf_change(changes.clone()).unwrap();
    assert_eq!(info.addrs, vec!["S1".to_owned()]);
    assert_eq!(
        curp.cluster().peer_urls(follower_id),
        Some(vec!["http://127.0.0.1:4567".to_owned()])
    );
    curp.fallback_conf_change(changes, info);
    let cluster_after_fallback = curp.cluster();
    assert_eq!(
        old_cluster.cluster_id(),
        cluster_after_fallback.cluster_id()
    );
    assert_eq!(old_cluster.self_id(), cluster_after_fallback.self_id());
    assert_eq!(
        old_cluster.all_members(),
        cluster_after_fallback.all_members()
    );
}

#[traced_test]
#[test]
fn leader_handle_propose_conf_change() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    let follower_id = curp.cluster().get_id_by_name("S1").unwrap();
    assert_eq!(
        curp.cluster().peer_urls(follower_id),
        Some(vec!["S1".to_owned()])
    );
    let changes = vec![ConfChange::update(
        follower_id,
        vec!["http://127.0.0.1:4567".to_owned()],
    )];
    curp.handle_propose_conf_change(ProposeId(TEST_CLIENT_ID, 0), changes)
        .unwrap();
}

#[traced_test]
#[test]
fn follower_handle_propose_conf_change() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 2);

    let follower_id = curp.cluster().get_id_by_name("S1").unwrap();
    assert_eq!(
        curp.cluster().peer_urls(follower_id),
        Some(vec!["S1".to_owned()])
    );
    let changes = vec![ConfChange::update(
        follower_id,
        vec!["http://127.0.0.1:4567".to_owned()],
    )];
    let result = curp.handle_propose_conf_change(ProposeId(TEST_CLIENT_ID, 0), changes);
    assert!(matches!(
        result,
        Err(CurpError::Redirect(Redirect {
            leader_id: None,
            term: 2,
        }))
    ));
}

#[traced_test]
#[test]
fn switch_config_add_uses_enriched_member_state() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };

    // Create a ConfChange with name and client_urls populated (as the leader would do)
    let cc = ConfChange::add(1, vec!["http://10.0.0.5:2380".to_owned()]).with_member_state(
        "node-5".to_owned(),
        vec!["http://10.0.0.5:2379".to_owned()],
        false,
    );
    curp.switch_config(cc);

    // Verify the member was added with the enriched state
    let member = curp.cluster().get(&1).expect("member 1 should exist");
    assert_eq!(member.name, "node-5");
    assert_eq!(
        member.peer_urls,
        vec!["http://10.0.0.5:2380".to_owned()]
    );
    assert_eq!(
        member.client_urls,
        vec!["http://10.0.0.5:2379".to_owned()]
    );
    assert!(!member.is_learner);
}

#[traced_test]
#[test]
fn switch_config_add_learner_uses_enriched_member_state() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };

    let cc =
        ConfChange::add_learner(1, vec!["http://10.0.0.5:2380".to_owned()]).with_member_state(
            "learner-5".to_owned(),
            vec!["http://10.0.0.5:2379".to_owned()],
            true,
        );
    curp.switch_config(cc);

    let member = curp.cluster().get(&1).expect("member 1 should exist");
    assert_eq!(member.name, "learner-5");
    assert!(member.is_learner);
    assert_eq!(
        member.client_urls,
        vec!["http://10.0.0.5:2379".to_owned()]
    );
}

#[traced_test]
#[test]
fn remove_fallback_restores_client_urls() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(5, mock_role_change(), task_manager)) };

    // First add a node with full member state
    let cc = ConfChange::add(1, vec!["http://10.0.0.5:2380".to_owned()]).with_member_state(
        "node-5".to_owned(),
        vec!["http://10.0.0.5:2379".to_owned()],
        false,
    );
    curp.switch_config(cc);
    assert!(curp.contains(1));

    // Remove the node
    let changes = vec![ConfChange::remove(1)];
    let info = curp.apply_conf_change(changes.clone()).unwrap();
    assert!(!curp.contains(1));

    // Verify fallback info captured client_urls
    assert_eq!(info.name, "node-5");
    assert_eq!(info.client_urls, vec!["http://10.0.0.5:2379".to_owned()]);

    // Fallback (undo the remove) should restore the member with client_urls
    curp.fallback_conf_change(changes, info);
    assert!(curp.contains(1));
    let member = curp.cluster().get(&1).expect("member 1 should exist after fallback");
    assert_eq!(member.name, "node-5");
    assert_eq!(
        member.client_urls,
        vec!["http://10.0.0.5:2379".to_owned()]
    );
}

#[traced_test]
#[test]
fn switch_config_update_missing_member_returns_none() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };

    // Try to update a non-existent node — should return None, not panic
    let cc = ConfChange::update(99999, vec!["http://10.0.0.99:2380".to_owned()]);
    let result = curp.switch_config(cc);
    assert!(result.is_none(), "Update of missing member should return None");
}

#[traced_test]
#[test]
fn switch_config_promote_missing_member_returns_none() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };

    // Try to promote a non-existent node — should return None, not panic
    let cc = ConfChange::promote(99999);
    let result = curp.switch_config(cc);
    assert!(result.is_none(), "Promote of missing member should return None");
}

#[traced_test]
#[test]
fn fallback_update_missing_member_does_not_panic() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };

    // Add a member, then remove it so it's gone
    let cc = ConfChange::add(1, vec!["http://10.0.0.5:2380".to_owned()]).with_member_state(
        "node-5".to_owned(),
        vec!["http://10.0.0.5:2379".to_owned()],
        false,
    );
    curp.switch_config(cc);
    curp.switch_config(ConfChange::remove(1));
    assert!(!curp.contains(1));

    // Attempting to fallback an Update for the now-missing member should not panic
    let changes = vec![ConfChange::update(1, vec!["http://10.0.0.5:2380".to_owned()])];
    let info = FallbackInfo {
        addrs: vec!["http://old:2380".to_owned()],
        name: String::new(),
        client_urls: vec![],
        is_learner: false,
    };
    curp.fallback_conf_change(changes, info);
    // Should complete without panicking
}

#[traced_test]
#[test]
fn fallback_promote_missing_member_does_not_panic() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };

    // Add a learner, then remove it
    let cc =
        ConfChange::add_learner(1, vec!["http://10.0.0.5:2380".to_owned()]).with_member_state(
            "learner-5".to_owned(),
            vec!["http://10.0.0.5:2379".to_owned()],
            true,
        );
    curp.switch_config(cc);
    curp.switch_config(ConfChange::remove(1));
    assert!(!curp.contains(1));

    // Attempting to fallback a Promote for the now-missing member should not panic
    let changes = vec![ConfChange::promote(1)];
    let info = FallbackInfo {
        addrs: vec![],
        name: String::new(),
        client_urls: vec![],
        is_learner: false,
    };
    curp.fallback_conf_change(changes, info);
    // Should complete without panicking
}

#[traced_test]
#[test]
fn leader_handle_move_leader() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.switch_config(ConfChange::add_learner(1234, vec!["address".to_owned()]));

    let res = curp.handle_move_leader(1234);
    assert!(res.is_err());

    let res = curp.handle_move_leader(12345);
    assert!(res.is_err());

    let target_id = curp.cluster().get_id_by_name("S1").unwrap();
    let res = curp.handle_move_leader(target_id);
    // need to send try become leader now after handle_move_leader
    assert!(res.is_ok_and(|b| b));

    let res = curp.handle_move_leader(target_id);
    // no need to send try become leader now after handle_move_leader, because it's duplicated
    assert!(res.is_ok_and(|b| !b));
}

#[traced_test]
#[test]
fn follower_handle_move_leader() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 2);

    let target_id = curp.cluster().get_id_by_name("S1").unwrap();
    let res = curp.handle_move_leader(target_id);
    assert!(matches!(res, Err(CurpError::Redirect(_))));
}

#[traced_test]
#[test]
fn leader_will_reset_transferee_after_remove_node() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(5, mock_role_change(), task_manager)) };

    let target_id = curp.cluster().get_id_by_name("S1").unwrap();
    let res = curp.handle_move_leader(target_id);
    assert!(res.is_ok_and(|b| b));
    assert_eq!(curp.get_transferee(), Some(target_id));

    curp.switch_config(ConfChange::remove(target_id));
    assert!(curp.get_transferee().is_none());
}

// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
#[traced_test]
#[test]
fn leader_will_reject_propose_when_transferring() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(5, mock_role_change(), task_manager)) };

    let target_id = curp.cluster().get_id_by_name("S1").unwrap();
    let res = curp.handle_move_leader(target_id);
    assert!(res.is_ok_and(|b| b));

    let propose_id = ProposeId(0, 0);
    let cmd = Arc::new(TestCommand::new_put(vec![1], 1));
    let res = curp.handle_propose(propose_id, cmd, 0);
    assert!(res.is_err());
}

#[traced_test]
#[test]
fn leader_will_reset_transferee_after_it_become_follower() {
    let task_manager = Arc::new(TaskManager::new());
    let curp = { Arc::new(RawCurp::new_test(5, mock_role_change(), task_manager)) };

    let target_id = curp.cluster().get_id_by_name("S1").unwrap();
    let res = curp.handle_move_leader(target_id);
    assert!(res.is_ok_and(|b| b));
    assert_eq!(curp.get_transferee(), Some(target_id));

    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 2);
    assert!(curp.get_transferee().is_none());
}

/*************** tests for verify_install_snapshot **************/

#[traced_test]
#[test]
fn verify_install_snapshot_accepts_when_snapshot_ahead() {
    // Follower has log at (index=0, term=0), snapshot is at (index=5, term=3)
    let task_manager = Arc::new(TaskManager::new());
    let curp = Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager));
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);
    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    assert!(curp.verify_install_snapshot(1, s1_id, 5, 3));
}

#[traced_test]
#[test]
fn verify_install_snapshot_accepts_same_index_and_term() {
    // Follower has log at (index=5, term=3) via base, snapshot also at (index=5, term=3)
    let task_manager = Arc::new(TaskManager::new());
    let curp = Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager));
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 3);
    {
        let mut log_w = curp.log.write();
        log_w.base_index = 5;
        log_w.base_term = 3;
    }
    assert!(curp.verify_install_snapshot(3, curp.id(), 5, 3));
}

#[traced_test]
#[test]
fn verify_install_snapshot_rejects_when_behind() {
    // Follower has log at (index=10, term=3), snapshot is at (index=5, term=3)
    let task_manager = Arc::new(TaskManager::new());
    let curp = Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager));
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 3);
    {
        let mut log_w = curp.log.write();
        log_w.base_index = 10;
        log_w.base_term = 3;
    }
    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    assert!(!curp.verify_install_snapshot(3, s1_id, 5, 3));
}

#[traced_test]
#[test]
fn verify_install_snapshot_accepts_higher_term() {
    // Follower has log at (index=10, term=2), snapshot is at (index=5, term=3)
    // Higher term wins regardless of index.
    let task_manager = Arc::new(TaskManager::new());
    let curp = Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager));
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 2);
    {
        let mut log_w = curp.log.write();
        log_w.base_index = 10;
        log_w.base_term = 2;
    }
    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    assert!(curp.verify_install_snapshot(3, s1_id, 5, 3));
}

#[traced_test]
#[test]
fn verify_install_snapshot_rejects_lower_term() {
    // Follower has log at (index=3, term=5), snapshot is at (index=10, term=2)
    // Lower term loses regardless of index.
    let task_manager = Arc::new(TaskManager::new());
    let curp = Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager));
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 5);
    {
        let mut log_w = curp.log.write();
        log_w.base_index = 3;
        log_w.base_term = 5;
    }
    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    assert!(!curp.verify_install_snapshot(5, s1_id, 10, 2));
}

#[traced_test]
#[test]
fn verify_install_snapshot_calibrates_term() {
    // Follower at term 1, snapshot arrives with term 3 — should step up
    let task_manager = Arc::new(TaskManager::new());
    let curp = Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager));
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);
    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    assert!(curp.verify_install_snapshot(3, s1_id, 5, 3));
    let st_r = curp.st.read();
    assert_eq!(st_r.term, 3);
    assert_eq!(st_r.leader_id, Some(s1_id));
}

#[traced_test]
#[test]
fn verify_install_snapshot_with_entries_ahead_of_snapshot() {
    // Follower has entries up to (index=8, term=3) — higher than snapshot
    // (index=5, term=3). Same term, follower ahead → reject.
    let task_manager = Arc::new(TaskManager::new());
    let curp = Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager));
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 3);
    // Push entries to extend the follower's log
    curp.push_cmd(
        ProposeId(TEST_CLIENT_ID, 0),
        Arc::new(TestCommand::default()),
    );
    curp.push_cmd(
        ProposeId(TEST_CLIENT_ID, 1),
        Arc::new(TestCommand::default()),
    );
    // entries are at index 1,2 with term 3; plus base at (0,0)
    // last_log_index = 2, last_log_term = 3
    let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
    // snapshot at (index=1, term=3) — behind follower's last index
    assert!(!curp.verify_install_snapshot(3, s1_id, 1, 3));
}

/// Helper to build a RawCurp simulating recovery with pre-existing log entries.
/// `n` is the number of members in the cluster (including self "S0"),
/// `last_applied` is the commit index at crash time,
/// `entries` are the log entries recovered from the WAL.
fn build_recovered_curp(
    n: u64,
    last_applied: LogIndex,
    entries: Vec<LogEntry<TestCommand>>,
) -> RawCurp<TestCommand, TestRoleChange> {
    let all_members: HashMap<_, _> = (0..n)
        .map(|i| (format!("S{i}"), vec![format!("S{i}")]))
        .collect();
    let cluster_info = Arc::new(ClusterInfo::from_members_map(all_members, [], "S0"));
    let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
    let lease_manager = Arc::new(RwLock::new(LeaseManager::new()));
    let sync_events = cluster_info
        .peers_ids()
        .into_iter()
        .map(|id| (id, Arc::new(Event::new())))
        .collect();
    let connects = cluster_info
        .peers_ids()
        .into_iter()
        .map(|id| {
            (
                id,
                InnerConnectApiWrapper::new_from_arc(Arc::new(MockInnerConnectApi::new())),
            )
        })
        .collect();
    let curp_config = CurpConfigBuilder::default()
        .log_entries_cap(100)
        .build()
        .unwrap();
    let curp_storage = Arc::new(DB::open(&curp_config.engine_cfg).unwrap());
    let _ignore = curp_storage.recover().unwrap();
    let sp = Arc::new(Mutex::new(SpeculativePool::new(vec![Box::new(
        TestSpecPool::default(),
    )])));
    let ucp = Arc::new(Mutex::new(UncommittedPool::new(vec![Box::new(
        TestUncomPool::default(),
    )])));
    let (as_tx, as_rx) = flume::unbounded();
    std::mem::forget(as_rx);
    let resp_txs = Arc::new(Mutex::default());
    let id_barrier = Arc::new(IdBarrier::new());
    let task_manager = Arc::new(TaskManager::new());

    RawCurp::builder()
        .cluster_info(cluster_info)
        .is_leader(false) // recovery starts as follower
        .cmd_board(cmd_board)
        .lease_manager(lease_manager)
        .cfg(Arc::new(curp_config))
        .sync_events(sync_events)
        .role_change(mock_role_change())
        .task_manager(task_manager)
        .connects(connects)
        .curp_storage(curp_storage)
        .last_applied(last_applied)
        .entries(entries)
        .spec_pool(sp)
        .uncommitted_pool(ucp)
        .as_tx(as_tx)
        .resp_txs(resp_txs)
        .id_barrier(id_barrier)
        .build_raw_curp()
        .unwrap()
}

#[traced_test]
#[test]
fn recovery_populates_fallback_contexts_for_uncommitted_add() {
    use crate::log_entry::LogEntry;

    // Simulate recovery with 2 entries: one committed Empty at index 1,
    // and one uncommitted ConfChange(Add) at index 2.
    let entries = vec![
        LogEntry::new(1, 1, ProposeId(0, 0), EntryData::<TestCommand>::Empty),
        LogEntry::new(
            2,
            1,
            ProposeId(0, 1),
            vec![ConfChange::add(
                99,
                vec!["http://new-node:2380".to_owned()],
            )
            .with_member_state(
                "new-node".to_owned(),
                vec!["http://new-node:2379".to_owned()],
                false,
            )],
        ),
    ];

    // last_applied=1 means entry at index 1 is committed, index 2 is uncommitted
    let curp = build_recovered_curp(3, 1, entries);

    // The uncommitted ConfChange at index 2 should have a fallback context
    let log_r = curp.log.read();
    assert!(
        log_r.fallback_contexts.contains_key(&2),
        "fallback context should exist for uncommitted ConfChange at index 2"
    );
    let ctx = &log_r.fallback_contexts[&2];
    assert_eq!(ctx.origin_entry.index, 2);
    // For Add, fallback info should have empty addrs (remove will be used to undo)
    assert!(ctx.addrs.is_empty());
}

#[traced_test]
#[test]
fn recovery_populates_fallback_contexts_for_uncommitted_remove() {
    use crate::log_entry::LogEntry;

    // Simulate recovery with an uncommitted Remove at index 2.
    // The ConfChange carries the removed member's state (Phase 1 enrichment).
    let entries = vec![
        LogEntry::new(1, 1, ProposeId(0, 0), EntryData::<TestCommand>::Empty),
        LogEntry::new(
            2,
            1,
            ProposeId(0, 1),
            vec![ConfChange::remove(99).with_member_state(
                "removed-node".to_owned(),
                vec!["http://removed:2379".to_owned()],
                false,
            )],
        ),
    ];

    let curp = build_recovered_curp(3, 1, entries);

    let log_r = curp.log.read();
    assert!(log_r.fallback_contexts.contains_key(&2));
    let ctx = &log_r.fallback_contexts[&2];
    // For Remove, fallback should carry the member's state for re-adding
    assert_eq!(ctx.name, "removed-node");
    assert_eq!(ctx.client_urls, vec!["http://removed:2379".to_owned()]);
}

#[traced_test]
#[test]
fn recovery_skips_committed_conf_change_entries() {
    use crate::log_entry::LogEntry;

    // Both entries are committed (last_applied=2), so no fallback contexts needed
    let entries = vec![
        LogEntry::new(1, 1, ProposeId(0, 0), EntryData::<TestCommand>::Empty),
        LogEntry::new(
            2,
            1,
            ProposeId(0, 1),
            vec![ConfChange::add(
                99,
                vec!["http://new-node:2380".to_owned()],
            )],
        ),
    ];

    let curp = build_recovered_curp(3, 2, entries);

    let log_r = curp.log.read();
    assert!(
        log_r.fallback_contexts.is_empty(),
        "committed conf changes should not have fallback contexts"
    );
}

#[traced_test]
#[test]
fn recovery_populates_fallback_for_multiple_uncommitted_conf_changes() {
    use crate::log_entry::LogEntry;

    // Index 1: committed Empty
    // Index 2: uncommitted Add
    // Index 3: uncommitted Promote (for same node)
    let entries = vec![
        LogEntry::new(1, 1, ProposeId(0, 0), EntryData::<TestCommand>::Empty),
        LogEntry::new(
            2,
            1,
            ProposeId(0, 1),
            vec![ConfChange::add_learner(
                99,
                vec!["http://new-node:2380".to_owned()],
            )
            .with_member_state(
                "new-node".to_owned(),
                vec!["http://new-node:2379".to_owned()],
                true,
            )],
        ),
        LogEntry::new(3, 1, ProposeId(0, 2), vec![ConfChange::promote(99)]),
    ];

    let curp = build_recovered_curp(3, 1, entries);

    let log_r = curp.log.read();
    assert!(
        log_r.fallback_contexts.contains_key(&2),
        "fallback context should exist for Add at index 2"
    );
    assert!(
        log_r.fallback_contexts.contains_key(&3),
        "fallback context should exist for Promote at index 3"
    );
    // Non-conf-change (index 1) should not have fallback
    assert!(!log_r.fallback_contexts.contains_key(&1));
}
