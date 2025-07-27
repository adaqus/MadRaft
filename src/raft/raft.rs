use crate::raft;

use self::{logs::Log, msg::*};
use core::sync;
use futures::{
    channel::mpsc,
    lock::Mutex as AsyncMutex,
    stream::{AbortHandle, Abortable, FuturesUnordered},
    FutureExt, SinkExt, StreamExt,
};
use madsim::{
    fs::{self, File},
    net::Endpoint,
    rand::{self, Rng},
    task::JoinHandle,
    time::{self, *},
    Request,
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::min,
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};
use tracing::{debug, info, trace, warn};
use tracing_subscriber::field::debug;
use transport::Transport;

mod logs;
mod msg;
mod transport;

#[derive(Clone)]
pub struct RaftHandle {
    inner: Arc<Mutex<Raft>>,
    heartbeat_sender: mpsc::UnboundedSender<Term>,
}

type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;
type Term = usize;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: usize,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: usize,
        index: usize,
    },
}

#[derive(Debug)]
pub struct Start {
    /// The index that the command will appear at if it's ever committed.
    pub index: usize,
    /// The current term.
    pub term: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("This node is not a leader, next leader: {0}")]
    NotLeader(usize),
    #[error("Outdated leader term, current: {current}, peer's: {peer}")]
    OutdatedTerm { current: usize, peer: usize },
    #[error("IO error")]
    IO(#[from] io::Error),
    #[error("Shutdown pending")]
    ShutdownPending,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

impl State {
    fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }
}

/// Data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    current_term: usize,
    voted_for: Option<usize>,
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // write!(f, "Raft({},t={},l=[{},{}],{:?})", self.me,)
        write!(
            f,
            "Raft({},t={},lt={},li={})",
            self.me,
            self.state.current_term,
            self.state.log.last_log_term(),
            self.state.log.len()
        )
    }
}

impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let (heartbeat_sender, mut heartbeat_receiver) = mpsc::unbounded();
        trace!("Binding node {} to {}", me + 1, peers[me]);
        let peers_len = peers.len();
        let ep = Arc::new(Endpoint::bind(peers[me]).await.expect("failed to bind"));
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            ep: ep.clone(),
            apply_ch,
            state: State::default(),
            self_weak_ref: Weak::new(),
            pending_election: None,
            heartbeat_task: None,
            commit_index: Arc::new(AtomicUsize::new(0)),
            match_index: vec![0; peers_len],
            last_applied: 0,
            follower_sync_tasks: Vec::new(),
            commit_index_tasks: Vec::new(),
        }));

        let handle = RaftHandle {
            inner,
            heartbeat_sender,
        };

        {
            let mut raft = handle.inner.lock().unwrap();
            raft.self_weak_ref = Arc::downgrade(&handle.inner);
        }

        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server(ep);

        let raft = handle.inner.clone();
        info!("{:?} created", raft.lock().unwrap());

        madsim::task::spawn(async move {
            loop {
                let heartbeat_timeout = Raft::generate_election_timeout();
                debug!("Raft({}): Heartbeat timeout: {:?}", me, heartbeat_timeout);
                let mut sleep = time::sleep(heartbeat_timeout).fuse();

                // If timeout and heartbeat happen simultaneously, prefer timeout
                futures::select_biased! {
                    _ = sleep => {
                        // Log a warning if no heartbeat was received in time.
                        let mut raft_guard = raft.lock().expect("unlock Raft");
                        warn!("{:?}: No heartbeat received within {:?}", *raft_guard, heartbeat_timeout);
                        if raft_guard.state.is_leader() {
                            info!("{:?}: I'm the leader, no need to start election", raft_guard);
                            continue;
                        }
                        trace!("Old state: {:?}", raft_guard.state);
                        raft_guard.state.role = Role::Candidate;
                        raft_guard.state.current_term += 1;
                        raft_guard.state.voted_for = Some(me);
                        trace!("New state: {:?}", raft_guard.state);
                        raft_guard.perform_election();
                    },
                    msg = heartbeat_receiver.next() => {
                        match msg {
                            None => {
                                warn!("Raft({me}): Heartbeat channel closed");
                                break;
                            }
                            Some(term) => {
                                let mut raft_guard = raft.lock().expect("unlock Raft");
                                debug!("{:?}: Heartbeat received (t={term}), reset election timeout.", raft_guard);
                                // If we receive a heartbeat here, it means we can safely set following state
                                raft_guard.pending_election.take().map(|e| e.abort());
                                // If we are a follower, we can reset the heartbeat task
                                raft_guard.heartbeat_task.take().map(|h| h.abort());
                                // If we are a follower, we do not sync with followers
                                raft_guard.follower_sync_tasks.iter_mut().for_each(|task| {
                                    task.abort();
                                });
                                raft_guard.follower_sync_tasks.clear();

                                trace!("Old state: {:?}", raft_guard.state);
                                raft_guard.state.role = Role::Follower;
                                if term > raft_guard.state.current_term {
                                    raft_guard.state.voted_for = None;
                                }
                                raft_guard.state.current_term = term;
                                trace!("New state: {:?}", raft_guard.state);
                            }
                        }
                    },
                }
            }
        });

        (handle, recver)
    }

    /// Start agreement on the next command to be appended to Raft's log.
    ///
    /// If this server isn't the leader, returns [`Error::NotLeader`].
    /// Otherwise start the agreement and return immediately.
    ///
    /// There is no guarantee that this command will ever be committed to the
    /// Raft log, since the leader may fail or lose an election.
    pub async fn start(&self, cmd: &[u8]) -> Result<Start> {
        let mut raft = self.inner.lock().unwrap();
        info!("{:?} start", *raft);
        raft.start(cmd)
    }

    /// The current term of this peer.
    pub fn term(&self) -> usize {
        let raft = self.inner.lock().unwrap();
        raft.state.current_term
    }

    /// The current term of this peer.
    pub fn voted_for(&self) -> Option<usize> {
        let raft = self.inner.lock().unwrap();
        raft.state.voted_for
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.inner.lock().unwrap();
        raft.state.is_leader()
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub async fn cond_install_snapshot(
        &self,
        _last_included_term: usize,
        _last_included_index: usize,
        _snapshot: &[u8],
    ) -> bool {
        todo!()
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub async fn snapshot(&self, index: usize, snapshot: &[u8]) -> Result<()> {
        todo!()
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self) -> io::Result<()> {
        let persist: Persist = Persist {
            current_term: self.term(),
            voted_for: self.voted_for(),
        };
        let snapshot: Vec<u8> = vec![]; //TODO real snapshot
        let state = bincode::serialize(&persist).unwrap();

        // you need to store persistent state in file "state"
        // and store snapshot in file "snapshot".
        // DO NOT change the file names.
        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        // make sure data is flushed to the disk,
        // otherwise data will be lost on power fail.
        file.sync_all().await?;

        let file = fs::File::create("snapshot").await?;
        file.write_all_at(&snapshot, 0).await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        match fs::read("snapshot").await {
            Ok(snapshot) => {
                let this = self.inner.lock().unwrap();
                // this.snapshot = snapshot;
                todo!("restore snapshot");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                todo!("restore state");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn start_rpc_server(&self, endpoint: Arc<Endpoint>) {
        let this = self.clone();
        endpoint.add_rpc_handler(move |args: RequestVoteArgs| {
            let this = this.clone();
            async move { this.request_vote_handler(args).await.unwrap() }
        });
        let this = self.clone();
        endpoint.add_rpc_handler(move |args: AppendEntriesArgs| {
            let mut this = this.clone();
            async move { this.append_entries_handler(args).await.unwrap() }
        });
    }

    async fn request_vote_handler(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.request_vote_handler(args)
        };
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }

    async fn append_entries_handler(
        &mut self,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply> {
        let leader_term = args.term;
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.append_entries_handler(args)
        };

        // We consider heartbeat as received only if Raft instance can respond with success.
        // If so, we can reset the election timeout.
        if reply.success {
            {
                let mut this = self.inner.lock().unwrap();
                trace!(
                    "{:?}: AppendEntries/heartbeat successful, updating term to {}",
                    *this,
                    leader_term
                );
            }
            self.heartbeat_sender
                .send(leader_term)
                .await
                .expect("Reset election timeout");
        } else {
            trace!(
                "{:?}: AppendEntries/heartbeat failed, my response is {:?}",
                *self.inner.lock().unwrap(),
                reply
            );
        }
        Ok(reply)
    }
}

/// State of a raft peer.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
struct State {
    current_term: usize,
    role: Role,
    voted_for: Option<usize>,
    log: Log,
}

struct Raft {
    peers: Vec<SocketAddr>,
    me: usize,

    // Network endpoint
    ep: Arc<Endpoint>,

    apply_ch: MsgSender,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state: State,

    commit_index: Arc<AtomicUsize>,

    // Match index for each peer
    match_index: Vec<usize>,

    // Index of the last log entry applied to the state machine
    last_applied: usize,

    // Self-reference to use in async tasks
    self_weak_ref: Weak<Mutex<Self>>,
    pending_election: Option<JoinHandle<()>>,
    heartbeat_task: Option<JoinHandle<()>>,
    follower_sync_tasks: Vec<JoinHandle<()>>,
    commit_index_tasks: Vec<JoinHandle<()>>,
}

// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, data: &[u8]) -> Result<Start> {
        if !self.state.is_leader() {
            let leader = (self.me + 1) % self.peers.len();
            return Err(Error::NotLeader(leader));
        }

        let log_index = self.state.log.push(LogEntry {
            term: self.state.current_term as usize,
            data: data.to_vec(),
        });

        self.match_index[self.me] = log_index; // Update match index for self

        Ok(Start {
            index: log_index as usize,
            term: self.state.current_term,
        })
    }

    // Here is an example to apply committed message.
    fn apply(&mut self) {
        while self.commit_index.load(Ordering::SeqCst) > self.last_applied {
            self.last_applied += 1;

            let msg = ApplyMsg::Command {
                data: self.state.log[self.last_applied].data.clone(),
                index: self.last_applied,
            };
            self.apply_ch.unbounded_send(msg).unwrap();
        }
    }

    fn request_vote_handler(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        // TODO if also candidate then don't vote (???)

        if self.state.current_term > args.term {
            let reply = RequestVoteReply {
                term: self.state.current_term,
                vote_granted: false,
            };
            trace!(
                "{self:?}: sending response: {reply:?} (current term: {}, arg term: {})",
                self.state.current_term,
                args.term
            );
            return reply;
        }

        // If we are a candidate, we can stop the election, because other candidate has higher term
        if args.term > self.state.current_term {
            trace!(
                "{self:?}: received {:?} with higher ter, switching to follower",
                args
            );
            self.state.role = Role::Follower;
            self.state.current_term = args.term;
            self.state.voted_for = None;
            self.pending_election.take().map(|e| e.abort());
            self.heartbeat_task.take().map(|h| h.abort());
        }

        let last_log_index = self.state.log.len();
        let last_log_term = self.state.log.last_log_term();

        if (self.state.voted_for.is_none() || self.state.voted_for == Some(args.candidate_id))
            && last_log_term <= args.last_log_term
            && last_log_index <= args.last_log_index
        {
            self.state.voted_for = Some(args.candidate_id);
            let reply = RequestVoteReply {
                term: self.state.current_term,
                vote_granted: true,
            };
            trace!("{self:?}: sending response: {reply:?} (granted vote)");
            return reply;
        }

        let reply = RequestVoteReply {
            term: self.state.current_term,
            vote_granted: false,
        };
        trace!(
            "{self:?}: sending response: {reply:?}, voted for {:?} != {}",
            self.state.voted_for,
            args.candidate_id
        );
        reply
    }

    fn append_entries_handler(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let resp = if args.term < self.state.current_term {
            trace!(
                "{self:?}: AppendEntries/heartbeat from outdated term {}, current term is {}",
                args.term,
                self.state.current_term
            );
            AppendEntriesReply {
                term: self.state.current_term,
                success: false,
            }
        } else {
            if self.state.log.get(args.prev_log_index).is_some()
                && self.state.log[args.prev_log_index].term != args.prev_log_term
            {
                // If the log entry at prev_log_index does not match the term, we reject the request
                AppendEntriesReply {
                    term: self.state.current_term,
                    success: false,
                }
            } else {
                let new_log_index = args.prev_log_index + 1;
                if self.state.log.get(new_log_index).is_some()
                    && self.state.log[new_log_index].term != args.prev_log_term
                {
                    // If logs are conflicting, replace own log with leader's log
                    self.state.log.clear_from(new_log_index);
                }

                args.entries.iter().for_each(|entry| {
                    self.state.log.push(LogEntry {
                        term: args.term,
                        data: entry.data.clone(),
                    });
                });

                // Update own commit index
                if args.leader_commit > self.commit_index.load(Ordering::SeqCst) {
                    let new_commit_index = min(args.leader_commit, self.state.log.len());
                    self.commit_index.store(new_commit_index, Ordering::SeqCst);
                }

                // If the log entry matches, we accept the request
                AppendEntriesReply {
                    term: self.state.current_term,
                    success: true,
                }
            }
        };

        // Apply to state machine if new logs are committed
        self.apply();

        resp
    }

    // Here is an example to generate random number.
    fn generate_election_timeout() -> Duration {
        // see rand crate for more details
        Duration::from_millis(rand::thread_rng().gen_range(150..300))
    }

    fn perform_election(&mut self) {
        let last_log_index = self.state.log.len();
        let last_log_term = self.state.log.last_log_term();
        let args = RequestVoteArgs {
            term: self.state.current_term,
            candidate_id: self.me,
            last_log_index,
            last_log_term,
        };
        trace!("{self:?}: starting election, args: {args:?}");
        let endpoint = self.ep.clone();

        let mut rpcs = FuturesUnordered::new();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            let args = args.clone();
            let ep = endpoint.clone();
            trace!("{self:?}: sending vote request to {i}");
            rpcs.push(async move { ep.call(peer, args).await });
        }

        let timeout = Self::generate_election_timeout();
        let me = self.me;
        let quorum = (self.peers.len() + 1) / 2;
        let current_term = self.state.current_term;

        enum VotingResult {
            Outdated { peer_term: usize },
            Won,
            NoQuorum,
        }

        let mut election = Box::pin(
            async move {
                // Vote for self
                let mut vote_cnt = 1;
                let mut result = VotingResult::NoQuorum;

                while let Some(resp) = rpcs.next().await {
                    match resp {
                        Err(e) => {
                            warn!("Raft({me}): RPC error: {:?}", e);
                        }
                        Ok(reply) => {
                            trace!("Raft({me},t={current_term}): voting RPC reply: {reply:?}");
                            if reply.term > current_term {
                                info!("Raft({me}): peer term ({}) is higher than current term ({current_term})", reply.term);
                                result = VotingResult::Outdated { peer_term: reply.term };
                                break;
                            }
                            if reply.vote_granted {
                                vote_cnt += 1;
                            }

                            if vote_cnt >= quorum {
                                info!("Raft({me}): received enough votes ({vote_cnt})");
                                result = VotingResult::Won;
                                break;
                            }
                        }
                    };
                }

                result
            }
            .fuse(),
        );

        let raft_ref = self.self_weak_ref.clone();

        self.pending_election = Some(madsim::task::spawn(async move {
            futures::select_biased! {
                _ = time::sleep(timeout).fuse() => {
                    warn!("Raft({me}): election timed out ({timeout:?}), waiting for another one");
                },
                result = election => {
                    let raft = match raft_ref.upgrade() {
                        Some(raft) => raft,
                        None => {
                            warn!("Raft({me}): Election: Raft instance is gone");
                            return;
                        }
                    };
                    let mut raft_guard = raft.lock().unwrap();
                    match result {
                        VotingResult::Outdated { peer_term } => raft_guard.change_state(Role::Follower, peer_term),
                        VotingResult::Won => {
                            raft_guard.change_state(Role::Leader, current_term);
                            raft_guard.start_heartbeat_task();
                            raft_guard.start_follower_sync_tasks();
                        },
                        VotingResult::NoQuorum => info!("Raft({me}): no quorum, staying a candidate")
                    }
                }
            }
        }));
    }

    fn change_state(&mut self, role: Role, term: usize) {
        info!("Raft({}): changing state to {role:?}, term {term}", self.me);
        self.state.role = role;
        self.state.current_term = term;
        self.state.voted_for = None;
        self.pending_election.take().map(|e| e.abort());
        self.heartbeat_task.take().map(|h| h.abort());
        self.follower_sync_tasks.iter_mut().for_each(|f| f.abort());
        self.follower_sync_tasks.clear();
        self.commit_index_tasks.iter_mut().for_each(|c| c.abort());
        self.commit_index_tasks.clear();
    }

    fn start_heartbeat_task(&mut self) {
        assert!(self.state.is_leader());

        let me = self.me;
        let raft_ref = self.self_weak_ref.clone();

        self.heartbeat_task = Some(madsim::task::spawn(async move {
            trace!("Raft({me}): starting heartbeat task");
            loop {
                let (args, endpoint, peers) = {
                    let raft = match raft_ref.upgrade() {
                        Some(raft) => raft,
                        None => {
                            warn!("Raft({me}): Heartbeat: Raft instance is gone");
                            return;
                        }
                    };
                    let mut raft_guard = raft.lock().unwrap();
                    (
                        AppendEntriesArgs {
                            term: raft_guard.state.current_term,
                            leader_id: raft_guard.me as usize,
                            prev_log_index: raft_guard.state.log.len(),
                            prev_log_term: raft_guard.state.log.last_log_term(),
                            entries: vec![],
                            leader_commit: raft_guard.commit_index.load(Ordering::SeqCst),
                        },
                        raft_guard.ep.clone(),
                        raft_guard.peers.clone(),
                    )
                };

                for (i, &peer) in peers.iter().enumerate() {
                    if i == me {
                        continue;
                    }
                    let ep = endpoint.clone();
                    let args = args.clone();
                    let raft_ref_clone = raft_ref.clone();
                    madsim::task::spawn(async move {
                        trace!("Raft({me}): sending heartbeat to peer {}", peer);
                        let resp = ep.call(peer, args).await;
                        match resp {
                            Ok(resp) => {
                                let raft = match raft_ref_clone.upgrade() {
                                    Some(raft) => raft,
                                    None => {
                                        warn!("Raft({me}): Heartbeat: Raft instance is gone");
                                        return;
                                    }
                                };
                                let mut raft_guard = raft.lock().unwrap();
                                if resp.term < raft_guard.state.current_term {
                                    raft_guard.change_state(Role::Follower, resp.term);
                                }
                            }
                            Err(err) => {
                                warn!(
                                    "Raft({me}): Heartbeat: error sending to peer {peer}: {:?}",
                                    err
                                );
                            }
                        }
                    });
                }

                time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }

    fn start_follower_sync_tasks(&mut self) {
        assert!(self.state.is_leader());

        let me = self.me;
        let self_weak_ref = self.self_weak_ref.clone();
        let peers = self.peers.clone();
        let commit_index = self.commit_index.clone();
        let current_term = self.state.current_term;

        // Create a Vec to store the follower sync task handles
        let mut follower_sync_tasks = Vec::new();
        let mut commit_index_tasks = Vec::new();

        for (i, &peer) in peers.iter().enumerate() {
            if i == me {
                continue;
            }

            let (sync_sender, mut sync_receiver) = mpsc::unbounded::<FollowerSyncMsg>();
            let self_weak_ref_clone = self_weak_ref.clone();
            let leader_commit_index = commit_index.clone();

            // Create a transport for this peer
            let transport = Arc::new(AsyncMutex::new(transport::MadsimTransport::new(
                self.ep.clone(),
            )));

            // Create a match index tracker for this peer
            let match_index = Arc::new(AtomicUsize::new(0));

            // Get log reference
            let log = Arc::new(AsyncMutex::new(Log::new())); // TODO: Replace with actual log reference

            // Start with next_index just after the last entry in the log
            let next_index = self.state.log.len() + 1;

            // Create a follower sync instance
            let mut follower_sync = FollowerSync::new(
                leader_commit_index,
                transport.clone(),
                log.clone(),
                next_index,
                match_index.clone(),
                me as usize,
                current_term,
                sync_sender,
            );

            // Spawn a task to run the sync loop
            let sync_task = madsim::task::spawn(async move {
                follower_sync.sync_loop(peer, i).await;
            });

            // Store the task handles
            follower_sync_tasks.push(sync_task);

            // Create a commit index update task
            let commit_index_task = madsim::task::spawn(async move {
                while let Some(msg) = sync_receiver.next().await {
                    match msg {
                        FollowerSyncMsg::OutdatedTerm { peer_term, peer } => {
                            if let Some(raft) = self_weak_ref_clone.upgrade() {
                                let mut raft_guard = raft.lock().unwrap();
                                if peer_term < raft_guard.state.current_term {
                                    warn!(
                                        "Raft({}): Outdated term from peer {}, current term: {}, peer term: {}",
                                        me, peer, raft_guard.state.current_term, peer_term
                                    );
                                    raft_guard.change_state(Role::Follower, peer_term);
                                }
                            }
                        }
                        FollowerSyncMsg::UpdateCommitIndex { index, peer } => {
                            if let Some(raft) = self_weak_ref_clone.upgrade() {
                                let mut raft_guard = raft.lock().unwrap();
                                debug!(
                                    "Raft({}): Updating commit index to {} from peer {}",
                                    me, index, peer
                                );
                                raft_guard.match_index[peer] = index;
                                raft_guard.update_commit_index();
                            }
                        }
                    }
                }
            });

            commit_index_tasks.push(commit_index_task);
        }

        self.follower_sync_tasks = follower_sync_tasks;
        self.commit_index_tasks = commit_index_tasks;
    }

    fn update_commit_index(&mut self) {
        // Check if there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] ≥ N, and log[N].term == currentTerm

        let current_commit_index = self.commit_index.load(Ordering::SeqCst);

        // Sort the match indices in descending order
        let mut match_indices = self.match_index.clone();
        match_indices.sort_unstable();

        // Find the log index that a majority of servers have replicated (median of match indices)
        let majority = (self.peers.len() + 1) / 2;
        let majority_match_index = match_indices[majority - 1];

        // Only update if the majority match index is greater than our current commit index
        if majority_match_index > current_commit_index {
            // Only commit entries from the current term
            let term_at_index = self
                .state
                .log
                .get(majority_match_index)
                .map(|entry| entry.term)
                .unwrap_or(0);

            if term_at_index == self.state.current_term as usize {
                debug!(
                    "Raft({}): Updating commit index from {} to {}",
                    self.me, current_commit_index, majority_match_index
                );
                self.commit_index
                    .store(majority_match_index, Ordering::SeqCst);

                self.apply();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("RequestVoteReply")]
struct RequestVoteArgs {
    term: usize,
    candidate_id: usize,
    last_log_index: usize,
    last_log_term: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteReply {
    term: usize,
    vote_granted: bool,
}

enum RequestVoteResult {
    Timeout,
    AllResponded,
    Error,
    PeerReply(RequestVoteReply),
}

#[derive(Debug, Clone, Serialize, Deserialize, Request, PartialEq)]
#[rtype("AppendEntriesReply")]
pub struct AppendEntriesArgs {
    pub term: usize,
    pub leader_id: usize,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub entries: Vec<LogEntry>,
    pub leader_commit: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub term: usize,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: usize,
    pub success: bool,
}

#[derive(Debug, PartialEq)]
enum FollowerSyncMsg {
    OutdatedTerm { peer_term: usize, peer: usize },
    UpdateCommitIndex { index: usize, peer: usize },
}

pub struct FollowerSync<T: Transport> {
    leader_commit_index: Arc<AtomicUsize>,
    next_index: usize,
    match_index: Arc<AtomicUsize>,
    transport: Arc<AsyncMutex<T>>,
    log: Arc<AsyncMutex<Log>>,
    leader_id: usize,
    leader_term: usize,
    sync_sender: mpsc::UnboundedSender<FollowerSyncMsg>,
}

impl<T: Transport> FollowerSync<T> {
    pub fn new(
        leader_commit_index: Arc<AtomicUsize>,
        transport: Arc<AsyncMutex<T>>,
        log: Arc<AsyncMutex<Log>>,
        next_index: usize,
        match_index: Arc<AtomicUsize>,
        leader_id: usize,
        leader_term: usize,
        sync_sender: mpsc::UnboundedSender<FollowerSyncMsg>,
    ) -> Self {
        Self {
            leader_commit_index,
            transport,
            log,
            next_index,
            match_index,
            leader_id,
            leader_term,
            sync_sender,
        }
    }

    pub async fn sync_loop(&mut self, peer_addr: SocketAddr, peer_number: usize) {
        loop {
            // debug!("FollowerSync loop");
            let log = self.log.lock().await;
            let entries = log[self.next_index..].to_vec();
            let entries_len = entries.len();
            let new_match_index = self.match_index.load(Ordering::Relaxed) + entries_len;
            let prev_log_term = log.prev_log(self.next_index).map(|l| l.term).unwrap_or(0);
            drop(log);

            trace!(
                "FollowerSync: next_index={}, entries_len={}, new_match_index={}, prev_log_term={}",
                self.next_index,
                entries_len,
                new_match_index,
                prev_log_term
            );

            if entries_len > 0 {
                debug!("Send {} entries to peer {}", entries.len(), peer_addr);

                let args = AppendEntriesArgs {
                    term: self.leader_term,
                    leader_id: self.leader_id,
                    prev_log_index: self.next_index - 1,
                    prev_log_term,
                    entries,
                    leader_commit: self.leader_commit_index.load(Ordering::SeqCst),
                };

                let reply = {
                    let mut transport = self.transport.lock().await;
                    transport
                        .call_timeout(peer_addr, args, Duration::from_secs(5))
                        .await
                };

                debug!("Received reply from peer {}: {:?}", peer_addr, reply);

                match reply {
                    Ok(reply) => {
                        if reply.term > self.leader_term {
                            debug!("Peer {} has higher term, aborting sync", peer_addr);
                            self.sync_sender
                                .send(FollowerSyncMsg::OutdatedTerm {
                                    peer_term: self.leader_term,
                                    peer: reply.term,
                                })
                                .await
                                .expect("Failed to send outdated term message");
                            break;
                        }

                        if reply.success {
                            debug!("Peer {} accepted {} entries", peer_addr, entries_len);
                            self.match_index.store(new_match_index, Ordering::Relaxed);
                            self.next_index = new_match_index + 1;
                            self.sync_sender
                                .send(FollowerSyncMsg::UpdateCommitIndex {
                                    index: new_match_index,
                                    peer: peer_number,
                                })
                                .await
                                .expect("Failed to send update commit index message");
                            debug!("Sent update commit index message to sync channel");
                        } else {
                            debug!(
                                "Peer {} rejected {} entries, retrying from earlier entry",
                                peer_addr, entries_len
                            );
                            // TODO backoff
                            time::sleep(Duration::from_millis(5)).await;
                            self.next_index -= 1;
                            continue;
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to send AppendEntries to peer {}: {:?}, retrying",
                            peer_addr, e
                        );
                        // TODO backoff
                        time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };
            }

            // Give the peer some time to process the entries
            time::sleep(Raft::generate_election_timeout()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        net::SocketAddr,
        sync::{atomic::AtomicUsize, Arc},
        time::Duration,
        vec,
    };

    use futures::StreamExt;
    use futures::{channel::mpsc, lock::Mutex as AsyncMutex};
    use madsim::time;
    use tracing::debug;
    use tracing_subscriber::field::debug;

    use crate::raft::raft::{logs::Log, transport};

    use super::{
        transport::testing::MockTransport, AppendEntriesArgs, AppendEntriesReply, FollowerSync,
        FollowerSyncMsg, LogEntry,
    };

    fn init_logger() {
        if std::env::var("TRACING").is_ok() {
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_file(true)
                .with_line_number(true)
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        }
    }

    async fn before(
        entries_in_log: usize,
    ) -> (
        FollowerSync<MockTransport>,
        Arc<AsyncMutex<MockTransport>>,
        Arc<AsyncMutex<Log>>,
        Arc<AtomicUsize>,
        mpsc::UnboundedReceiver<FollowerSyncMsg>,
    ) {
        let mut log_inner = Log::new();
        for _ in 0..entries_in_log {
            log_inner.push(LogEntry {
                term: 1,
                data: vec![7, 8, 9],
            });
        }

        let transport = Arc::new(AsyncMutex::new(MockTransport::new()));
        let mut log = Arc::new(AsyncMutex::new(log_inner));
        let commit_index = Arc::new(AtomicUsize::new(0));
        let next_index = log.lock().await.len();
        let match_index = Arc::new(AtomicUsize::new(0));
        let leader_id = 0;
        let leader_term = 1;
        let (sync_sender, mut sync_receiver) = mpsc::unbounded();
        let mut sync = FollowerSync::new(
            commit_index,
            transport.clone(),
            log.clone(),
            next_index,
            match_index.clone(),
            leader_id,
            leader_term,
            sync_sender,
        );

        (sync, transport, log, match_index, sync_receiver)
    }

    #[madsim::test]
    async fn follower_sync_happy_path() {
        // init_logger();

        let (mut sync, transport, mut log, match_index, mut sync_receiver) = before(1).await;

        madsim::task::spawn(async move {
            sync.sync_loop(SocketAddr::from(([10, 0, 0, 200], 1)), 0)
                .await;
        });

        let transport_guard = transport.lock().await;
        transport_guard
            .respond::<AppendEntriesArgs>(AppendEntriesReply {
                term: 1,
                success: true,
            })
            .await;
        drop(transport_guard);

        let update_msg = sync_receiver.next().await.unwrap();
        assert_eq!(
            super::FollowerSyncMsg::UpdateCommitIndex { index: 1, peer: 0 },
            update_msg
        );
        assert_eq!(1, match_index.load(std::sync::atomic::Ordering::SeqCst));

        let mut log_guard = log.lock().await;
        log_guard.push(LogEntry {
            term: 1,
            data: vec![4, 5, 6],
        });
        drop(log_guard);

        let transport_guard = transport.lock().await;
        transport_guard
            .respond::<AppendEntriesArgs>(AppendEntriesReply {
                term: 1,
                success: true,
            })
            .await;
        drop(transport_guard);

        let update_msg = sync_receiver.next().await.unwrap();
        assert_eq!(
            super::FollowerSyncMsg::UpdateCommitIndex { index: 2, peer: 0 },
            update_msg
        );
        assert_eq!(2, match_index.load(std::sync::atomic::Ordering::SeqCst));

        let mut log_guard = log.lock().await;
        for _ in 0..1000 {
            log_guard.push(LogEntry {
                term: 1,
                data: vec![7, 8, 9],
            });
        }
        drop(log_guard);

        let transport_guard = transport.lock().await;
        transport_guard
            .respond::<AppendEntriesArgs>(AppendEntriesReply {
                term: 1,
                success: true,
            })
            .await;
        drop(transport_guard);

        let update_msg = sync_receiver.next().await.unwrap();
        assert_eq!(
            super::FollowerSyncMsg::UpdateCommitIndex {
                index: 1002,
                peer: 0
            },
            update_msg
        );
        assert_eq!(1002, match_index.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[madsim::test]
    async fn follower_sync_step_down_if_follower_has_higher_term() {
        // init_logger();

        let (mut sync, transport, mut log, match_index, mut sync_receiver) = before(1).await;

        madsim::task::spawn(async move {
            sync.sync_loop(SocketAddr::from(([10, 0, 0, 200], 1)), 0)
                .await;
        });

        let transport_guard = transport.lock().await;
        transport_guard
            .respond::<AppendEntriesArgs>(AppendEntriesReply {
                term: 2,
                success: false,
            })
            .await;
        drop(transport_guard);

        let update_msg = sync_receiver.next().await.unwrap();
        assert_eq!(
            super::FollowerSyncMsg::OutdatedTerm {
                peer_term: 1,
                peer: 2
            },
            update_msg
        );
        assert_eq!(0, match_index.load(std::sync::atomic::Ordering::SeqCst));
    }
}
