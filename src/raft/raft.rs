use crate::raft;

use self::{logs::Logs, msg::*};
use futures::{
    channel::mpsc,
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
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tracing::{debug, info, trace, warn};

mod logs;
mod msg;

#[derive(Clone)]
pub struct RaftHandle {
    inner: Arc<Mutex<Raft>>,
    heartbeat_sender: mpsc::UnboundedSender<()>,
}

type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Debug)]
pub struct Start {
    /// The index that the command will appear at if it's ever committed.
    pub index: u64,
    /// The current term.
    pub term: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("this node is not a leader, next leader: {0}")]
    NotLeader(usize),
    #[error("IO error")]
    IO(#[from] io::Error),
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
    current_term: u64,
    voted_for: Option<usize>,
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // write!(f, "Raft({},t={},l=[{},{}],{:?})", self.me,)
        write!(f, "Raft({})", self.me,)
    }
}

impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let (heartbeat_sender, mut heartbeat_receiver) = mpsc::unbounded();
        trace!("Binding node {} to {}", me + 1, peers[me]);
        let ep = Arc::new(Endpoint::bind(peers[me]).await.expect("failed to bind"));
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            ep: ep.clone(),
            apply_ch,
            state: State::default(),
            pending_election: None,
        }));
        let handle = RaftHandle {
            inner,
            heartbeat_sender,
        };
        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server(ep);

        let raft = handle.inner.clone();
        let heartbeat_timeout = Raft::generate_election_timeout();

        madsim::task::spawn(async move {
            loop {
                let mut sleep = time::sleep(heartbeat_timeout).fuse();
                // If timeout and heartbeat happen simultaneously, prefer timeout
                futures::select_biased! {
                    _ = sleep => {
                        // Log a warning if no heartbeat was received in time.
                        warn!("Raft({me}): No heartbeat received within {:?}", heartbeat_timeout);
                        let mut raft_guard = raft.lock().expect("unlock Raft");
                        raft_guard.state.role = Role::Candidate;
                        raft_guard.state.term += 1;
                        raft_guard.perform_election();
                    },
                    msg = heartbeat_receiver.next() => {
                        match msg {
                            None => {
                                warn!("Raft({me}): Heartbeat channel closed");
                                break;
                            }
                            Some(_) => {
                                debug!("Raft({me}): Heartbeat received, reset election timeout.");
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
    pub fn term(&self) -> u64 {
        let raft = self.inner.lock().unwrap();
        raft.state.term
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
        _last_included_term: u64,
        _last_included_index: u64,
        _snapshot: &[u8],
    ) -> bool {
        todo!()
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub async fn snapshot(&self, index: u64, snapshot: &[u8]) -> Result<()> {
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
        self.heartbeat_sender
            .send(())
            .await
            .expect("Reset election timeout");
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.append_entries_handler(args)
        };
        Ok(reply)
    }
}

struct Raft {
    peers: Vec<SocketAddr>,
    me: usize,

    // network endpoint
    ep: Arc<Endpoint>,

    apply_ch: MsgSender,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state: State,

    pending_election: Option<JoinHandle<()>>,
}

/// State of a raft peer.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct State {
    term: u64,
    role: Role,
    last_log_index: u64,
    last_log_term: u64,
    voted_for: Option<usize>,
}

// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, _data: &[u8]) -> Result<Start> {
        if !self.state.is_leader() {
            let leader = (self.me + 1) % self.peers.len();
            return Err(Error::NotLeader(leader));
        }
        todo!("start agreement");
    }

    // Here is an example to apply committed message.
    fn apply(&self) {
        let msg = ApplyMsg::Command {
            data: todo!("apply msg"),
            index: todo!("apply msg"),
        };
        self.apply_ch.unbounded_send(msg).unwrap();
    }

    fn request_vote_handler(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        // TODO if also candidate then don't vote (???)

        if self.state.term > args.term {
            return RequestVoteReply {
                term: self.state.term,
                vote_granted: false,
            };
        }

        if (self.state.voted_for.is_none() || self.state.voted_for == Some(args.candidate_id))
            && self.state.last_log_term <= args.last_log_term
            && self.state.last_log_index <= args.last_log_index
        {
            self.state.voted_for = Some(args.candidate_id);
            self.state.term = args.term;
            return RequestVoteReply {
                term: self.state.term,
                vote_granted: true,
            };
        }

        RequestVoteReply {
            term: self.state.term,
            vote_granted: false,
        }
    }

    fn append_entries_handler(&mut self, _args: AppendEntriesArgs) -> AppendEntriesReply {
        AppendEntriesReply {
            term: self.state.term,
            success: true,
        }
    }

    // Here is an example to generate random number.
    fn generate_election_timeout() -> Duration {
        // see rand crate for more details
        Duration::from_millis(rand::thread_rng().gen_range(150..300))
    }

    fn perform_election(&mut self) {
        let args = RequestVoteArgs {
            term: self.state.term,
            candidate_id: self.me,
            last_log_index: self.state.last_log_index,
            last_log_term: self.state.last_log_term,
        };
        let endpoint = self.ep.clone();

        let mut rpcs = FuturesUnordered::new();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            let args = args.clone();
            let ep = endpoint.clone();
            rpcs.push(async move { ep.call(peer, args).await });
        }

        let timeout = Self::generate_election_timeout();
        let me = self.me;
        let quorum = (self.peers.len() + 1) / 2;
        let current_term = self.state.term;

        enum VotingResult {
            Outdated { peer_term: u64 },
            Won,
            NoQuorum,
        }

        let mut election = Box::pin(
            async move {
                let mut votes = vec![];
                while let Some(resp) = rpcs.next().await {
                    match resp {
                        Err(e) => {
                            warn!("Raft({me}): RPC error: {:?}", e);
                        }
                        Ok(reply) => {
                            votes.push(reply);
                        }
                    };
                }

                // Vote for self
                let mut vote_cnt = 1;
                let mut result = VotingResult::NoQuorum;

                for vote in &votes {
                    if vote.term > current_term {
                        info!("Raft({me}): peer term ({}) is higher than current term ({current_term})", vote.term);
                        result = VotingResult::Outdated { peer_term: vote.term };
                        break;
                    }
                    if vote.vote_granted {
                        vote_cnt += 1;
                    }

                    if vote_cnt >= quorum {
                        info!("Raft({me}): received enough votes ({vote_cnt})");
                        result = VotingResult::Won;
                        break;
                    }
                }

                result
            }
            .fuse(),
        );

        self.pending_election = Some(madsim::task::spawn(async move {
            futures::select_biased! {
                _ = time::sleep(timeout).fuse() => {
                    warn!("Raft(me): election timed out");
                    todo!("React on election timeout")
                },
                result = election => {
                    match result {
                        VotingResult::Outdated { peer_term } => todo!("React on outdated term"),
                        VotingResult::Won => todo!("React on winning election"),
                        VotingResult::NoQuorum => todo!("React on no quorum"),
                    }
                }
            }
        }));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("RequestVoteReply")]
struct RequestVoteArgs {
    term: u64,
    candidate_id: usize,
    last_log_index: u64,
    last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteReply {
    term: u64,
    vote_granted: bool,
}

enum RequestVoteResult {
    Timeout,
    AllResponded,
    Error,
    PeerReply(RequestVoteReply),
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("AppendEntriesReply")]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<Log>,
    pub leader_commit: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Log {
    pub term: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}
