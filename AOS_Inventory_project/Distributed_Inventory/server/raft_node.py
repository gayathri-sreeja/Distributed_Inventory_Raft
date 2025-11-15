# server/raft_node.py
import json
import os
import random
import threading
import time
import grpc
import base64
import tempfile

from proto import raft_pb2, raft_pb2_grpc


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    """
    Minimal Raft for Milestone 2:
      - Leader election
      - Heartbeats
      - Log replication (majority commit)
      - Safe persistence (JSON with base64 payloads)
    """

    def __init__(self, node_id, peers, persist_path, apply_callback):
        self.node_id = node_id
        # peers: dict {peer_id: "host:port"} where port is raft gRPC port for that peer
        self.peers = peers or {}
        self.persist_path = persist_path
        self.apply_callback = apply_callback

        # Persistent state
        self.current_term = 0
        self.voted_for = None
        # Log stores dicts: {term, command_type, command_b64}
        self.log = []

        # Volatile state
        self.state = "Follower"  # Follower | Candidate | Leader
        self.commit_index = 0
        self.last_applied = 0
        self.leader_id = None

        # NOTE: add leader_address so other services can forward requests
        # this was missing earlier and caused "'RaftNode' object has no attribute 'leader_address'"
        # leader_address should be a "host:port" string for the leader (or None)
        self.leader_address = None

        # Leader state
        self.next_index = {}
        self.match_index = {}

        # Timers
        self.last_heard = time.time()
        self.election_timeout = self._new_timeout()

        # Load persisted state if present
        self._load_persist()

        # Background loop (single thread)
        threading.Thread(target=self._run, daemon=True).start()

    # ---------------- Persistence (safe) ----------------
    def _load_persist(self):
        if not self.persist_path:
            return
        if not os.path.exists(self.persist_path):
            return
        try:
            with open(self.persist_path, "r") as f:
                data = json.load(f)
            self.current_term = data.get("term", 0)
            self.voted_for = data.get("voted_for", None)
            self.log = data.get("log", [])
            # Convert legacy keys if any
            for e in self.log:
                if "command_b64" not in e and "command_bytes" in e:
                    e["command_b64"] = base64.b64encode(
                        e["command_bytes"] if isinstance(e["command_bytes"], bytes)
                        else str(e["command_bytes"]).encode()
                    ).decode()
                    e.pop("command_bytes", None)
        except Exception as e:
            #print(f"[RAFT] Persist file corrupt at {self.persist_path}, resetting. ({e})")
            self.current_term = 0
            self.voted_for = None
            self.log = []
            self._save_persist()

    def _save_persist(self):
        if not self.persist_path:
            return
        os.makedirs(os.path.dirname(self.persist_path) or ".", exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(prefix="raft_", suffix=".json",
                                            dir=os.path.dirname(self.persist_path) or ".")
        try:
            with os.fdopen(tmp_fd, "w") as f:
                json.dump({
                    "term": self.current_term,
                    "voted_for": self.voted_for,
                    "log": self.log
                }, f)
            os.replace(tmp_path, self.persist_path)
        except Exception as e:
            print(f"[RAFT] Persist save failed: {e}")
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    # ---------------- Helpers ----------------
    def _new_timeout(self):
        # Election timeout randomized between 1.0 and 2.0 seconds (suitable for small test clusters)
        return random.uniform(1.0, 2.0)

    @staticmethod
    def _decode_entry_for_apply(entry_dict):
        """
        Convert stored log entry (with command_b64) into a dict with raw bytes for callback.
        """
        out = {
            "term": entry_dict["term"],
            "command_type": entry_dict["command_type"],
            "command_bytes": base64.b64decode(entry_dict["command_b64"])
        }
        return out

    # ---------------- RequestVote ----------------
    def RequestVote(self, request, context):
        # If candidate's term is less than current, reject
        if request.term < self.current_term:
            return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False)

        # If candidate has higher term, step down and update term
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = "Follower"
            self._save_persist()

        # Check if we can vote for this candidate (not voted or voted for them)
        can_vote = (self.voted_for is None or self.voted_for == request.candidate_id)

        # Up-to-date log check:
        # RPC uses last_log_index (1-based index of last log entry) and last_log_term
        my_last_index = len(self.log)  # number of entries (this maps to 1-based last index)
        my_last_term = self.log[-1]["term"] if self.log else 0

        # Candidate's log is at least as up-to-date if:
        # candidate_last_term > my_last_term OR (equal and candidate_last_index >= my_last_index)
        candidate_up_to_date = (
            request.last_log_term > my_last_term or
            (request.last_log_term == my_last_term and request.last_log_index >= my_last_index)
        )

        if can_vote and candidate_up_to_date:
            self.voted_for = request.candidate_id
            # reset timer because we granted a vote (we heard from candidate)
            self.last_heard = time.time()
            self.election_timeout = self._new_timeout()
            self._save_persist()
            return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=True)

        return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False)

    # ---------------- AppendEntries ----------------
    def AppendEntries(self, request, context):
        """
        Handles heartbeats and log replication.
        RPC fields semantics assumed:
         - prev_log_index: index of log entry immediately preceding new entries (1-based, 0 if none)
         - prev_log_term: term of prev_log_index (0 if none)
         - entries: repeated LogEntry (index, term, command_type, command_bytes)
         - leader_commit: leader's commit index (1-based number of entries committed)
        Local storage:
         - self.log is python list; log index i (1-based) maps to self.log[i-1]
        """

        # If RPC term < current_term, reject
        if request.term < self.current_term:
            return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

        # If RPC term > current_term, adopt it
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = "Follower"
            self._save_persist()

        # Accept as follower and reset election timeout
        self.state = "Follower"
        self.leader_id = request.leader_id
        # set leader_address so others can forward to leader (peers maps id->host:port)
        # if leader_id is one of peers keys, peers[leader_id] is the "host:port"
        self.leader_address = self.peers.get(request.leader_id)
        self.last_heard = time.time()
        self.election_timeout = self._new_timeout()

        # Prev log consistency: prev_log_index is 1-based; 0 means no prev
        prev_idx = request.prev_log_index
        prev_term = request.prev_log_term

        if prev_idx > 0:
            # we must have at least prev_idx entries
            if len(self.log) < prev_idx:
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)
            # term must match
            local_prev_term = self.log[prev_idx - 1]["term"]
            if local_prev_term != prev_term:
                # conflict: delete from prev_idx onwards (keep entries before prev_idx)
                # Since prev_idx points to the conflicting entry, truncate up to prev_idx-1
                self.log = self.log[:prev_idx - 1]
                self._save_persist()
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

        # Append any new entries (overwrite conflicting ones)
        if len(request.entries) > 0:
            # Truncate any entries after prev_idx (prev_idx may be 0)
            # Keep entries up to prev_idx (1-based) => slice up to prev_idx
            if prev_idx >= 0 and len(self.log) > prev_idx:
                self.log = self.log[:prev_idx]
            # Append incoming entries
            for e in request.entries:
                # Use the entry's term and command bytes; store b64 for persistence
                self.log.append({
                    "term": e.term,
                    "command_type": e.command_type,
                    "command_b64": base64.b64encode(e.command_bytes).decode()
                })
            self._save_persist()

        # Update commit index and apply entries up to leader_commit
        # leader_commit is 1-based count of entries leader says are committed
        if request.leader_commit > self.commit_index:
            # commit_index must not exceed last log index
            self.commit_index = min(request.leader_commit, len(self.log))
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                raw_entry = self.log[self.last_applied - 1]
                if self.apply_callback:
                    try:
                        self.apply_callback(self._decode_entry_for_apply(raw_entry))
                    except Exception as e:
                        print(f"[RAFT] apply callback error: {e}")

        return raft_pb2.AppendEntriesResponse(
            term=self.current_term, success=True, match_index=len(self.log)
        )

    # ---------------- Node loop ----------------
    def _run(self):
        """
        Main background loop:
         - if leader: send periodic heartbeats
         - if follower/candidate: check election timeout and start election
        """
        while True:
            # leader heartbeats every 0.1s; followers check timeout often enough
            time.sleep(0.1)
            if self.state == "Leader":
                # send heartbeat asynchronously
                try:
                    self._send_heartbeats()
                except Exception:
                    pass
                continue

            # follower/candidate: if haven't heard from leader within election timeout → start election
            if time.time() - self.last_heard > self.election_timeout:
                self._start_election()

    # ---------------- Election ----------------
    def _start_election(self):
        """
        Start new election: become candidate, increment term, request votes.
        Uses short RPC timeouts and threads to contact peers concurrently.
        """
        # Become candidate
        self.state = "Candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        # reset timer to avoid immediate re-election
        self.last_heard = time.time()
        self.election_timeout = self._new_timeout()
        self._save_persist()

        votes = 1  # voted for self
        needed = (len(self.peers) + 1) // 2 + 1

       # print(f"[RAFT] {self.node_id} starting election term={self.current_term} needed={needed}")

        votes_lock = threading.Lock()

        def ask(peer_id, addr):
            nonlocal votes
            try:
                channel = grpc.insecure_channel(addr)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                # last_log_index is 1-based length of log
                last_log_index = len(self.log)
                last_log_term = self.log[-1]["term"] if self.log else 0
                resp = stub.RequestVote(
                    raft_pb2.RequestVoteRequest(
                        term=self.current_term,
                        candidate_id=self.node_id,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term
                    ),
                    timeout=1.0
                )
                if resp.vote_granted:
                    with votes_lock:
                        votes += 1
            except Exception as e:
                # debug network issues
                print(f".")#[RAFT] RequestVote to {peer_id} ({addr}) failed: {e}")

        threads = []
        for pid, addr in self.peers.items():
            t = threading.Thread(target=ask, args=(pid, addr), daemon=True)
            t.start()
            threads.append(t)
        # join with a reasonable timeout so slow peers don't block election
        for t in threads:
            t.join(timeout=1.2)

        print(f".")#[RAFT] {self.node_id} election term={self.current_term} votes={votes}/{needed}")

        if votes >= needed:
            self._become_leader()
        else:
            # didn't get majority → remain follower/candidate (reset timers)
            self.state = "Follower"
            self.last_heard = time.time()
            self.election_timeout = self._new_timeout()

    def _become_leader(self):
        self.state = "Leader"
        self.leader_id = self.node_id
        # leader is self; there's no peer address for self in peers dict, so clear leader_address
        self.leader_address = None
        # next_index should be last_log_index + 1 (1-based)
        last_index_plus_one = len(self.log) + 1
        self.next_index = {p: last_index_plus_one for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # mark heard now so followers won't immediately start election
        self.last_heard = time.time()
        print(f"Leader elected: {self.node_id} (term={self.current_term})")
        # send initial heartbeat to assert leadership
        try:
            self._send_heartbeats()
        except Exception as e:
            print(f".")#[RAFT] initial heartbeat error: {e}")

    # ---------------- Heartbeats ----------------
    def _send_heartbeats(self):
        """
        Send AppendEntries RPC as heartbeats (no entries) to all peers.
        prev_log_index is current last log index (1-based), prev_log_term accordingly.
        Use a short RPC timeout so leader doesn't block when a peer is down.
        """
        prev_idx = len(self.log)  # 1-based last index (0 if empty)
        prev_term = self.log[-1]["term"] if self.log else 0

        for pid, addr in self.peers.items():
            try:
                channel = grpc.insecure_channel(addr)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                # Send empty entries as heartbeat; leader_commit is current commit_index
                stub.AppendEntries(
                    raft_pb2.AppendEntriesRequest(
                        term=self.current_term,
                        leader_id=self.node_id,
                        prev_log_index=prev_idx,
                        prev_log_term=prev_term,
                        entries=[],  # heartbeat
                        leader_commit=self.commit_index
                    ),
                    timeout=1.0
                )
            except Exception as e:
                # non-fatal: peer might be down or unreachable
                print(f".")#[RAFT] AppendEntries to {pid} ({addr}) failed: {e}")

    # ---------------- Propose (client write) ----------------
    def propose(self, command_type, command_bytes):
        """
        Leader-side: append command to local log, replicate to followers, and commit on majority.
        Returns True if commit succeeded, False otherwise (or not leader).
        """
        if self.state != "Leader":
            return False

        # Append locally
        new_index = len(self.log) + 1  # new entry will have 1-based index = new_index
        entry_stored = {
            "term": self.current_term,
            "command_type": command_type,
            "command_b64": base64.b64encode(command_bytes).decode()
        }
        self.log.append(entry_stored)
        self._save_persist()

        # Replicate to followers
        acks = 1
        needed = (len(self.peers) + 1) // 2 + 1

        # build the LogEntry proto once
        log_entry_proto = raft_pb2.LogEntry(
            index=new_index,
            term=self.current_term,
            command_type=command_type,
            command_bytes=command_bytes
        )

        # For each peer, send AppendEntries with the single new entry
        for pid, addr in self.peers.items():
            try:
                channel = grpc.insecure_channel(addr)
                stub = raft_pb2_grpc.RaftServiceStub(channel)

                prev_index = new_index - 1  # prev entry index (0 if this is first)
                prev_term = self.log[prev_index - 1]["term"] if prev_index > 0 else 0

                resp = stub.AppendEntries(
                    raft_pb2.AppendEntriesRequest(
                        term=self.current_term,
                        leader_id=self.node_id,
                        prev_log_index=prev_index,
                        prev_log_term=prev_term,
                        entries=[log_entry_proto],
                        leader_commit=self.commit_index
                    ),
                    timeout=1.5
                )

                if resp.success:
                    acks += 1
                    # optimistic update of match_index/next_index
                    self.match_index[pid] = new_index
                    self.next_index[pid] = new_index + 1
                else:
                    # follower rejected due to log inconsistency; back off next_index
                    # decrement next_index for that follower (simple strategy)
                    ni = self.next_index.get(pid, new_index)
                    self.next_index[pid] = max(1, ni - 1)
            except Exception as e:
                print(f".")#[RAFT] AppendEntries to {pid} failed: {e}")

        # If majority acknowledged, commit and apply locally
        if acks >= needed:
            self.commit_index = new_index
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                raw_entry = self.log[self.last_applied - 1]
                if self.apply_callback:
                    try:
                        self.apply_callback(self._decode_entry_for_apply(raw_entry))
                    except Exception as e:
                        print(f"[RAFT] apply callback error during propose: {e}")
            # propagate updated commit_index via heartbeats
            try:
                self._send_heartbeats()
            except Exception:
                pass
            return True

        # Not enough acks: leave log as is (leader may retry later)
        return False
