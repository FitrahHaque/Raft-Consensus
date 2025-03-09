package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

const DEBUG = 0

func (rn *Node) debug(format string, args ...interface{}) {
	if DEBUG > 0 {
		format = fmt.Sprintf("[%d] %s", rn.id, format)
		log.Printf(format, args...)
	}
}

func CreateNode(
	serverId uint64,
	peerList Set,
	server *Server,
	db *Database,
	ready <-chan interface{},
	commitChan chan CommitEntry,
) *Node {
	node := &Node{
		id:                 serverId,
		peerList:           peerList,
		server:             server,
		db:                 db,
		commitChan:         commitChan,
		newCommitReady:     make(chan struct{}, 16),
		trigger:            make(chan struct{}, 1),
		currentTerm:        0,
		votedFor:           -1,
		potentialLeader:    -1,
		log:                make([]LogEntry, 0),
		commitLength:       0,
		lastApplied:        0,
		state:              Follower,
		electionResetEvent: time.Now(),
		nextIndex:          make(map[uint64]uint64),
		matchedIndex:       make(map[uint64]uint64),
	}
	if node.db.HasData() {
		// fmt.Printf("db has data Restoring from storage on node: %d\n", node.id)
		node.restoreFromStorage()
	}

	go func() {
		<-ready
		//RPC call to leader to join cluster assuming leader is automatically known - logic
		// if err := node.joinCluster(); err != nil {
		// 	log.Fatalf("Failed to join cluster: %v\n", err)
		// }
		node.mu.Lock()
		node.electionResetEvent = time.Now()
		node.mu.Unlock()
		// fmt.Printf("Starting election timer on node: %d\n", node.id)
		node.runElectionTimer()
	}()

	go node.sendCommit()

	return node
}

// func (node *Node) joinCluster() error {
// 	var reply JoinClusterReply
// 	args := JoinClusterArgs{ServerId: node.id, ServerAddr: node.server.listener.Addr()}
// 	// leaderAddr := node.peerList.GetRandomPeer()
// 	// if leaderAddr == nil {
// 	// 	return errors.New("no leader found")
// 	// }

// 	if err := node.server.RPC(leaderId, "RaftNode.JoinCluster", args, &reply); err != nil {
// 		return err
// 	}
// 	if !reply.Success {
// 		return fmt.Errorf("join cluster failed: leader %d, term %d", reply.LeaderId, reply.CurrentTerm)
// 	}
// 	return nil
// }

func (node *Node) addPeer(peerId uint64) {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.peerList.Add(peerId)
	node.nextIndex[peerId] = uint64(len(node.log)) + 1
	node.matchedIndex[peerId] = 0
	fmt.Printf("[%d] Added peer %d to cluster\n", node.id, peerId)
}

func (node *Node) removePeer(peerId uint64) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.peerList.Exists(peerId) {
		node.peerList.Remove(peerId)
	}
}

func (node *Node) sendCommit() {
	for range node.newCommitReady {
		node.mu.Lock()
		lastAppliedSaved := node.lastApplied
		currentTermSaved := node.currentTerm
		var pendingCommitEntries []LogEntry
		if node.commitLength > node.lastApplied {
			pendingCommitEntries = node.log[node.lastApplied:node.commitLength]
			node.lastApplied = node.commitLength
		}
		node.mu.Unlock()
		for i, entry := range pendingCommitEntries {
			// fmt.Printf("Commited entry for node Id: %d, entry: %v\n", node.id, entry.Command)
			node.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   lastAppliedSaved + uint64(i) + 1,
				Term:    currentTermSaved,
			}
		}
	}
}

func (node *Node) runElectionTimer() {
	timeoutDuration := node.electionTimeout()
	node.mu.Lock()
	termStarted := node.currentTerm
	node.mu.Unlock()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		node.mu.Lock()
		if node.state != Candidate && node.state != Follower {
			node.mu.Unlock()
			return
		}
		if termStarted != node.currentTerm {
			// fmt.Printf("Term changed from %v to %v for peer %v\n", termStarted, node.currentTerm, node.id)
			node.mu.Unlock()
			return
		}
		if elapsed := time.Since(node.electionResetEvent); elapsed >= timeoutDuration {
			node.startElection()
			node.mu.Unlock()
			return
		}
		node.mu.Unlock()
	}
}

func (node *Node) electionTimeout() time.Duration {
	if os.Getenv("RAFT_FORCE_MORE_REELECTION") == "true" && rand.Intn(3) > 0 {
		return time.Duration(1500) * time.Millisecond
	} else {
		return time.Duration(1500+rand.Intn(1500)) * time.Millisecond
	}
}

func (node *Node) startElection() {
	node.state = Candidate
	node.currentTerm += 1
	fmt.Printf("Calling startElection() by %d for term %d with peers: %v\n", node.id, node.currentTerm, node.peerList)
	candidacyTerm := node.currentTerm
	node.electionResetEvent = time.Now()
	node.votedFor = int64(node.id)
	node.potentialLeader = int64(node.id)
	votesReceived := 1
	go func() {
		node.mu.Lock()
		defer node.mu.Unlock()
		if node.state == Candidate && votesReceived*2 > node.peerList.Size()+1 {
			node.becomeLeader()
		}
	}()

	for peer := range node.peerList.peerSet {
		go func(peer uint64) {
			node.mu.Lock()
			lastLogIndex, lastLogTerm := node.lastLogIndexAndTerm()
			node.mu.Unlock()
			args := RequestVoteArgs{
				Term:         candidacyTerm,
				CandidateId:  node.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			if err := node.server.RPC(peer, "RaftNode.RequestVote", args, &reply); err == nil {
				// fmt.Printf("Got reply back from RequestVote on %d from peer %v -> %v\n", node.id, peer, reply)
				//update nextIndex here?
				node.mu.Lock()
				defer node.mu.Unlock()
				if node.state != Candidate {
					return
				}
				if reply.Term > candidacyTerm {
					node.becomeFollower(reply.Term, -1)
					return
				}
				if reply.Term == candidacyTerm && reply.VoteGranted {
					votesReceived += 1
					if votesReceived*2 > node.peerList.Size()+1 {
						node.becomeLeader()
						return
					}
				}
			}
		}(peer)
	}

	go node.runElectionTimer()
}

func (node *Node) becomeLeader() {
	fmt.Printf("Node %d has become Leader for Term %d\n", node.id, node.currentTerm)
	node.state = Leader
	node.potentialLeader = int64(node.id)
	for peer := range node.peerList.peerSet {
		node.nextIndex[peer] = uint64(len(node.log)) + 1
		node.matchedIndex[peer] = 0
	}
	go func(heartbeatTimeout time.Duration) {
		node.leaderSendAppendEntries()
		timer := time.NewTimer(heartbeatTimeout)
		defer timer.Stop()
		for {
			doSend := false
			select {
			case <-timer.C:
				doSend = true
				timer.Stop()
				timer.Reset(heartbeatTimeout)
			case _, ok := <-node.trigger:
				// fmt.Printf("becomeLeader running on node %d, leader, term = %v, %v\n", node.id, node.state, node.currentTerm)
				if ok {
					doSend = true
				} else {
					return
				}
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(heartbeatTimeout)
			}
			if doSend {
				if node.state != Leader {
					return
				}
				node.leaderSendAppendEntries()
			}
		}
	}(50 * time.Millisecond)
}

func (node *Node) leaderSendAppendEntries() {
	node.mu.Lock()
	leadershipTerm := node.currentTerm
	node.mu.Unlock()

	go func(peer uint64) {
		if node.peerList.Size() == 0 {
			if uint64(len(node.log)) > node.commitLength {
				// fmt.Printf("Before Committing log entries on node %d\n", node.id)
				commitLengthSaved := node.commitLength
				for i := node.commitLength + 1; i <= uint64(len(node.log)); i++ {
					if node.log[i-1].Term == node.currentTerm {
						node.commitLength = i
					}
				}
				if commitLengthSaved != node.commitLength {
					// fmt.Printf("Committing log entries on node %d\n", node.id)
					node.newCommitReady <- struct{}{}
					node.trigger <- struct{}{}
				}
			}
		}
	}(node.id)

	for peer := range node.peerList.peerSet {
		go func(peer uint64) {
			node.mu.Lock()
			nextIndexSaved := node.nextIndex[peer]
			lastLogIndexSaved := int(nextIndexSaved) - 1
			lastLogTermSaved := uint64(0)
			if lastLogIndexSaved > 0 {
				lastLogTermSaved = node.log[lastLogIndexSaved-1].Term
			}
			entries := node.log[nextIndexSaved-1:]
			// fmt.Printf("nextIndexSaved is %d and lastLogIndexSaved is %d for peer %d from node %d on term %d\n", nextIndexSaved, uint64(lastLogIndexSaved), peer, node.id, node.currentTerm)
			// fmt.Printf("[LeaderSendAppendEntries peer %d] entries size: %d\n", peer, len(entries))
			args := AppendEntriesArgs{
				Term:         leadershipTerm,
				LeaderId:     node.id,
				LastLogIndex: uint64(lastLogIndexSaved),
				LastLogTerm:  lastLogTermSaved,
				Entries:      entries,
				LeaderCommit: node.commitLength,
			}
			node.mu.Unlock()
			var reply AppendEntriesReply
			if err := node.server.RPC(peer, "RaftNode.AppendEntries", args, &reply); err == nil {
				node.mu.Lock()
				defer node.mu.Unlock()
				if reply.Term > leadershipTerm {
					node.becomeFollower(reply.Term, -1)
					return
				}
				if node.state == Leader && leadershipTerm == reply.Term {
					if reply.Success {
						node.nextIndex[peer] = nextIndexSaved + uint64(len(entries))
						node.matchedIndex[peer] = node.nextIndex[peer] - 1
						commitLengthSaved := node.commitLength
						for i := node.commitLength + 1; i <= uint64(len(node.log)); i++ {
							if node.log[i-1].Term == node.currentTerm {
								matchCount := 1
								for p := range node.peerList.peerSet {
									if node.matchedIndex[p] >= i {
										matchCount++
									}
								}
								if matchCount*2 > node.peerList.Size()+1 {
									node.commitLength = i
								}
							}
						}
						if commitLengthSaved != node.commitLength {
							// fmt.Printf("[LeaderSendAppendEntries Reply from peer %d] matchIndex[peer], nextIndex[peer], rn.commitIndex = %v, %v, %v\n", peer, node.matchedIndex[peer], node.nextIndex[peer], node.commitLength)
							node.newCommitReady <- struct{}{}
							node.trigger <- struct{}{}
						}
					} else {
						if reply.RecoveryTerm == 0 {
							node.nextIndex[peer] = reply.RecoveryIndex
						} else {
							lastLogIndex := uint64(0)
							for i := uint64(len(node.log)); i > 0; i-- {
								if node.log[i-1].Term == reply.RecoveryTerm {
									lastLogIndex = i
									break
								}
							}
							if lastLogIndex == 0 {
								node.nextIndex[peer] = reply.RecoveryIndex
							} else {
								node.nextIndex[peer] = lastLogIndex + 1
							}
						}
					}
				}
			}
		}(peer)
	}

}

func (node *Node) lastLogIndexAndTerm() (uint64, uint64) {
	if len(node.log) > 0 {
		lastIndex := uint64(len(node.log) - 1)
		return lastIndex, node.log[lastIndex].Term
	}
	return 0, 0
}

func (node *Node) joinAsPeer(leaderId uint64, term uint64, peerSet map[uint64]struct{}) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.peerList.Add(leaderId)
	node.becomeFollower(term, int64(leaderId))
	for peerId := range peerSet {
		if peerId != node.id {
			fmt.Printf("[%d] Connected to peer %d\n", node.id, peerId)
			node.peerList.Add(peerId)
		}
	}
}

func (node *Node) becomeFollower(newTerm uint64, leaderId int64) {
	node.state = Follower
	node.currentTerm = newTerm
	node.votedFor = -1
	node.potentialLeader = leaderId
	node.electionResetEvent = time.Now()

	go node.runElectionTimer()
}

func (node *Node) persistToStorage() {
	for _, data := range []struct {
		name  string
		value interface{}
	}{
		{"currentTerm", node.currentTerm},
		{"votedFor", node.votedFor},
		{"log", node.log},
	} {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(data.value); err != nil {
			log.Fatal("encode error: ", err)
		}
		node.db.Set(data.name, buf.Bytes())
	}
}

func (node *Node) restoreFromStorage() {
	fmt.Printf("Restoring from storage on node: %d\n", node.id)
	for _, data := range []struct {
		name  string
		value interface{}
	}{
		{"currentTerm", &node.currentTerm},
		{"votedFor", &node.votedFor},
		{"log", &node.log},
	} {
		if value, found := node.db.Get(data.name); found {
			dec := gob.NewDecoder(bytes.NewBuffer(value))
			if err := dec.Decode(data.value); err != nil {
				log.Fatal("decode error: ", err)
			}
		} else {
			log.Fatal("No data found for", data.name)
		}
	}
}

func (node *Node) readFromStorage(key string, reply interface{}) error {
	if value, found := node.db.Get(key); found {
		dec := gob.NewDecoder(bytes.NewBuffer(value))
		if err := dec.Decode(reply); err != nil {
			return err
		}
		return nil
	} else {
		err := fmt.Errorf("KeyNotFound:%v", key)
		return err
	}
}

func (node *Node) newLogEntry(command interface{}) (bool, interface{}, error) {
	node.mu.Lock()
	// fmt.Printf("[Query %d] %v\n", node.id, command)
	// fmt.Printf("Running Submit on node %d (leader, term = %v %v)\n", node.id, node.state == Leader, node.currentTerm)
	if node.state == Leader {
		switch v := command.(type) {
		case Read:
			// fmt.Printf("READ v: %v", v)
			key := v.Key
			var value int
			readErr := node.readFromStorage(key, &value)
			// fmt.Printf("key, value = %v, %v\n", key, value)
			node.mu.Unlock()
			return true, value, readErr
		case AddServer:
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		case RemoveServer:
			if !node.peerList.Exists(v.ServerId) || node.id == v.ServerId {
				node.mu.Unlock()
				return false, nil, errors.New("server with id " + fmt.Sprint(v.ServerId) + " cannot be removed from peer " + fmt.Sprint(node.id))
			}
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			node.peerList.Remove(v.ServerId)
			// fmt.Printf("[%d] Removed peer %d from leader list\n", node.id, v.ServerId)
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		default:
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		}
	} else {
		switch v := command.(type) {
		case Read:
			// fmt.Printf("READ v: %v", v)
			key := v.Key
			var value int
			readErr := node.readFromStorage(key, &value)
			// fmt.Printf("key, value = %v, %v\n", key, value)
			node.mu.Unlock()
			return true, value, readErr
		}
	}

	node.mu.Unlock()
	return false, nil, nil
}

func (node *Node) Stop() {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.state = Dead
	node.potentialLeader = -1
	close(node.newCommitReady)
}

func (node *Node) Report() (id int64, term uint64, isLeader bool) {
	node.mu.Lock()
	defer node.mu.Unlock()

	isLeader = node.state == Leader
	return node.potentialLeader, node.currentTerm, isLeader
}

func (state NodeState) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("Error: Unknown state")
	}
}
