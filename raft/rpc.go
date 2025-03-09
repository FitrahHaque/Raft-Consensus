package raft

import (
	"fmt"
	"math"
	"time"
)

func (node *Node) JoinCluster(args JoinClusterArgs, reply *JoinClusterReply) error {
	node.mu.Lock()
	// fmt.Printf("JoinCluster RPC from node %d on node %d\n", args.ServerId, node.id)
	if node.state != Leader {
		reply.Success = false
		reply.Term = 0
		reply.LeaderId = node.potentialLeader
		if node.potentialLeader != -1 {
			reply.LeaderAddr = node.server.GetPeerAddress(uint64(node.potentialLeader))
		}
		node.mu.Unlock()
		return nil
	}
	reply.LeaderId = int64(node.id)
	reply.Term = node.currentTerm
	if err := node.server.ConnectToPeer(args.ServerId, args.ServerAddr); err != nil {
		reply.Success = false
		node.mu.Unlock()
		return fmt.Errorf("failed to connect to peer %d: %v\n", args.ServerId, err)
	}
	reply.Success = true
	cmd := AddServer{ServerId: args.ServerId, Addr: args.ServerAddr}
	node.mu.Unlock()

	go node.newLogEntry(cmd)

	return nil
}

func (node *Node) FetchPeerList(args FetchPeerListArgs, reply *FetchPeerListReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.state != Leader {
		reply.Success = false
		reply.Term = 0
		reply.LeaderId = node.potentialLeader
		if node.potentialLeader != -1 {
			reply.LeaderAddr = node.server.GetPeerAddress(uint64(node.potentialLeader))
		}
		return nil
	}
	reply.Success = true
	reply.Term = node.currentTerm
	peerSet := make(map[uint64]struct{}, len(node.peerList.peerSet))
	for k, v := range node.peerList.peerSet {
		peerSet[k] = v
	}
	reply.PeerSet = peerSet
	reply.PeerAddress = node.server.getAllPeerAddresses()
	return nil
}

func (node *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.state == Dead || !node.peerList.Exists(args.CandidateId) {
		return nil
	}
	lastLogIndex, lastLogTerm := node.lastLogIndexAndTerm()
	if args.Term > node.currentTerm {
		node.becomeFollower(args.Term, int64(args.CandidateId))
	}

	if args.Term == node.currentTerm &&
		(node.votedFor == -1 || node.votedFor == int64(args.CandidateId)) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		node.votedFor = int64(args.CandidateId)
		node.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = node.currentTerm
	node.persistToStorage()
	return nil
}

func (node *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.state == Dead || !node.peerList.Exists(args.LeaderId) {
		return nil
	}
	if args.Term > node.currentTerm || args.LeaderId != uint64(node.potentialLeader) {
		node.becomeFollower(args.Term, int64(args.LeaderId))
	}
	reply.Success = false
	if args.Term == node.currentTerm {
		if node.state != Follower {
			node.becomeFollower(args.Term, int64(args.LeaderId))
		}
		node.electionResetEvent = time.Now()
		if args.LastLogIndex == 0 ||
			args.LastLogIndex <= uint64(len(node.log)) && args.LastLogTerm == node.log[args.LastLogIndex-1].Term {
			reply.Success = true
			//check node.commitLength?
			node.log = append(node.log[:args.LastLogIndex], args.Entries...)
			for _, entry := range args.Entries {
				cmd := entry.Command
				switch v := cmd.(type) {
				case AddServer:
					if node.id != v.ServerId {
						node.server.ConnectToPeer(v.ServerId, v.Addr)
					}
				case RemoveServer:
					if node.id != v.ServerId && node.peerList.Exists(v.ServerId) {
						node.peerList.Remove(v.ServerId)
					}
				}
			}
			if args.LeaderCommit > node.commitLength {
				node.commitLength = uint64(math.Min(float64(args.LeaderCommit), float64((len(node.log)))))
				node.newCommitReady <- struct{}{}
			}
		} else {
			if args.LastLogIndex > uint64(len(node.log)) {
				reply.RecoveryIndex = uint64(len(node.log)) + 1
				reply.RecoveryTerm = 0
			} else {
				reply.RecoveryTerm = node.log[args.LastLogIndex-1].Term
				reply.RecoveryIndex = 1
				for i := args.LastLogIndex - 1; i > 0; i-- {
					if node.log[i-1].Term != reply.RecoveryTerm {
						reply.RecoveryIndex = i + 1
						break
					}
				}
			}
		}
	}
	reply.Term = node.currentTerm
	node.persistToStorage()
	return nil
}

func (node *Node) LeaveCluster(args LeaveClusterArgs, reply *LeaveClusterReply) error {
	node.mu.Lock()

	if node.state != Leader {
		reply.Success = false
		node.mu.Unlock()
		return nil
	}
	reply.Success = true
	if !node.peerList.Exists(args.ServerId) {
		node.mu.Unlock()
		return nil
	}
	cmd := RemoveServer{ServerId: args.ServerId}
	node.mu.Unlock()

	go node.newLogEntry(cmd)

	return nil
}
