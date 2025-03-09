package raft

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Write struct {
	Key string
	Val int
}

type Read struct {
	Key string
}

type AddServer struct {
	ServerId uint64
	Addr     string
}

type RemoveServer struct {
	ServerId uint64
}

type Server struct {
	id          uint64
	mu          sync.Mutex
	peerList    Set
	peerAddress map[uint64]string
	rpcServer   *rpc.Server
	listener    net.Listener
	peers       map[uint64]*rpc.Client
	quit        chan interface{}
	wg          sync.WaitGroup

	node       *Node
	db         *Database
	commitChan chan CommitEntry
	ready      <-chan interface{}
}

func CreateServer(
	serverId uint64,
	db *Database,
	ready <-chan interface{},
	commitChan chan CommitEntry,
) (*Server, error) {
	server := new(Server)
	server.id = serverId
	server.peerList = makeSet()
	server.peers = make(map[uint64]*rpc.Client)
	server.peerAddress = make(map[uint64]string)
	server.db = db
	server.ready = ready
	server.commitChan = commitChan
	server.quit = make(chan interface{})
	return server, nil
}

func (server *Server) ConnectionAccept() {
	defer server.wg.Done()

	for {
		connection, err := server.listener.Accept()
		if err != nil {
			select {
			case <-server.quit:
				log.Printf("[%d] Accepting no more connection\n", server.id)
				return
			default:
				log.Fatalf("[%d] Error in accepting connection %s\n", server.id, err)
			}
		}
		server.wg.Add(1)
		go func() {
			server.rpcServer.ServeConn(connection)
			server.wg.Done()
		}()
	}
}

func (server *Server) Serve(port ...string) {
	server.mu.Lock()
	server.node = CreateNode(server.id, server.peerList, server, server.db, server.ready, server.commitChan)
	// server.node = NewRaftNode(server.id, server.peerList, server, server.db, server.ready, server.commitChan)
	server.rpcServer = rpc.NewServer()
	server.rpcServer.RegisterName("RaftNode", server.node)
	var err error
	var tcpPort string = ":"
	if len(port) == 1 {
		tcpPort = tcpPort + port[0]
	} else {
		tcpPort = tcpPort + "0"
	}
	server.listener, err = net.Listen("tcp", tcpPort)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] Listening at %s\n", server.id, server.listener.Addr())
	server.mu.Unlock()

	server.wg.Add(1)
	go server.ConnectionAccept()
}

func (server *Server) DisconnectAll() {
	server.mu.Lock()
	defer server.mu.Unlock()
	for id := range server.peers {
		server.node.removePeer(id)
		go server.DisconnectPeer(id)
	}
}

func (server *Server) RequestToLeaveCluster() {
	args := LeaveClusterArgs{ServerId: server.id}
	var reply LeaveClusterReply
	if err := server.RPC(0, "RaftNode.LeaveCluster", args, &reply); err != nil {
		log.Printf("[%d] Error leaving cluster: %v\n", server.id, err)
	}
	if reply.Success {
		server.DisconnectAll()
	}
}

func (server *Server) Shutdown() {
	server.RequestToLeaveCluster()
	server.Stop()
	close(server.commitChan)
}

func (server *Server) Stop() {
	server.node.Stop()
	close(server.quit)
	server.listener.Close()
	log.Printf("[%d] Waiting for existing connections to close\n", server.id)
	server.wg.Wait()
	log.Printf("[%d] All connections closed. Stopping server\n", server.id)
}

func (server *Server) GetListenerAddr() net.Addr {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.listener.Addr()
}

func (server *Server) ConnectToPeer(peerId uint64, addr string) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	// fmt.Printf("Before Connecting to peer %d at address %v\n", peerId, addr)
	peer, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	server.peers[peerId] = peer
	server.peerAddress[peerId] = addr
	// fmt.Printf("Connected to peer %d at address %v\n", peerId, addr)
	return nil
}

func (server *Server) DisconnectPeer(peerId uint64) error {
	// fmt.Printf("Before Disconnecting peer %d\n", peerId)
	server.mu.Lock()
	defer server.mu.Unlock()
	// fmt.Printf("Disconnecting peer %d\n", peerId)
	peer := server.peers[peerId]
	if peer != nil && !server.peerList.Exists(peerId) {
		err := peer.Close()
		delete(server.peers, peerId)
		delete(server.peerAddress, peerId)
		fmt.Printf("Peer %d is disconnected\n", peerId)
		return err
	}
	return nil
}

func (server *Server) RPC(peerId uint64, rpcCall string, args interface{}, reply interface{}) error {
	server.mu.Lock()
	peer := server.peers[peerId]
	server.mu.Unlock()
	if peer == nil {
		return fmt.Errorf("[%d] RPC Call to peer %d after it has been closed", server.id, peerId)
	} else {
		return peer.Call(rpcCall, args, reply)
	}
}

func (server *Server) GetServerId() uint64 {
	return server.id
}

func (server *Server) SetData(key string, value []byte) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.db.Set(key, value)
}

func (server *Server) GetData(key string) ([]byte, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.db.Get(key)
}

func (server *Server) AddToCluster(serverId uint64) {
	if serverId == server.id {
		return
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.peers[serverId] != nil && !server.peerList.Exists(serverId) {
		server.node.addPeer(serverId)
	}
}

func (server *Server) RemoveFromCluster(serverId uint64) {
	if serverId == server.id {
		return
	}
	server.mu.Lock()
	if !server.peerList.Exists(serverId) {
		server.mu.Unlock()
		server.DisconnectPeer(serverId)
		return
	}
	server.mu.Unlock()
}

func (server *Server) RequestToJoinCluster(leaderId uint64, addr string) error {
	// fmt.Printf("Joining cluster with leader id %d and address: %v\n", leaderId, addr)
	if leaderId < 0 {
		fmt.Printf("invalid leader id %d\n", leaderId)
		return errors.New("invalid leader id")
	}
	if server.GetServerId() == leaderId {
		return errors.New("cannot join own cluster")
	}

	// fmt.Printf("Connecting to leader %d at address %v\n", leaderId, addr)
	if err := server.ConnectToPeer(leaderId, addr); err != nil {
		fmt.Printf("Error connecting to leader %d at address %v\n", leaderId, addr)
		return err
	}
	// fmt.Printf("Connected to leader %d at address %v\n", leaderId, addr)

	joinClusterArgs := JoinClusterArgs{ServerId: server.id, ServerAddr: server.listener.Addr().String()}
	var joinClusterReply JoinClusterReply
	if err := server.RPC(leaderId, "RaftNode.JoinCluster", joinClusterArgs, &joinClusterReply); err != nil {
		fmt.Printf("Error joining cluster: %v\n", err)
		return err
	}
	if joinClusterReply.Success {
		// fmt.Printf("Joined cluster successfully: %v\n", joinClusterReply)
		fetchPeerListArgs := FetchPeerListArgs{Term: joinClusterReply.Term}
		var fetchPeerListReply FetchPeerListReply
		if err := server.RPC(leaderId, "RaftNode.FetchPeerList", fetchPeerListArgs, &fetchPeerListReply); err != nil {
			return err
		}
		if fetchPeerListReply.Success {
			// fmt.Printf("Fetched peer list successfully: %v\n", fetchPeerListReply.PeerAddress)
			for peerId, addr := range fetchPeerListReply.PeerAddress {
				if peerId != server.id {
					server.ConnectToPeer(peerId, addr)
				}
			}
			server.node.joinAsPeer(joinClusterReply.LeaderId, fetchPeerListReply.Term, fetchPeerListReply.PeerSet)
		} else {
			return fmt.Errorf("failed to fetch peer list from leader %d", leaderId)
		}
	} else {
		fmt.Printf("Failed to join cluster: %v\n", joinClusterReply)
	}
	return nil
}

func (server *Server) CheckLeader() (int, int, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.node.Report()
}

func (server *Server) GetCurrentTerm() uint64 {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.node.currentTerm
}

func (server *Server) getPeerAddress() map[uint64]string {
	server.mu.Lock()
	defer server.mu.Unlock()
	peerAddress := make(map[uint64]string, len(server.peerAddress))
	for k, v := range server.peerAddress {
		peerAddress[k] = v
	}
	return peerAddress
}
