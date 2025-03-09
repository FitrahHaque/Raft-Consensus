package raft

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

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
	var wg sync.WaitGroup
	for id := range server.peers {
		server.node.removePeer(id)
		wg.Add(1)
		go func(peerId uint64) {
			defer wg.Done()
			server.DisconnectPeer(peerId)
		}(id)
	}
	server.mu.Unlock()
	wg.Wait()
}

func (server *Server) RequestToLeaveCluster() {
	args := LeaveClusterArgs{ServerId: server.id}
	var reply LeaveClusterReply
	if err := server.RPC(0, "RaftNode.LeaveCluster", args, &reply); err != nil { //need to fix the leader (easy)
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
	if server.peers[serverId] != nil {
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
	if server.GetServerId() == leaderId {
		return errors.New("cannot join own cluster")
	}
	joinClusterArgs := JoinClusterArgs{ServerId: server.id, ServerAddr: server.listener.Addr().String()}
	var joinClusterReply JoinClusterReply
	for i := 0; i < 5; i++ {
		if err := server.ConnectToPeer(leaderId, addr); err != nil {
			fmt.Printf("Error connecting to leader %d at address %v\n", leaderId, addr)
			return err
		}
		if err := server.RPC(leaderId, "RaftNode.JoinCluster", joinClusterArgs, &joinClusterReply); err != nil {
			fmt.Printf("Error joining cluster: %v\n", err)
			return err
		}
		if joinClusterReply.Success {
			fetchPeerListArgs := FetchPeerListArgs{Term: joinClusterReply.Term}
			var fetchPeerListReply FetchPeerListReply
			if err := server.RPC(leaderId, "RaftNode.FetchPeerList", fetchPeerListArgs, &fetchPeerListReply); err != nil {
				fmt.Printf("Error fetching peer list from leader %d\n", leaderId)
				return err
			}
			if fetchPeerListReply.Success {
				for peerId, addr := range fetchPeerListReply.PeerAddress {
					if peerId != server.id {
						server.ConnectToPeer(peerId, addr)
					}
				}
				server.node.joinAsPeer(uint64(leaderId), fetchPeerListReply.Term, fetchPeerListReply.PeerSet)
				return nil
			} else if fetchPeerListReply.LeaderId != -1 {
				leaderId = uint64(fetchPeerListReply.LeaderId)
				addr = fetchPeerListReply.LeaderAddr
			}
		} else if joinClusterReply.LeaderId != -1 {
			leaderId = uint64(joinClusterReply.LeaderId)
			addr = joinClusterReply.LeaderAddr
		}
		time.Sleep(1000)
	}
	return fmt.Errorf("failed to join cluster: %v\n", joinClusterReply)
}

func (server *Server) CheckLeader() (int64, uint64, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.node.Report()
}

func (server *Server) GetCurrentTerm() uint64 {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.node.currentTerm
}

func (server *Server) getAllPeerAddresses() map[uint64]string {
	server.mu.Lock()
	defer server.mu.Unlock()
	peerAddress := make(map[uint64]string, len(server.peerAddress))
	for k, v := range server.peerAddress {
		peerAddress[k] = v
	}
	return peerAddress
}

func (server *Server) GetPeerAddress(peerId uint64) string {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.peerAddress[peerId]
}

func (server *Server) SubmitToServer(cmd interface{}) (bool, interface{}, error) {
	return server.node.newLogEntry(cmd)
}
