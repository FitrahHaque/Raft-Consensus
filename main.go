package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/FitrahHaque/raft-consensus/raft"
)

var mu sync.Mutex
var Commits []raft.CommitEntry

// Assume serverIds are unique
func CreateServer(serverId uint64) (*raft.Server, error) {
	if serverId < 0 {
		return nil, errors.New("invalid peer id")
	}
	commitChan := make(chan raft.CommitEntry)
	storage := raft.NewDatabase()
	ready := make(chan interface{})
	server, err := raft.CreateServer(serverId, storage, ready, commitChan)
	if err != nil {
		return nil, err
	}
	port := fmt.Sprintf("%d", 8080+serverId)
	server.Serve(port)
	go CollectCommits(server, commitChan)

	close(ready)
	return server, nil
}

func CollectCommits(server *raft.Server, commitChan chan raft.CommitEntry) error {
	for commit := range commitChan {
		// fmt.Printf("Collect Commits from node %d, entry: %+v\n", server.GetServerId(), commit)
		//do we need a lock? - logic
		mu.Lock()
		// fmt.Printf("Collect Commits from node %d, entry: %+v\n", i, commit)
		// logtest(server.GetServerId(), "collectCommits (%d) got %+v", server.GetServerId(), commit)
		switch v := commit.Command.(type) {
		case raft.Write:
			var buf bytes.Buffer        // Buffer to hold the data
			enc := gob.NewEncoder(&buf) // Create a new encoder

			if err := enc.Encode(v.Val); err != nil { // Encode the data
				mu.Unlock()
				return err
			}
			fmt.Printf("Set key: %s, value: %d\n", v.Key, v.Val)
			server.SetData(v.Key, buf.Bytes()) // Save the data to the database
		case raft.AddServer:
			// fmt.Printf("Add server\n")
			server.AddToCluster(v.ServerId)
		case raft.RemoveServer:
			if server.GetServerId() != v.ServerId {
				server.RemoveFromCluster(v.ServerId)
				// fmt.Printf("[%d] Server %d removed from cluster\n", server.GetServerId(), v.ServerId)
			}
		default:
			// fmt.Printf("default\n")
			break
		}
		Commits = append(Commits, commit)
		mu.Unlock()
	}
	return nil
}

// write integer value to a string key in the database
// OPTIONAL to pass a particular server id to send command to
func SetData(server *raft.Server, key string, val int) error {
	cmd := raft.Write{Key: key, Val: val}
	if success, _, _ := server.SubmitToServer(cmd); success {
		return nil
	} else {
		return errors.New("command could not be submitted, try different server(leader)")
	}
}

// read integer value of a string key from the database
// OPTIONAL to pass a particular server id to send command to
func GetData(server *raft.Server, key string) (int, error) {
	cmd := raft.Read{Key: key}
	if success, reply, err := server.SubmitToServer(cmd); success {
		if err != nil {
			return -1, err
		} else {
			value, _ := reply.(int)
			return value, nil
		}
	} else {
		return 0, errors.New("command could not be submitted, try different server")
	}
}

// // add new server to the raft cluster
// func AddServers(cluster *raft.ClusterSimulator, serverIds []int) error {
// 	if cluster == nil {
// 		return errors.New("raft cluster not created")
// 	}
// 	commandToServer := raft.AddServers{ServerIds: serverIds}
// 	var err error
// 	serverId, _, err := cluster.CheckUniqueLeader()

// 	if err != nil {
// 		return err
// 	}

// 	if serverId < 0 {
// 		return errors.New("unable to submit command to any server")
// 	}

// 	if success, _, err := cluster.SubmitToServer(serverId, commandToServer); success {
// 		if err != nil {
// 			return err
// 		} else {
// 			return nil
// 		}
// 	} else {
// 		return errors.New("command could not be submitted, try different server")
// 	}
// }

// // remove server from the raft cluster
// func RemoveServers(cluster *raft.ClusterSimulator, serverIds []int) error {
// 	if cluster == nil {
// 		return errors.New("raft cluster not created")
// 	}
// 	commandToServer := raft.RemoveServers{ServerIds: serverIds}
// 	var err error
// 	serverId, _, err := cluster.CheckUniqueLeader()

// 	if err != nil {
// 		return err
// 	}

// 	if serverId < 0 {
// 		return errors.New("unable to submit command to any server")
// 	}

// 	if success, _, err := cluster.SubmitToServer(serverId, commandToServer); success {
// 		if err != nil {
// 			return err
// 		} else {
// 			return nil
// 		}
// 	} else {
// 		return errors.New("command could not be submitted, try different server")
// 	}
// }

// // disconnect a peer from the cluster
// func DisconnectPeer(cluster *raft.ClusterSimulator, peerId int) error {
// 	if cluster == nil {
// 		return errors.New("raft cluster not created")
// 	}
// 	if peerId < 0 {
// 		return errors.New("invalid peer id passed")
// 	}
// 	err := cluster.DisconnectPeer(uint64(peerId))
// 	return err
// }

// // reconnect a disconnected peer to the cluster
// func ReconnectPeer(cluster *raft.ClusterSimulator, peerId int) error {
// 	if cluster == nil {
// 		return errors.New("raft cluster not created")
// 	}
// 	if peerId < 0 {
// 		return errors.New("invalid peer id passed")
// 	}
// 	err := cluster.ReconnectPeer(uint64(peerId))
// 	return err
// }

// // crash a server
// func CrashPeer(cluster *raft.ClusterSimulator, peerId int) error {
// 	if cluster == nil {
// 		return errors.New("raft cluster not created")
// 	}
// 	if peerId < 0 {
// 		return errors.New("invalid peer id passed")
// 	}
// 	err := cluster.CrashPeer(uint64(peerId))
// 	return err
// }

// // restart a server
// func RestartPeer(cluster *raft.ClusterSimulator, peerId int) error {
// 	if cluster == nil {
// 		return errors.New("raft cluster not created")
// 	}
// 	if peerId < 0 {
// 		return errors.New("invalid peer id passed")
// 	}
// 	err := cluster.RestartPeer(uint64(peerId))
// 	return err
// }

// // shutdown all servers in the cluster and stop raft
// func Shutdown(cluster *raft.ClusterSimulator) error {
// 	if cluster == nil {
// 		return errors.New("raft cluster not created")
// 	}
// 	cluster.Shutdown()
// 	cluster = nil
// 	return nil
// }

func RemoveServer(server *raft.Server) {
	server.RequestToLeaveCluster()
}

// shutdown the server
func Stop(server *raft.Server) error {
	if server == nil {
		return nil
	}
	server.Stop()
	server = nil
	//*remove the server from the cluster logic
	return nil
}

func PrintMenu() {
	fmt.Println("\n\n           	RAFT MENU: [nodes are 0 indexed]")
	fmt.Println("+---------------------------+------------------------------------+")
	fmt.Println("| Sr |  USER COMMANDS       |      ARGUMENTS                     |")
	fmt.Println("+----+----------------------+------------------------------------+")
	fmt.Println("| 1  | create server        |      Id   		                  |")
	fmt.Println("| 2  | set data             |      key, value     				  |")
	fmt.Println("| 3  | get data             |      key                           |")
	fmt.Println("| 4  | disconnect peer      |      peerId                        |")
	fmt.Println("| 5  | reconnect peer       |      peerId                        |")
	fmt.Println("| 6  | crash peer           |      peerId                        |")
	fmt.Println("| 7  | restart peer         |      peerId                        |")
	fmt.Println("| 8  | shutdown             |      _                             |")
	fmt.Println("| 9  | check leader         |      _                             |")
	fmt.Println("| 10 | stop execution       |      _                             |")
	fmt.Println("| 11 | add servers (x)      |      [peerIds]                     |")
	fmt.Println("| 12 | remove server        |                                    |")
	fmt.Println("| 13 | join cluster         | 	    leaderId, leaderAddress       |")
	fmt.Println("+----+----------------------+------------------------------------+")
	fmt.Println("")
	fmt.Println("+--------------------      USER      ----------------------------+")
	fmt.Println("+                                                                +")
	fmt.Println("+ User input should be of the format:  Sr ...Arguments           +")
	fmt.Println("+ Example:  2 4 1 3                                              +")
	fmt.Println("+----------------------------------------------------------------+")
	fmt.Println("")
}

func main() {
	var input string
	var server *raft.Server = nil
	var peerId int = 0

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigCh
		fmt.Println("SIGNAL RECEIVED")
		Stop(server)
		os.Exit(0)
	}()

	gob.Register(raft.Write{})
	gob.Register(raft.Read{})
	gob.Register(raft.AddServer{})
	gob.Register(raft.RemoveServer{})

	fmt.Println("\n\n=============================================================")
	fmt.Println("=    Ensure that you set [DEBUG=0] in [raft/raft.go] file   =")
	fmt.Println("=============================================================")
	PrintMenu()

	for {
		fmt.Println("WAITING FOR INPUTS..")
		fmt.Println("")

		reader := bufio.NewReader(os.Stdin)
		input, _ = reader.ReadString('\n')
		tokens := strings.Fields(input)
		command, err0 := strconv.Atoi(tokens[0])
		if err0 != nil {
			fmt.Println("Wrong input")
			continue
		}
		switch command {
		case 1:
			if len(tokens) < 2 {
				fmt.Println("number of peers not passed")
				break
			}
			var err error
			peerId, err = strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Println("invalid number of peers")
				break
			}
			server, err = CreateServer(uint64(peerId))
			if err == nil {
				fmt.Printf("SERVER with id %d CREATED !!!\n", peerId)
			} else {
				fmt.Printf("err: %v\n", err)
			}
		case 13:
			if len(tokens) < 3 {
				fmt.Println("leader Id and port not passed")
				break
			}
			leaderId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Println("invalid leader id")
				break
			}
			if err != nil {
				fmt.Println("invalid leader port")
				break
			}
			err = server.RequestToJoinCluster(uint64(leaderId), tokens[2])
			if err != nil {
				fmt.Printf("err: %v\n", err)
			}
		case 2:
			if len(tokens) < 3 {
				fmt.Println("key or value not passed")
				break
			}
			val, err := strconv.Atoi(tokens[2])
			if err != nil {
				fmt.Println("invalid value passed")
				break
			}
			err = SetData(server, tokens[1], val)
			if err == nil {
				fmt.Printf("WRITE TO KEY %s WITH VALUE %d SUCCESSFUL\n", tokens[1], val)
			} else {
				fmt.Printf("%v\n", err)
			}
		case 3:
			if len(tokens) < 2 {
				fmt.Println("key not passed")
				break
			}
			val, err := GetData(server, tokens[1])
			if err == nil {
				fmt.Printf("READ KEY %s VALUE %d\n", tokens[1], val)
			} else {
				fmt.Printf("%v\n", err)
			}
		// case 4:
		// 	if len(tokens) < 2 {
		// 		fmt.Println("peer id not passed")
		// 		break
		// 	}
		// 	peer, err := strconv.Atoi(tokens[1])
		// 	if err != nil /*|| peer >= peers*/ {
		// 		fmt.Printf("invalid server id %d passed\n", peer)
		// 		break
		// 	}

		// 	err = DisconnectPeer(cluster, peer)
		// 	if err == nil {
		// 		fmt.Printf("PEER %d DISCONNECTED\n", peer)
		// 	} else {
		// 		fmt.Printf("%v\n", err)
		// 	}
		// case 5:
		// 	if len(tokens) < 2 {
		// 		fmt.Println("peer id not passed")
		// 		break
		// 	}
		// 	peer, err := strconv.Atoi(tokens[1])
		// 	if err != nil /*|| peer >= peers */ {
		// 		fmt.Printf("invalid server id %d passed\n", peer)
		// 		break
		// 	}
		// 	err = ReconnectPeer(cluster, peer)
		// 	if err == nil {
		// 		fmt.Printf("PEER %d RECONNECTED\n", peer)
		// 	} else {
		// 		fmt.Printf("%v\n", err)
		// 	}
		// case 6:
		// 	if len(tokens) < 2 {
		// 		fmt.Println("peer id not passed")
		// 		break
		// 	}
		// 	peer, err := strconv.Atoi(tokens[1])
		// 	if err != nil /*|| peer >= peers*/ {
		// 		fmt.Printf("invalid server id %d passed\n", peer)
		// 		break
		// 	}
		// 	err = CrashPeer(cluster, peer)
		// 	if err == nil {
		// 		fmt.Printf("PEER %d CRASHED\n", peer)
		// 	} else {
		// 		fmt.Printf("%v\n", err)
		// 	}
		// case 7:
		// 	if len(tokens) < 2 {
		// 		fmt.Println("peer id not passed")
		// 		break
		// 	}
		// 	peer, err := strconv.Atoi(tokens[1])
		// 	if err != nil /*|| peer >= peers*/ {
		// 		fmt.Printf("invalid server id %d passed\n", peer)
		// 		break
		// 	}
		// 	err = RestartPeer(cluster, peer)
		// 	if err == nil {
		// 		fmt.Printf("PEER %d RESTARTED\n", peer)
		// 	} else {
		// 		fmt.Printf("%v\n", err)
		// 	}
		// case 8:
		// 	err := Shutdown(cluster)
		// 	if err == nil {
		// 		fmt.Println("ALL SERVERS STOPPED AND RAFT SERVICE STOPPED")
		// 	} else {
		// 		fmt.Printf("%v\n", err)
		// 	}
		// 	cluster = nil
		case 9:
			leaderId, term, isLeader := server.CheckLeader()
			if isLeader || leaderId != -1 {
				fmt.Printf("LEADER ID: %d, TERM: %d\n", leaderId, term)
			} else {
				fmt.Printf("NODE %d IS NOT LEADER FOR CURRENT TERM %d\n", server.GetServerId(), server.GetCurrentTerm())
			}
		// case 10:
		// 	err := Stop(cluster)
		// 	if err == nil {
		// 		fmt.Println("STOPPING EXECUTION, NO INPUTS WILL BE TAKEN FURTHER")
		// 		cluster = nil
		// 		return
		// 	} else {
		// 		fmt.Printf("%v\n", err)
		// 	}
		// case 11:
		// 	if len(tokens) < 2 {
		// 		fmt.Println("peer ids not passed")
		// 		break
		// 	}
		// 	serverIds := make([]int, len(tokens)-1)
		// 	var val int
		// 	var err error
		// 	for i := 1; i < len(tokens); i++ {
		// 		val, err = strconv.Atoi(tokens[i])
		// 		if err != nil {
		// 			fmt.Println("Invalid server ID")
		// 			break
		// 		}
		// 		serverIds[i-1] = val
		// 	}

		// 	err = AddServers(cluster, serverIds)
		// 	if err == nil {
		// 		fmt.Printf("Added ServerIDs: %v to cluster", serverIds)
		// 	} else {
		// 		fmt.Printf("%v\n", err)
		// 	}
		case 12:
			RemoveServer(server)
			fmt.Printf("Server %d removed from cluster\n", server.GetServerId())
		default:
			fmt.Println("Invalid Command")
		}
		fmt.Println("\n---------------------------------------------------------")
		PrintMenu()
	}
}
