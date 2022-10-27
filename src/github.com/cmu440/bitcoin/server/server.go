package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
)

const chunksize = 10000

type server struct {
	lspServer lsp.Server
	miners    []*Miner
	allMiners map[int]*Miner
	clients   []*Client
}

type Client struct {
	clientID int
	data     string
	count    int
	tasks    []*Task
	hash     uint64
	nonce    uint64
}

type Miner struct {
	MinerID  int
	clientID int
	Low      uint64
	High     uint64
}

type Task struct {
	Low  uint64
	High uint64
}

func startServer(port int) (*server, error) {
	lspserver, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	server := &server{lspserver,
		make([]*Miner, 0),
		make(map[int]*Miner),
		make([]*Client, 0)}
	return server, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "serverLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()
	for {
		connID, payload, err := srv.lspServer.Read()
		if err != nil {
			//fmt.Println("err:", connID)
			if _, ok := srv.allMiners[connID]; ok {
				srv.minerFailure(connID)
			} else {
				srv.clientFailure(connID)
			}
		} else {
			var message bitcoin.Message
			err = json.Unmarshal(payload, &message)
			if err != nil {
				return
			}
			switch message.Type {
			case bitcoin.Join:
				//insert to miners
				miner := &Miner{connID, -1, 0, 0}
				srv.miners = append(srv.miners, miner)
				//fmt.Println("join:", connID)
				srv.allMiners[connID] = miner
			case bitcoin.Request:
				lower := message.Lower
				upper := message.Upper
				client := &Client{connID,
					message.Data,
					0,
					make([]*Task, 0),
					^uint64(0),
					0}
				for {
					client.count += 1
					if lower+chunksize >= upper {
						client.tasks = append(client.tasks, &Task{lower, upper})
						break
					} else {
						client.tasks = append(client.tasks, &Task{lower, lower + chunksize})
						lower += chunksize
					}
				}
				srv.insertClient(client)
			case bitcoin.Result:
				miner := srv.allMiners[connID]
				clientID := miner.clientID
				for i, client := range srv.clients {
					// fmt.Println("xx", srv.clients[i].clientID, clientID)
					if client.clientID == clientID {
						client.count -= 1
						if message.Hash < client.hash {
							client.nonce = message.Nonce
							client.hash = message.Hash
						}
						//if finished all subtasks computation
						if client.count == 0 {
							result := bitcoin.NewResult(client.hash, client.nonce)
							output, err := json.Marshal(result)
							if err != nil {
								continue
							}
							err = srv.lspServer.Write(client.clientID, output)
							if err != nil {
								srv.clientFailure(client.clientID)
							}
							// fmt.Println("~", srv.clients[0].clientID)
							// fmt.Println("~~~", srv.clients[1].clientID)
							srv.clients = append(srv.clients[:i], srv.clients[i+1:]...)
						}
						//} else if i != 0 {
						// // swap if less count
						// if client.count < srv.clients[i-1].count {
						//    temp := srv.clients[i-1]
						//    srv.clients[i-1] = client
						//    srv.clients[i] = temp
						// }
						//}
						break
					}
				}
				//insert to miners
				miner.clientID = -1
				srv.miners = append(srv.miners, miner)
			}
		}
		srv.process()
	}
}

//scheduler
func (srv *server) insertClient(client *Client) {
	flag := true
	for i, curr := range srv.clients {
		if len(curr.tasks) <= len(client.tasks) {
			continue
		} else {
			// srv.printClients()
			srv.clients = append(srv.clients[:i], append([]*Client{client}, srv.clients[i:]...)...)
			// srv.printClients()
			flag = false
			break
		}
	}
	if flag {
		srv.clients = append(srv.clients, client)
	}
}

//process and send request to miner
func (srv *server) process() {
	var client *Client
	i := 0
	for {
		// fmt.Println(len(srv.clients), len(srv.miners))
		if len(srv.clients) == 0 || len(srv.miners) == 0 {
			break
		}
		// traverse tasks in order
		for {
			if i < len(srv.clients) {
				client = srv.clients[i]
				// fmt.Println("!!", client.clientID, len(client.tasks))
			} else {
				break
			}
			if len(client.tasks) == 0 {
				i += 1
			} else {
				break
			}
		}
		// no task to compute
		if i == len(srv.clients) {
			break
		}
		miner := srv.miners[0]
		miner.Low = client.tasks[0].Low
		miner.High = client.tasks[0].High
		miner.clientID = client.clientID
		// fmt.Println(client.clientID, len(client.tasks), client.count)
		output, err := json.Marshal(bitcoin.NewRequest(client.data, miner.Low, miner.High))
		if err != nil {
			continue
		}
		err = srv.lspServer.Write(miner.MinerID, output)
		if err != nil {
			srv.minerFailure(miner.MinerID)
		} else {
			client.tasks = client.tasks[1:]
		}
		srv.miners = srv.miners[1:]
	}
}

func (srv *server) minerFailure(connID int) {
	// fmt.Println("miner:", connID)
	miner := srv.allMiners[connID]
	if miner.clientID != -1 {
		for _, client := range srv.clients {
			if client.clientID == miner.clientID {
				task := &Task{miner.Low, miner.High}
				client.tasks = append([]*Task{task}, client.tasks...)
				break
			}
		}
	}
}

func (srv *server) clientFailure(connID int) {
	// fmt.Println("client:", connID)
	for i, client := range srv.clients {
		if client.clientID == connID {
			srv.clients = append(srv.clients[:i], srv.clients[i+1:]...)
			break
		}
	}
}

func (srv *server) printClients() {
	fmt.Print("debug:")
	for _, client := range srv.clients {
		fmt.Print(client.clientID)
	}
}
