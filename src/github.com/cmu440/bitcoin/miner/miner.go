package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	// You will need this for randomized isn
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	// TODO: implement this!
	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return nil, err
	}

	// Miner sends join request
	payload, err := json.Marshal(bitcoin.NewJoin())
	if err != nil {
		return nil, err
	}
	err = client.Write(payload)
	if err != nil {
		fmt.Println("Failed to send join request")
		return nil, err
	}

	return client, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "minerLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	// TODO: implement this!
	for {
		request, err := miner.Read()
		if err != nil {
			return
		}

		var message bitcoin.Message
		json.Unmarshal(request, &message)
		if message.Type != bitcoin.Request {
			continue
		}

		// Exhausts all possible nonces
		nonce := message.Lower
		hash := ^uint64(0)
		for i := message.Lower; i <= message.Upper; i++ {
			tmpHash := bitcoin.Hash(message.Data, i)
			if tmpHash < hash {
				hash = tmpHash
				nonce = i
			}
		}

		// Return the least hash value
		response, _ := json.Marshal(bitcoin.NewResult(hash, nonce))
		miner.Write(response)
	}
}
