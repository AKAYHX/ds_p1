package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/cmu440/lsp"
)

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name               = "clientLog.txt"
		flag               = os.O_RDWR | os.O_CREATE
		perm               = os.FileMode(0666)
		disconnectErrorMsg = "disconnected"
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer client.Close()

	// TODO: implement this!
	// Send request to the server
	payload, err := json.Marshal(bitcoin.NewRequest(message, 0, maxNonce))
	if err != nil {
		return
	}
	err = client.Write(payload)
	if err != nil {
		printDisconnected()
		file.WriteString("Failed to send request. Error: " + err.Error())
		return
	}

	// Read response from the server
	response, err := client.Read()
	if err != nil {
		if err.Error() == disconnectErrorMsg {
			printDisconnected()
		}
		file.WriteString("Failed to read. Error: " + err.Error())
		return
	}
	var responseMsg bitcoin.Message
	json.Unmarshal(response, &responseMsg)

	printResult(responseMsg.Hash, responseMsg.Nonce)
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
