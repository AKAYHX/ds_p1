// Contains the implementation of a LSP client.

package lsp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"sort"
	"time"
)

const (
	MaxPacketSize = 1000
)

type client struct {
	udpConn *lspnet.UDPConn
	udpAddr *lspnet.UDPAddr
	connID  int
	closed  chan bool
	// related params
	params *Params
	// Current sent seq num
	currentSeqNum chan int
	// Current processed seq num
	currentAckNum chan int
	// Current data msg
	largestDataNum chan int
	// Waited to be ack msg seq num (sorted in order)
	// ackQueue chan []int
	// Waited to be process data message
	readyDataMsg chan Message
	// Current epoch
	currentEpoch chan int
	// Ack-ed seq num (sorted in order)
	ackMsgQueue chan []int
	// Map of backoff for each message
	backoffMap chan map[int]int
	// Map of messages that need to be resend, resend epoch as the key
	resendMsgMap chan map[int][]Message
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	udpConn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	cli := &client{
		udpConn:        udpConn,
		udpAddr:        addr,
		connID:         0,
		params:         params,
		closed:         make(chan bool, 1),
		currentSeqNum:  make(chan int, 1),
		currentAckNum:  make(chan int, 1),
		largestDataNum: make(chan int, 1),
		backoffMap:     make(chan map[int]int, 1),
		readyDataMsg:   make(chan Message, 1),
		currentEpoch:   make(chan int, 1),
		resendMsgMap:   make(chan map[int][]Message, 1),
		ackMsgQueue:    make(chan []int, 1),
	}
	cli.closed <- false
	cli.currentSeqNum <- initialSeqNum
	cli.currentAckNum <- -1
	cli.largestDataNum <- -1
	cli.backoffMap <- make(map[int]int)
	cli.currentEpoch <- 0
	cli.resendMsgMap <- make(map[int][]Message)
	cli.ackMsgQueue <- []int{}

	go cli.epochTimer()

	backoff := 0
	for {
		// Send connect message
		if cli.setupConnection(initialSeqNum) {
			go cli.handleMessage()
			go cli.handleResendMessage()
			fmt.Printf("true\n")

			return cli, nil
		}
		fmt.Printf("false\n")
		backoff++
		if backoff > cli.params.MaxBackOffInterval {
			break
		}

		// Wait for one epoch to resend
		time.After(time.Duration(cli.params.EpochMillis))
	}
	return nil, errors.New("exceed MaxBackOffInterval")
}

func (c *client) setupConnection(initialSeqNum int) bool {
	request, err := json.Marshal(NewConnect(initialSeqNum))
	if err != nil {
		return false
	}
	_, err = c.udpConn.Write(request)

	buffer := make([]byte, MaxPacketSize)
	var response Message
	bytes, err := c.udpConn.Read(buffer)
	if err = json.Unmarshal(buffer[:bytes], &response); err != nil {
		return false
	}

	// Check if the connection is valid
	if response.Type == MsgAck && response.SeqNum == initialSeqNum {
		c.connID = response.ConnID
		return true
	}
	return false
}

func (c *client) epochTimer() {
	for {
		closed := <-c.closed
		c.closed <- closed
		if closed {
			return
		}

		time.After(time.Duration(c.params.EpochMillis))
		currentEpoch := <-c.currentEpoch
		currentEpoch += 1
		c.currentEpoch <- currentEpoch
	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	closed := <-c.closed
	c.closed <- closed
	if closed {
		return nil, nil
	}
	fmt.Printf("call read\n")
	for {
		select {
		case msg := <-c.readyDataMsg:
			ackNum := <-c.currentAckNum
			ackNum = msg.SeqNum
			c.currentAckNum <- ackNum

			return msg.Payload, nil
		}
	}
}

func (c *client) readMessage() Message {
	fmt.Printf("popo\n")
	buffer := make([]byte, MaxPacketSize)
	var response Message
	bytes, err := bufio.NewReader(c.udpConn).Read(buffer)
	if err = json.Unmarshal(buffer[:bytes], &response); err != nil {
		fmt.Println("cannot marshal")
	} else if response.ConnID != c.connID {
		fmt.Println("incorrect conn id")
	}
	fmt.Printf("popo\n")

	return response
}

func (c *client) handleMessage() {
	for {
		closed := <-c.closed
		c.closed <- closed
		if closed {
			return
		}

		fmt.Printf("start reading...\n")
		message := c.readMessage()
		go func(c *client) {
			fmt.Printf("==> message:%s\n", message.String())

			if message.Type == MsgAck {
				// Handle Ack
				c.handleAckMsg(message)
			} else if message.Type == MsgCAck {
				// Handle CAck
				c.handleCAckMsg(message)
			} else if message.Type == MsgData {
				// Handle data
				c.handleDataMsg(message)
			}
		}(c)
		fmt.Printf("complete reading...\n")
	}
}

func (c *client) handleAckMsg(msg Message) {
	queue := <-c.ackMsgQueue
	i := 0
	for i < len(queue) {
		if queue[i] > msg.SeqNum {
			break
		}
		i++
	}
	newQueue := append(queue[:i], msg.SeqNum)
	if i < len(queue) {
		newQueue = append(newQueue, queue[i:]...)
	}

	c.ackMsgQueue <- newQueue
	fmt.Printf("complete handleAckMsg\n")
}

func (c *client) handleCAckMsg(msg Message) {
	queue := <-c.ackMsgQueue
	i := 0
	for i < len(queue) {
		if queue[i] > msg.SeqNum {
			break
		}
		i++
	}
	newQueue := queue[:i]

	var j int
	if i == 0 {
		j = 0
	} else {
		j = queue[i-1] + 1
	}
	for j < msg.SeqNum {
		newQueue = append(newQueue, j)
		j++
	}

	if i < len(queue) {
		newQueue = append(newQueue, queue[i:]...)
	}

	c.ackMsgQueue <- newQueue
}

func (c *client) handleDataMsg(msg Message) {
	dataNum := <-c.largestDataNum
	dataNum = Max(dataNum, msg.SeqNum)
	c.largestDataNum <- dataNum

	for {
		ackNum := <-c.currentAckNum
		c.currentAckNum <- ackNum
		if ackNum < 0 || msg.SeqNum-1 == ackNum {
			c.readyDataMsg <- msg
			break
		}
	}

	// Ack the data
	ack, _ := json.Marshal(NewAck(c.connID, msg.SeqNum))
	c.udpConn.Write(ack)
}

func (c *client) Write(payload []byte) error {
	fmt.Printf("writeeeeee %s\n", string(payload))
	closed := <-c.closed
	c.closed <- closed
	if closed {
		return nil
	}

	seqNum := <-c.currentSeqNum
	seqNum++
	c.currentSeqNum <- seqNum

	message := NewData(c.connID, seqNum, len(payload), payload, 0)
	marshaledMsg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = c.udpConn.Write(marshaledMsg)
	c.updateBackoffEpoch(message)

	return err
}

func (c *client) handleResendMessage() {
	for {
		closed := <-c.closed
		if closed {
			c.closed <- closed
			return
		}

		currentEpoch := <-c.currentEpoch
		c.currentEpoch <- currentEpoch

		resendMap := <-c.resendMsgMap
		for epoch, msgList := range resendMap {
			if epoch <= currentEpoch {
				go c.processResendMessage(msgList)
				delete(resendMap, epoch)
			}
		}
		c.resendMsgMap <- resendMap
	}
}

func (c *client) processResendMessage(msgList []Message) {
	ackQueue := <-c.ackMsgQueue
	c.ackMsgQueue <- ackQueue
	for _, msg := range msgList {
		// Check if the msg has been ack. If so, no need to resend; otherwise resend it.
		idx := sort.SearchInts(ackQueue, msg.SeqNum)
		if idx >= len(ackQueue) || ackQueue[idx] != msg.SeqNum {
			marshaledMsg, _ := json.Marshal(msg)
			c.udpConn.Write(marshaledMsg)
			c.updateBackoffEpoch(&msg)
		}
	}
}

// Update the backoff map and put into the resend queue
func (c *client) updateBackoffEpoch(msg *Message) {
	resendEpoch := <-c.currentEpoch
	c.currentEpoch <- resendEpoch
	backoffMap := <-c.backoffMap
	var backoff int
	if backoff, found := backoffMap[msg.SeqNum]; found {
		resendEpoch += backoff
		backoffMap[msg.SeqNum] = backoff * 2
	} else {
		resendEpoch += 1
		backoffMap[msg.SeqNum] = 2
	}
	c.backoffMap <- backoffMap
	if backoff > c.params.MaxBackOffInterval {
		return
	}

	resendMap := <-c.resendMsgMap
	if resendQueue, found := resendMap[resendEpoch]; found {
		resendQueue = append(resendQueue, *msg)
		resendMap[resendEpoch] = resendQueue
	} else {
		resendMap[resendEpoch] = []Message{*msg}
	}
	c.resendMsgMap <- resendMap
}

func (c *client) Close() error {
	closed := <-c.closed
	if closed {
		c.closed <- closed
		return nil
	}
	closed = true
	c.closed <- closed

	// Block until all pending msg processed
	for {
		// All ack msg have been processed
		ackQueue := <-c.ackMsgQueue
		c.ackMsgQueue <- ackQueue
		if len(ackQueue) > 0 {
			continue
		}

		// All write data have been ack-ed
		largestDataNum := <-c.largestDataNum
		c.largestDataNum <- largestDataNum
		currentAckNum := <-c.currentAckNum
		c.currentAckNum <- currentAckNum
		if largestDataNum > currentAckNum {
			continue
		}
		break
	}

	return c.udpConn.Close()
}

func Max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

// TODO checksum
// TODO heartbeat
// TODO sliding window
