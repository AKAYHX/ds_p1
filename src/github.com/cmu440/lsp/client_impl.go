// Contains the implementation of a LSP client.

package lsp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

const (
	MaxPacketSize = 1000
)

type client struct {
	// TODO: implement this!
	udpConn *lspnet.UDPConn
	udpAddr *lspnet.UDPAddr
	connID  int
	closed  chan bool
	// Current sent seq num
	currentSeqNum chan int
	// Current processed seq num
	currentAckNum chan int
	// Current data msg
	largestDataNum chan int
	// Waited to be ack seq num (sorted in order)
	ackQueue chan []int
	// Waited to be process data message
	readyDataMsg chan Message
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

	// Send connect message
	request, err := json.Marshal(NewConnect(initialSeqNum))
	if err != nil {
		return nil, err
	}
	_, err = udpConn.Write(request)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, MaxPacketSize)
	var response Message
	bytes, err := udpConn.Read(buffer)
	if err = json.Unmarshal(buffer[:bytes], &response); err != nil {
		return nil, err
	}

	// Check if the connection is valid
	if response.Type == MsgAck && response.SeqNum == initialSeqNum {
		cli := &client{
			udpConn:        udpConn,
			udpAddr:        addr,
			connID:         response.ConnID,
			closed:         make(chan bool, 1),
			currentSeqNum:  make(chan int, 1),
			currentAckNum:  make(chan int, 1),
			largestDataNum: make(chan int, 1),
			ackQueue:       make(chan []int, 1),
			readyDataMsg:      make(chan Message, 1),
		}
		cli.closed <- false
		cli.currentSeqNum <- initialSeqNum
		cli.currentAckNum <- -1
		cli.largestDataNum <- -1
		cli.ackQueue <- []int{}

		go cli.handleMessage()

		return cli, nil
	}

	return nil, errors.New("failed to connect due to invalid response")
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	closed := <-c.closed
	c.closed <- closed
	if closed {
		return nil, nil
	}

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
	buffer := make([]byte, MaxPacketSize)
	var response Message
	bytes, err := bufio.NewReader(c.udpConn).Read(buffer)
	if err = json.Unmarshal(buffer[:bytes], &response); err != nil {
		_ = fmt.Errorf("cannot marshal")
	} else if response.ConnID != c.connID {
		_ = fmt.Errorf("incorrect conn id")
	}

	return response
}

func (c *client) handleMessage() {
	for {
		closed := <-c.closed
		c.closed <- closed
		if closed {
			return
		}

		message := c.readMessage()

		if message.Type == MsgAck {
			// Handle Ack
			go c.handleAckMsg(message)

		} else if message.Type == MsgCAck {
			// Handle CAck
			go c.handleCAckMsg(message)

		} else if message.Type == MsgData {
			// Handle data
			go c.handleDataMsg(message)
		}
	}
}

func (c *client) handleAckMsg(msg Message) {
	queue := <-c.ackQueue
	i := 0
	for i < len(queue) {
		if queue[i] == msg.SeqNum {
			break
		}
		i++
	}

	// Remove acked seq num
	queue = queue[:i]
	if i < len(queue)-1 {
		queue = append(queue, queue[i+1:]...)
	}
	c.ackQueue <- queue
}

func (c *client) handleCAckMsg(msg Message) {
	queue := <-c.ackQueue
	i := 0
	for i < len(queue) {
		if queue[i] == msg.SeqNum {
			break
		}
		i++
	}
	if i < len(queue)-1 {
		queue = queue[i+1:]
	} else {
		queue = []int{}
	}
	c.ackQueue <- queue
}

func (c *client) handleDataMsg(msg Message) {
	dataNum := <-c.largestDataNum
	dataNum = Max(dataNum, msg.SeqNum)
	c.largestDataNum <- dataNum

	for {
		ackNum := <-c.currentAckNum
		c.currentAckNum <- ackNum
		if ackNum < 0 || msg.SeqNum-1 == ackNum {
			c.readyDataMsg<-msg
			break
		}
	}

	// Ack the data
	ack, _ := json.Marshal(NewAck(c.connID, msg.SeqNum))
	c.udpConn.Write(ack)
}

func (c *client) Write(payload []byte) error {
	closed := <-c.closed
	c.closed <- closed
	if closed {
		return nil
	}

	seqNum := <-c.currentSeqNum
	seqNum++
	c.currentSeqNum <- seqNum

	message, err := json.Marshal(NewData(c.connID, seqNum, len(payload), payload, 0))
	if err != nil {
		return err
	}

	_, err = c.udpConn.Write(message)
	queue := <-c.ackQueue
	i := 0
	for i < len(queue) {
		if queue[i] > seqNum {
			break
		}
		i++
	}
	newQueue := append(queue[:i], seqNum)
	if i < len(queue) {
		newQueue = append(newQueue, queue[i:]...)
	}
	c.ackQueue <- newQueue

	return err
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
		ackQueue := <-c.ackQueue
		c.ackQueue <- ackQueue
		if len(ackQueue) > 0 {
			continue
		}

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
