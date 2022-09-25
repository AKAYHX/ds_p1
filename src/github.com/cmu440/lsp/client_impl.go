// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"sort"
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
	// Current ack-ed seq num
	currentAckNum chan int
	// All ack-ed and ready to process msg
	ackedMsg chan []Message
	// All ack-ed but cannot be processed msg (sorted in order)
	ackQueue chan []Message
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
	_, err = udpConn.WriteToUDP(request, addr)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, MaxPacketSize)
	var response Message
	bytes, _, err := udpConn.ReadFromUDP(buffer)
	if err = json.Unmarshal(buffer[:bytes], &response); err != nil {
		return nil, err
	}

	// Check if the connection is valid
	if response.Type == MsgAck && response.SeqNum == initialSeqNum {
		cli := &client{
			udpConn:       udpConn,
			udpAddr:       addr,
			connID:        response.ConnID,
			closed:        make(chan bool, 1),
			currentSeqNum: make(chan int, 1),
			currentAckNum: make(chan int, 1),
			ackedMsg:      make(chan []Message, 1), // TODO update to use pointer
			ackQueue:      make(chan []Message, 1),
		}
		cli.closed <- false
		cli.currentSeqNum <- initialSeqNum
		cli.currentAckNum <- -1
		cli.ackQueue <- []Message{}
		cli.ackedMsg <- []Message{}
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

	go c.ProcessMessage()

	for {
		select {
		case msgList := <-c.ackedMsg:
			if len(msgList) == 0 {
				c.ackedMsg <- msgList
			} else {
				c.ackedMsg <- msgList[1:]
				msg := msgList[0]

				if msg.Type == MsgData {
					// Ack the data
					ack, err := json.Marshal(NewAck(c.connID, msg.SeqNum))
					if err != nil {
						return nil, err
					}

					_, err = c.udpConn.WriteToUDP(ack, c.udpAddr)
					if err != nil {
						return nil, err
					}
				}

				ackNum := <-c.currentAckNum
				ackNum = msg.SeqNum
				c.currentAckNum <- ackNum
				return msg.Payload, nil
			}
		}
	}
}

func (c *client) ProcessMessage() {
	buffer := make([]byte, MaxPacketSize)
	var response Message
	bytes, _, err := c.udpConn.ReadFromUDP(buffer)
	if err = json.Unmarshal(buffer[:bytes], &response); err != nil {
		_ = fmt.Errorf("cannot marshal")
		return
	}
	if response.ConnID != c.connID {
		_ = fmt.Errorf("incorrect conn id")
		return
	}

	ackNum := <-c.currentAckNum
	c.currentAckNum <- ackNum
	if response.Type == MsgAck {
		// Handle Ack
		if ackNum == response.SeqNum-1 {
			// Case 1: the message comes in order
			c.AddAckedMsg(response)
		} else {
			// Case 2: the message comes out of order
			queue := <-c.ackQueue
			for i, msg := range queue {
				if ackNum == msg.SeqNum-1 {
					c.AddAckedMsg(msg)
					queue[i] = response
					break
				}
			}
			// TODO can improve the performance by optimizing here
			sort.Slice(queue, func(i, j int) bool {
				return queue[i].SeqNum < queue[j].SeqNum
			})
			c.ackQueue <- queue
		}

	} else if response.Type == MsgCAck {
		// Handle CAck
		queue := <-c.ackQueue
		ackedMsg := <-c.ackedMsg
		var newQueue []Message
		for i, msg := range queue {
			if msg.SeqNum > response.SeqNum {
				newQueue = queue[i:]
				break
			}
			ackedMsg = append(ackedMsg, msg)
		}
		c.ackedMsg <- ackedMsg
		c.ackQueue <- newQueue

	} else if response.Type == MsgData {
		// Handle data
		if ackNum == response.SeqNum-1 {
			c.AddAckedMsg(response)
		} else {
			queue := <-c.ackQueue
			queue = append(queue, response)
			c.ackQueue <- queue
		}
	}
}

func (c *client) AddAckedMsg(msg Message) {
	askedMsg := <-c.ackedMsg
	askedMsg = append(askedMsg, msg)
	c.ackedMsg <- askedMsg
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

	_, err = c.udpConn.WriteToUDP(message, c.udpAddr)
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
		ackNum := <-c.currentAckNum
		c.currentAckNum <- ackNum
		seqNum := <-c.currentSeqNum
		c.currentSeqNum <- seqNum

		if ackNum == seqNum {
			break
		}
	}

	return c.udpConn.Close()
}
