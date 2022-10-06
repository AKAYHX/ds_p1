// Contains the implementation of a LSP client.
/**
Sliding Window design:
 	openSlidingMsgWindow buffer of params.WindowSize
	slidingWindow ~ list of seq num
	write data: openSlidingMsgWindow is full -> block;
					not full -> update(nonAckMsgMap)
								-> len(nonAckMsgMap) > MaxUnackedMessages -> block
									-> update nonAckMsgMap, append(slidingWindow)
	receiving ack: update nonAckMsgMap,
					while (slidingWindow[0] not in nonAckMsgMap)
						{poll(openSlidingMsgWindow), slidingWindow[1:]}
*/

package lsp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

const (
	MaxPacketSize = 1000
	MilliToNano   = 1000000
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
	// Current processed data msg seq num
	currentProcessedMsgSeqNum chan int
	// Current data msg
	largestDataSeqNum chan int
	// Waited to be process data message
	readyDataMsg chan Message
	// Notify the current epoch
	epochTimeout chan bool
	// List of msg seq queue that need to be resend per epoch
	resendQueueList chan [][]int
	// Map of non-acked message {seq num: message}
	nonAckMsgMap chan map[int]*ClientMessage
	// Signal of the non-ack message slot
	openNonAckMsg chan int
	// Sliding window of messages being sent
	slidingWindow chan []int
	// Signal of the availability within the sliding window
	openSlidingMsgWindow chan int
	// Check if there is at least one message being sent in the current epoch
	activeEpoch chan int
	// Check the epoch since last message from the server
	idleEpoch chan int
	// Stop the client immediately rather than waiting the pending messages to be finished
	immediateStop chan bool
}

type ClientMessage struct {
	message *Message
	backoff int
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
		udpConn:                   udpConn,
		udpAddr:                   addr,
		connID:                    0,
		params:                    params,
		closed:                    make(chan bool, 1),
		currentSeqNum:             make(chan int, 1),
		currentProcessedMsgSeqNum: make(chan int, 1),
		largestDataSeqNum:         make(chan int, 1),
		readyDataMsg:              make(chan Message, 1),
		epochTimeout:              make(chan bool, 1),
		resendQueueList:           make(chan [][]int, 1),
		nonAckMsgMap:              make(chan map[int]*ClientMessage, 1),
		openSlidingMsgWindow:      make(chan int, params.WindowSize),
		activeEpoch:               make(chan int, 1),
		idleEpoch:                 make(chan int, 1),
		immediateStop:             make(chan bool, 1),
		slidingWindow:             make(chan []int, 1),
		openNonAckMsg:             make(chan int, params.MaxUnackedMessages),
	}
	cli.closed <- false
	cli.currentSeqNum <- initialSeqNum
	cli.currentProcessedMsgSeqNum <- -1
	cli.largestDataSeqNum <- -1
	cli.resendQueueList <- [][]int{}
	cli.nonAckMsgMap <- make(map[int]*ClientMessage)
	cli.epochTimeout <- true
	cli.immediateStop <- false
	cli.activeEpoch <- 0
	cli.idleEpoch <- 0
	cli.slidingWindow <- []int{}

	backoff := 0
	for {
		// Send connect message
		if cli.setupConnection(initialSeqNum) {
			go cli.handleMessage()
			go cli.handleResendMessage()
			go cli.epochTimer()

			return cli, nil
		}
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

		select {
		case <-time.After(time.Duration(MilliToNano * c.params.EpochMillis)):
			c.epochTimeout <- true

			go func() {
				select {
				case active := <-c.activeEpoch:
					c.activeEpoch <- 0
					if active == 0 {
						c.sendHeartBeat()
					}
				}

				select {
				case idleEpoch := <-c.idleEpoch:
					if idleEpoch < 0 {
						// At least one msg received
						c.idleEpoch <- 0
					} else {
						// No message from the server in this epoch
						c.idleEpoch <- idleEpoch + 1
					}
					if idleEpoch+1 >= c.params.EpochLimit {
						<-c.immediateStop
						c.immediateStop <- true
						<-c.closed
						c.closed <- true
						return
					}
				}
			}()
		}
	}
}

func (c *client) sendHeartBeat() {
	ack, _ := json.Marshal(NewAck(c.connID, 0))

	immediateStop := <-c.immediateStop
	c.immediateStop <- immediateStop
	if immediateStop {
		return
	}
	c.udpConn.Write(ack)
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

	for {
		select {
		case msg := <-c.readyDataMsg:
			ackNum := <-c.currentProcessedMsgSeqNum
			ackNum = msg.SeqNum
			c.currentProcessedMsgSeqNum <- ackNum
			return msg.Payload, nil
		}
	}
}

func (c *client) readMessage() Message {
	var response Message

	immediateStop := <-c.immediateStop
	c.immediateStop <- immediateStop
	if immediateStop {
		return response
	}

	buffer := make([]byte, MaxPacketSize)
	bytes, err := bufio.NewReader(c.udpConn).Read(buffer)

	if err = json.Unmarshal(buffer[:bytes], &response); err != nil {
		fmt.Println("cannot marshal")
	} else if response.ConnID != c.connID {
		fmt.Println("incorrect conn id")
	}

	// Received at least one message from the server
	idleEpoch := <-c.idleEpoch
	idleEpoch = -1
	c.idleEpoch <- idleEpoch

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
		go func(c *client) {
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
	}
}

func (c *client) handleAckMsg(message Message) {
	nonAckMsgMap := <-c.nonAckMsgMap
	c.removeNonAckMsg(nonAckMsgMap, message.SeqNum)
	c.nonAckMsgMap <- nonAckMsgMap

	c.updateSlidingWindow(nonAckMsgMap)
}

func (c *client) handleCAckMsg(msg Message) {
	nonAckMsgMap := <-c.nonAckMsgMap
	for seqNum := range nonAckMsgMap {
		if seqNum <= msg.SeqNum {
			c.removeNonAckMsg(nonAckMsgMap, msg.SeqNum)
		}
	}
	c.nonAckMsgMap <- nonAckMsgMap

	c.updateSlidingWindow(nonAckMsgMap)
}

func (c *client) removeNonAckMsg(nonAckMsgMap map[int]*ClientMessage, seqNum int) {
	delete(nonAckMsgMap, seqNum)
	select {
	case <-c.openNonAckMsg:
	default:
	}
}

func (c *client) updateSlidingWindow(nonAckMsgMap map[int]*ClientMessage) {
	slidingWindow := <-c.slidingWindow
	for idx, seqNum := range slidingWindow {
		if _, found := nonAckMsgMap[seqNum]; !found {
			// Message has been ack-ed
			select {
			case <-c.openSlidingMsgWindow:
			default:
			}
		} else {
			// not received ack
			slidingWindow = slidingWindow[idx:]
			break
		}
	}
	c.slidingWindow <- slidingWindow
}

func (c *client) handleDataMsg(msg Message) {
	dataNum := <-c.largestDataSeqNum
	dataNum = Max(dataNum, msg.SeqNum)
	c.largestDataSeqNum <- dataNum

	for {
		ackNum := <-c.currentProcessedMsgSeqNum
		c.currentProcessedMsgSeqNum <- ackNum
		// Process data in order
		if ackNum < 0 || msg.SeqNum-1 == ackNum {
			c.readyDataMsg <- msg
			break
		}
	}

	// Ack the data
	immediateStop := <-c.immediateStop
	c.immediateStop <- immediateStop
	if immediateStop {
		return
	}
	ack, _ := json.Marshal(NewAck(c.connID, msg.SeqNum))
	c.udpConn.Write(ack)
}

func (c *client) Write(payload []byte) error {
	closed := <-c.closed
	c.closed <- closed
	if closed {
		return nil
	}

	if c.udpConn == nil {
		return errors.New("broken udpConn")
	}

	seqNum := <-c.currentSeqNum
	seqNum++
	c.currentSeqNum <- seqNum

	go c.writeMessage(NewData(c.connID, seqNum, len(payload), payload,
		CalculateChecksum(c.connID, seqNum, len(payload), payload)))

	return nil
}

func (c *client) handleResendMessage() {
	for {
		immediateStop := <-c.immediateStop
		c.immediateStop <- immediateStop
		if immediateStop {
			return
		}

		select {
		case <-c.epochTimeout:
			// Pull the first resendQueue to resend
			var resendQueue []int
			resendQueueList := <-c.resendQueueList
			if len(resendQueueList) > 0 {
				resendQueue = resendQueueList[0]
				resendQueueList = resendQueueList[1:]
			}
			c.resendQueueList <- resendQueueList

			if len(resendQueue) > 0 {
				c.processResendMessageQueue(resendQueue)
			}
		}
	}
}

func (c *client) processResendMessageQueue(resendQueue []int) {
	nonAckMsgMap := <-c.nonAckMsgMap
	c.nonAckMsgMap <- nonAckMsgMap
	for _, seqNum := range resendQueue {
		if msg, found := nonAckMsgMap[seqNum]; found {
			go c.writeMessage(msg.message)
		}
	}
}

func (c *client) writeMessage(message *Message) {
	immediateStop := <-c.immediateStop
	c.immediateStop <- immediateStop
	if immediateStop {
		return
	}

	nonAckMsgMap := <-c.nonAckMsgMap
	c.nonAckMsgMap <- nonAckMsgMap
	if _, found := nonAckMsgMap[message.SeqNum]; !found {
		select {
		case c.openSlidingMsgWindow <- 1: // Block until the message can be sent
			select {
			case c.openNonAckMsg <- 1: // Block until the non-ack msg can be sent
				c.updateBackoffEpoch(message)
			}
		}
	}

	marshaledMsg, _ := json.Marshal(*message)
	c.udpConn.Write(marshaledMsg)

	active := <-c.activeEpoch
	active = 1
	c.activeEpoch <- active
}

// Update the backoff & nonAckMsgMap, and put into the resend queue
func (c *client) updateBackoffEpoch(msg *Message) {
	// Insert a new entry into nonAckMsgMap if it is a new msg
	nonAckMsgMap := <-c.nonAckMsgMap
	message, found := nonAckMsgMap[msg.SeqNum]
	var backoff int
	if !found {
		backoff = 0
		message = &ClientMessage{message: msg, backoff: 1}

		slidingWindow := <-c.slidingWindow
		slidingWindow = append(slidingWindow, msg.SeqNum)
		c.slidingWindow <- slidingWindow
	} else {
		backoff = message.backoff
		message.backoff *= 2
	}
	if backoff > c.params.MaxBackOffInterval {
		c.removeNonAckMsg(nonAckMsgMap, msg.SeqNum)
		c.nonAckMsgMap <- nonAckMsgMap

		select {
		case <-c.openSlidingMsgWindow:
		default:
		}
		slidingWindow := <-c.slidingWindow
		c.slidingWindow <- slidingWindow[1:]
	} else {
		nonAckMsgMap[msg.SeqNum] = message
		c.nonAckMsgMap <- nonAckMsgMap
	}

	// Put the message into resend queue
	resendQueueList := <-c.resendQueueList
	size := len(resendQueueList)
	for size <= backoff {
		resendQueueList = append(resendQueueList, []int{})
		size++
	}

	resendQueue := resendQueueList[backoff]
	resendQueue = append(resendQueue, msg.SeqNum)
	resendQueueList[backoff] = resendQueue
	c.resendQueueList <- resendQueueList
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
		// All write data msg have been ack-ed
		nonAckMsgMap := <-c.nonAckMsgMap
		c.nonAckMsgMap <- nonAckMsgMap
		if len(nonAckMsgMap) > 0 {
			continue
		}

		// All read data msg have been ack-ed
		largestDataSeqNum := <-c.largestDataSeqNum
		c.largestDataSeqNum <- largestDataSeqNum
		currentProcessedMsgSeqNum := <-c.currentProcessedMsgSeqNum
		c.currentProcessedMsgSeqNum <- currentProcessedMsgSeqNum
		if largestDataSeqNum > currentProcessedMsgSeqNum {
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


