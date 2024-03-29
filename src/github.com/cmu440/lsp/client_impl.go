// Contains the implementation of a LSP client.

package lsp

import (
	"bufio"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"time"
)

const (
	MaxPacketSize      = 1000
	MilliToNano        = 1000000
	DisconnectErrorMsg = "disconnected"
)

type client struct {
	udpConn *lspnet.UDPConn
	udpAddr *lspnet.UDPAddr
	connID  int
	// Begin the close process
	closing chan bool
	// Detect and notify when the connection is closed
	connectionClosed chan bool
	// Signal that all should be closed
	finalClose chan bool
	// related params
	params *Params
	// Current sent seq num
	currentSeqNum chan int
	// Current processed data msg seq num
	currentProcessedMsgSeqNum chan int
	// Current data msg
	largestDataSeqNum chan int
	// Largest msg seq num that has read
	largestReadMsgSeqNum chan int
	// Current epoch
	currentEpoch chan int
	// Notify the current epoch
	epochTimeout chan int
	// Map of non-acked message {seq num: message}
	nonAckMsgMap chan map[int]*ClientMessage
	// Sliding window of messages being sent
	slidingWindow chan []int
	// Check the epoch since last message from the server
	idleEpoch chan int
	// List of messages that we try to send but haven't sent
	writeMessageBuffer chan []*ClientMessage
	// A channel to notify ready to process message
	dataMsgReady chan bool
	// Cache all ready to process message in order
	readyDataMsgCache chan []Message
}

type ClientMessage struct {
	message     *Message
	backoff     int
	resendEpoch int
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
	cli := &client{
		connID:                    0,
		params:                    params,
		closing:                   make(chan bool, 1),
		currentSeqNum:             make(chan int, 1),
		currentProcessedMsgSeqNum: make(chan int, 1),
		largestDataSeqNum:         make(chan int, 1),
		dataMsgReady:              make(chan bool),
		epochTimeout:              make(chan int),
		nonAckMsgMap:              make(chan map[int]*ClientMessage, 1),
		idleEpoch:                 make(chan int, 1),
		connectionClosed:          make(chan bool),
		slidingWindow:             make(chan []int, 1),
		writeMessageBuffer:        make(chan []*ClientMessage, 1),
		currentEpoch:              make(chan int, 1),
		finalClose:                make(chan bool, 1),
		largestReadMsgSeqNum:      make(chan int, 1),
		readyDataMsgCache:         make(chan []Message, 1),
	}
	cli.closing <- false
	cli.finalClose <- false
	cli.currentSeqNum <- initialSeqNum
	cli.currentProcessedMsgSeqNum <- 0
	cli.largestDataSeqNum <- -1
	cli.nonAckMsgMap <- make(map[int]*ClientMessage)
	cli.idleEpoch <- 0
	cli.slidingWindow <- []int{}
	cli.writeMessageBuffer <- []*ClientMessage{}
	cli.currentEpoch <- 0
	cli.largestReadMsgSeqNum <- -1
	cli.readyDataMsgCache <- []Message{}

	epoch := 0
	for {
		// Send connect message
		if cli.setupConnection(initialSeqNum, hostport) {
			go cli.handleMessage()
			go cli.handleResendMessage()
			go cli.epochTimer()

			return cli, nil
		}
		// Set the epoch counter to limit re-connecting process
		epoch++
		if epoch > cli.params.EpochLimit {
			break
		}

		// Wait for one epoch to resend
		select {
		case <-time.After(time.Duration(MilliToNano * params.EpochMillis)):
		}
	}
	return nil, errors.New(DisconnectErrorMsg)
}

// Set up connection with the server
func (c *client) setupConnection(initialSeqNum int, hostport string) bool {
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return false
	}
	c.udpAddr = addr
	udpConn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return false
	}
	c.udpConn = udpConn

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

// The go routine to set up epoch timer.
func (c *client) epochTimer() {
	for {
		finalClosed := <-c.finalClose
		c.finalClose <- finalClosed
		if finalClosed {
			return
		}

		select {
		case <-time.After(time.Duration(MilliToNano * c.params.EpochMillis)):
			// Count the epoch and notify the new epoch arrives
			currentEpoch := <-c.currentEpoch
			currentEpoch += 1
			c.currentEpoch <- currentEpoch
			c.epochTimeout <- currentEpoch

			go func() {
				// Send heart beat if no message sent during this epoch
				c.sendHeartBeat()

				// Close the connection if the idle epoch exceed the limit
				select {
				case idleEpoch := <-c.idleEpoch:
					if idleEpoch < 0 {
						// At least one msg received
						c.idleEpoch <- 0
					} else {
						// No message from the server in this epoch
						c.idleEpoch <- idleEpoch + 1
					}
					if idleEpoch+1 > c.params.EpochLimit {
						select {
						case <-c.connectionClosed:
						default:
						}
						c.connectionClosed <- true
						<-c.closing
						c.closing <- true
						return
					}
				}
			}()
		}
	}
}

// Send heart beat
func (c *client) sendHeartBeat() {
	ack, _ := json.Marshal(NewAck(c.connID, 0))

	closed := <-c.closing
	c.closing <- closed
	if closed {
		return
	}
	_, err := c.udpConn.Write(ack)
	// Detect the closeness of server
	if err != nil {
		select {
		case closed = <-c.connectionClosed:
		default:
		}
		c.connectionClosed <- true
	}
}

func (c *client) ConnID() int {
	return c.connID
}

// Read function called by the server
func (c *client) Read() ([]byte, error) {
	closed := <-c.finalClose
	c.finalClose <- closed
	if closed {
		return nil, errors.New(DisconnectErrorMsg)
	}

	for {
		select {
		case <-c.dataMsgReady:
			// A message is ready to read (in order)
			msgList := <-c.readyDataMsgCache
			msg := msgList[0]
			c.readyDataMsgCache <- msgList[1:]

			<-c.largestReadMsgSeqNum
			c.largestReadMsgSeqNum <- msg.SeqNum

			return msg.Payload, nil
		case <-c.connectionClosed:
			// Check if all messages have been processed
			largestDataSeqNum := <-c.largestDataSeqNum
			c.largestDataSeqNum <- largestDataSeqNum
			largestReadMsgSeqNum := <-c.largestReadMsgSeqNum
			c.largestReadMsgSeqNum <- largestReadMsgSeqNum

			if largestReadMsgSeqNum == largestDataSeqNum {
				return nil, errors.New(DisconnectErrorMsg)
			}

			// Otherwise, block to process one more message
			select {
			case <-c.dataMsgReady:
				msgList := <-c.readyDataMsgCache
				msg := msgList[0]
				c.readyDataMsgCache <- msgList[1:]

				<-c.largestReadMsgSeqNum
				c.largestReadMsgSeqNum <- msg.SeqNum

				return msg.Payload, nil
			case finalClosed := <-c.finalClose:
				c.finalClose <- finalClosed
				if finalClosed {
					return nil, errors.New(DisconnectErrorMsg)
				}
			}
		}
	}
}

// Receive message from the server
func (c *client) readMessage() Message {
	var response Message

	buffer := make([]byte, MaxPacketSize)
	bytes, err := bufio.NewReader(c.udpConn).Read(buffer)

	if err != nil {
		closed := <-c.closing
		closed = true
		c.closing <- closed

		return response
	}
	json.Unmarshal(buffer[:bytes], &response)

	// Received at least one message from the server
	idleEpoch := <-c.idleEpoch
	idleEpoch = -1
	c.idleEpoch <- idleEpoch

	return response
}

// Handle incoming message routine
func (c *client) handleMessage() {
	for {
		message := c.readMessage()

		// Exit the goroutine when all messages processed
		finalClose := <-c.finalClose
		c.finalClose <- finalClose
		if finalClose {
			return
		}

		go func(c *client) {
			if message.Type == MsgAck {
				// Handle Ack
				c.handleAckMsg(message)
			} else if message.Type == MsgCAck {
				// Handle CAck
				c.handleCAckMsg(message)
			} else if message.Type == MsgData {
				// Update the largest message which needs to be processed
				dataNum := <-c.largestDataSeqNum
				dataNum = Max(dataNum, message.SeqNum)
				c.largestDataSeqNum <- dataNum

				// Handle data
				c.handleDataMsg(message)
			}
		}(c)
	}
}

// Handle ack msg
func (c *client) handleAckMsg(message Message) {
	nonAckMsgMap := <-c.nonAckMsgMap
	delete(nonAckMsgMap, message.SeqNum)
	c.updateSlidingWindow(nonAckMsgMap)
	c.nonAckMsgMap <- nonAckMsgMap
}

// Handle CAck
func (c *client) handleCAckMsg(msg Message) {
	nonAckMsgMap := <-c.nonAckMsgMap

	// Ack all previous messages
	for seqNum := range nonAckMsgMap {
		if seqNum <= msg.SeqNum {
			delete(nonAckMsgMap, seqNum)
		}
	}

	c.updateSlidingWindow(nonAckMsgMap)
	c.nonAckMsgMap <- nonAckMsgMap
}

// Update sliding window and nonAckMsgMap. Put exceeded msg into buffer.
func (c *client) updateSlidingWindow(nonAckMsgMap map[int]*ClientMessage) {
	oldSlidingWindow := <-c.slidingWindow
	var slidingWindow []int
	for idx, seqNum := range oldSlidingWindow {
		if _, found := nonAckMsgMap[seqNum]; found {
			// Msg not received ack
			slidingWindow = oldSlidingWindow[idx:]
			break
		}
	}

	// Put message into buffer if needed
	buffer := <-c.writeMessageBuffer
	i := 0
	for ; i < len(buffer); i++ {
		if len(slidingWindow) >= c.params.WindowSize ||
			len(nonAckMsgMap) >= c.params.MaxUnackedMessages {
			break
		}
		nonAckMsgMap[buffer[i].message.SeqNum] = buffer[i]
		slidingWindow = append(slidingWindow, buffer[i].message.SeqNum)
		c.writeMessage(buffer[i].message)
	}

	if i == len(buffer) {
		buffer = []*ClientMessage{}
	} else {
		buffer = buffer[i:]
	}
	c.writeMessageBuffer <- buffer
	c.slidingWindow <- slidingWindow
}

// Handle Data msg
func (c *client) handleDataMsg(msg Message) {
	// Handle variable length
	if len(msg.Payload) < msg.Size {
		return
	}
	if len(msg.Payload) > msg.Size {
		msg.Payload = msg.Payload[:msg.Size]
	}
	// Verify if the message is corrupted
	checksum := CalculateChecksum(c.connID, msg.SeqNum, msg.Size, msg.Payload)
	if checksum != msg.Checksum {
		return
	}

	go func() {
		// Ack the data
		ack, _ := json.Marshal(NewAck(c.connID, msg.SeqNum))
		c.udpConn.Write(ack)

		for {
			ackNum := <-c.currentProcessedMsgSeqNum
			// Process data in order
			if msg.SeqNum-1 == ackNum {
				msgList := <-c.readyDataMsgCache
				msgList = append(msgList, msg)
				c.readyDataMsgCache <- msgList

				ackNum = msg.SeqNum
				c.currentProcessedMsgSeqNum <- ackNum
				// Notify read routine to read the data
				c.dataMsgReady <- true
				break
			}
			c.currentProcessedMsgSeqNum <- ackNum
		}
	}()

}

// Write function called by the server
func (c *client) Write(payload []byte) error {
	closed := <-c.finalClose
	c.finalClose <- closed
	if closed {
		return errors.New(DisconnectErrorMsg)
	}

	seqNum := <-c.currentSeqNum
	seqNum++
	c.currentSeqNum <- seqNum

	currentEpoch := <-c.currentEpoch
	c.currentEpoch <- currentEpoch
	clientMessage := &ClientMessage{message: NewData(c.connID, seqNum, len(payload), payload,
		CalculateChecksum(c.connID, seqNum, len(payload), payload)), backoff: 0, resendEpoch: currentEpoch + 1}

	nonAckMsgMap := <-c.nonAckMsgMap
	slidingWindow := <-c.slidingWindow
	if len(nonAckMsgMap) < c.params.MaxUnackedMessages &&
		len(slidingWindow) < c.params.WindowSize {
		// Can be written right now (satisfied window limit and unack limit)
		slidingWindow = append(slidingWindow, seqNum)
		nonAckMsgMap[seqNum] = clientMessage
		c.slidingWindow <- slidingWindow
		c.nonAckMsgMap <- nonAckMsgMap
		c.writeMessage(clientMessage.message)

	} else {
		// Cannot be sent
		c.slidingWindow <- slidingWindow
		c.nonAckMsgMap <- nonAckMsgMap

		buffer := <-c.writeMessageBuffer
		buffer = append(buffer, clientMessage)
		c.writeMessageBuffer <- buffer
	}

	return nil
}

// Send message to the server
func (c *client) writeMessage(message *Message) {
	marshaledMsg, _ := json.Marshal(message)
	c.udpConn.Write(marshaledMsg)
}

// Go Routine for resending messages
func (c *client) handleResendMessage() {
	for {
		closed := <-c.finalClose
		c.finalClose <- closed
		if closed {
			return
		}

		select {
		case currentEpoch := <-c.epochTimeout:
			// New epoch arrives
			slidingWindow := <-c.slidingWindow
			c.slidingWindow <- slidingWindow
			nonAckMsgMap := <-c.nonAckMsgMap
			for _, seqNum := range slidingWindow {
				if msg, found := nonAckMsgMap[seqNum]; found {
					// Only resend un-acked message
					if msg.resendEpoch <= currentEpoch {
						// Only resend messages which needs to be resend at the current epoch
						marshaledMsg, _ := json.Marshal(msg.message)
						c.udpConn.Write(marshaledMsg)

						// Calculate backoff
						if msg.backoff == 0 {
							msg.backoff = 1
						} else {
							msg.backoff *= 2
						}
						msg.backoff = Min(msg.backoff, c.params.MaxBackOffInterval)
						msg.resendEpoch = currentEpoch + msg.backoff + 1
						nonAckMsgMap[seqNum] = msg
					}
				}
			}
			c.nonAckMsgMap <- nonAckMsgMap
		}
	}
}

func (c *client) Close() error {
	// Send signals to start closing process
	closed := <-c.closing
	closed = true
	c.closing <- closed

	// Block until all pending msg processed
	for {
		// All write data msg have been ack-ed
		slidingWindow := <-c.slidingWindow
		c.slidingWindow <- slidingWindow
		if len(slidingWindow) > 0 {
			continue
		}
		buffer := <-c.writeMessageBuffer
		c.writeMessageBuffer <- buffer
		if len(buffer) > 0 {
			continue
		}

		// All read data msg have been ack-ed
		largestDataSeqNum := <-c.largestDataSeqNum
		c.largestDataSeqNum <- largestDataSeqNum
		largestReadMsgSeqNum := <-c.largestReadMsgSeqNum
		c.largestReadMsgSeqNum <- largestReadMsgSeqNum
		if largestDataSeqNum > largestReadMsgSeqNum {
			continue
		}
		break
	}
	c.udpConn.Close()

	// Notify to exit all routines
	<-c.finalClose
	c.finalClose <- true

	return nil
}

// Max Helper function to find the larger integer
func Max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

// Min Helper function to find the smaller integer
func Min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}
