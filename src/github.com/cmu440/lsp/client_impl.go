// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
)

type client struct {
	// TODO: implement this!
	connID int
	currentSeqNum int
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
	udpConn, err := lspnet.ListenUDP("udp", addr)
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

	var buffer []byte
	var response Message
	bytes, _, err := udpConn.ReadFromUDP(buffer)
	if err = json.Unmarshal(buffer[:bytes], &response); err != nil {
		return nil, err
	}

	// Check if the connection is valid
	if response.Type == MsgAck && response.SeqNum == initialSeqNum {
		return &client{
			connID: response.ConnID,
			currentSeqNum: initialSeqNum,
		}, nil
	}

	return nil, errors.New("failed to connect due to invalid response")
}

func (c *client) ConnID() int {
	return -1
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
