// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

const Maxsize = 2000

type server struct {
	conn               *lspnet.UDPConn
	readChan           chan *Msg
	writeChan          chan *Msg
	preWriteChan       chan *writeData
	outChan            chan *Message
	connectChan        chan *Msg
	clients            map[int]*serverClient
	id                 int
	ackChan            chan *Msg
	closeMain          chan bool
	closeRead          chan bool
	closeWrite         chan bool
	closeBuffer        chan bool
	closeConnect       chan bool
	closeAck           chan bool
	closeSeqNum        chan bool
	closeMsg           chan bool
	bufferChan         chan *serverClient
	seqAdd             chan *serverClient
	seqRead            chan *serverClient
	seqRes             chan int
	writeMsg           chan *writeClient
	readMsg            chan *readClient
	resMsg             chan *Message
	EpochLimit         int
	EpochMillis        int
	WindowSize         int
	MaxBackOffInterval int
	MaxUnackedMessages int
	clientBuffer       chan int
	closeunAckRoutine  chan bool
	serverseqAdd       chan *serverClient
	serverseqRead      chan *serverClient
	serverseqRes       chan int
	clientWinReq       chan *clientWindow
	clientWinRes       chan int
	unAckReq           chan *unAckMsg
	closehandleWrite   chan bool
	closeEpoch         chan bool
}

type serverClient struct {
	id           int
	SeqNum       int
	addr         *lspnet.UDPAddr
	msgList      map[int]*Message
	closed       bool
	serverSeqNum int
	unAcked      []*unAckMsg
	buffer       []*Msg
	left         int
	sent         bool
	heard        int
}

type Msg struct {
	addr    *lspnet.UDPAddr
	message *Message
}

type writeData struct {
	connId  int
	payload []byte
}

type readClient struct {
	client *serverClient
	seq    int
}

type writeClient struct {
	client  *serverClient
	seq     int
	message *Message
}

type clientWindow struct {
	client *serverClient
	num    int
}

type unAckMsg struct {
	client         *serverClient
	seq            int
	message        *Message
	CurrentBackoff int
	epoch          int
	add            int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	str := lspnet.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	addr, err := lspnet.ResolveUDPAddr("udp", str)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	s := &server{conn,
		make(chan *Msg),
		make(chan *Msg),
		make(chan *writeData),
		make(chan *Message),
		make(chan *Msg),
		make(map[int]*serverClient),
		0,
		make(chan *Msg),
		make(chan bool),
		make(chan bool),
		make(chan bool),
		make(chan bool),
		make(chan bool),
		make(chan bool),
		make(chan bool),
		make(chan bool),
		make(chan *serverClient),
		make(chan *serverClient),
		make(chan *serverClient),
		make(chan int),
		make(chan *writeClient),
		make(chan *readClient),
		make(chan *Message),
		params.EpochLimit,
		params.EpochMillis,
		params.WindowSize,
		params.MaxBackOffInterval,
		params.MaxUnackedMessages,
		make(chan int),
		make(chan bool),
		make(chan *serverClient),
		make(chan *serverClient),
		make(chan int),
		make(chan *clientWindow),
		make(chan int),
		make(chan *unAckMsg),
		make(chan bool),
		make(chan bool),
	}
	go s.mainRoutine()
	go s.readRoutine()
	go s.writeRoutine()
	go s.connectRoutine()
	go s.ackRoutine()
	go s.seqNumRoutine()
	go s.msgRoutine()
	go s.handleWrite()
	go s.unAckRoutine()
	return s, nil
}

func (s *server) mainRoutine() {
	for {
		select {
		case msg := <-s.readChan:
			//fmt.Println("readchan")
			message := msg.message
			id := msg.message.ConnID
			client := s.clients[id]
			if client != nil {
				client.heard = 0
			}
			switch message.Type {
			case MsgConnect:
				s.connectChan <- msg
			case MsgAck, MsgCAck:
				s.unAckReq <- &unAckMsg{client, -1, message, -1, -1, -1}
			case MsgData:
				s.ackChan <- msg
				seq := msg.message.SeqNum
				s.seqRead <- client
				currSeq := <-s.seqRes
				if seq <= currSeq {
					continue
				} else if seq == currSeq+1 {
					s.seqAdd <- client
					s.outChan <- msg.message
					for {
						s.seqRead <- client
						currSeq := <-s.seqRes
						s.readMsg <- &readClient{client, currSeq + 1}
						message := <-s.resMsg
						if message == nil {
							break
						}
						s.outChan <- message
						s.seqAdd <- client
					}
				} else {
					s.writeMsg <- &writeClient{client, seq, message}
				}
			}
		case <-s.closeMain:
			return
		}
	}
}

func (s *server) epochRoutine() {
	ticker := time.NewTicker(time.Duration(s.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			fmt.Println("ticker!!")
			for _, client := range s.clients {
				client.heard += 1
				if client.heard >= s.EpochLimit {
					delete(s.clients, client.id)
					continue
				}
				for _, msg := range client.unAcked {
					if msg.epoch == msg.CurrentBackoff {
						s.writeChan <- &Msg{client.addr, msg.message}
						msg.CurrentBackoff += msg.add + 1
						msg.add *= 2
						if msg.add > s.MaxBackOffInterval {
							msg.add = s.MaxBackOffInterval
						}
					}
					msg.epoch += 1
				}
				if client.sent {
					client.sent = false
				} else {
					ack := NewAck(client.id, 0)
					s.writeChan <- &Msg{client.addr, ack}
				}
			}
		case <-s.closeEpoch:
			ticker.Stop()
			return
		}
	}
}

//for sliding window
func (s *server) unAckRoutine() {
	for {
		select {
		case <-s.closeunAckRoutine:
			return
		case req := <-s.unAckReq:
			client := req.client
			seq := req.seq
			message := req.message
			//ack msg
			if seq == -1 {
				//fmt.Println("to ack ", message.SeqNum)
				dup := true
				for i := 0; i < len(client.unAcked); i++ {
					//fmt.Print(client.unAcked[i].message.SeqNum, " ")
					if client.unAcked[i].message.SeqNum == message.SeqNum {
						client.unAcked = append(client.unAcked[:i], client.unAcked[i+1:]...)
						dup = false
						break
					}
				}
				if dup {
					//fmt.Println("dup")
					continue
				}
				if len(client.unAcked) == 0 {
					if len(client.buffer) > 0 {
						client.left = client.buffer[0].message.SeqNum - 1
					} else {
						s.serverseqRead <- client
						num := <-s.serverseqRes
						client.left = num
					}
				} else {
					client.left = client.unAcked[0].message.SeqNum
				}
				left := client.left
				for {
					if len(client.buffer) > 0 && len(client.unAcked) < s.MaxUnackedMessages && left+s.WindowSize >= client.buffer[0].message.SeqNum {
						s.writeChan <- client.buffer[0]
						client.unAcked = append(client.unAcked, &unAckMsg{client, client.buffer[0].message.SeqNum, client.buffer[0].message, 0, 0, 1})
						client.buffer = client.buffer[1:]
					} else {
						break
					}
				}
				//fmt.Println("acked ", message.SeqNum)
				// add unack to list
			} else {
				left := client.left
				msg := &Msg{client.addr, message}
				if len(client.unAcked) < s.MaxUnackedMessages && left+s.WindowSize >= seq {
					//fmt.Println("toWrite", message.SeqNum)
					client.unAcked = append(client.unAcked, req)
					s.writeChan <- msg
				} else {
					//fmt.Println(len(client.unAcked), s.MaxUnackedMessages, left, s.WindowSize, seq)
					//fmt.Println("toBuffer", message.SeqNum)
					client.buffer = append(client.buffer, msg)
				}
			}
		}
	}
}

func (s *server) handleWrite() {
	for {
		select {
		case <-s.closehandleWrite:
			return
		case writedata := <-s.preWriteChan:
			connId := writedata.connId
			payload := writedata.payload
			client := s.clients[writedata.connId]
			SeqNum := client.serverSeqNum + 1
			s.serverseqAdd <- client
			size := len(writedata.payload)
			sum := CalculateChecksum(connId, SeqNum, size, payload)
			message := NewData(connId, SeqNum, size, payload, sum)
			s.unAckReq <- &unAckMsg{client, SeqNum, message, 0, 0, 1}
		}
	}
}

func (s *server) msgRoutine() {
	for {
		select {
		case <-s.closeMsg:
			return
		case msg := <-s.writeMsg:
			msg.client.msgList[msg.seq] = msg.message
		case msg := <-s.readMsg:
			message, ok := msg.client.msgList[msg.seq]
			if !ok {
				s.resMsg <- nil
			} else {
				s.resMsg <- message
			}
		}
	}
}

func (s *server) seqNumRoutine() {
	for {
		select {
		case <-s.closeSeqNum:
			return
		case client := <-s.seqAdd:
			client.SeqNum += 1
		case client := <-s.seqRead:
			s.seqRes <- client.SeqNum
		case client := <-s.serverseqAdd:
			client.serverSeqNum += 1
		case client := <-s.serverseqRead:
			s.serverseqRes <- client.serverSeqNum
		}
	}
}

func (s *server) ackRoutine() {
	for {
		select {
		case <-s.closeAck:
			return
		case msg := <-s.ackChan:
			ack := NewAck(msg.message.ConnID, msg.message.SeqNum)
			msg = &Msg{msg.addr, ack}
			s.writeChan <- msg
		}
	}
}

func (s *server) connectRoutine() {
	for {
		select {
		case <-s.closeConnect:
			return
		case msg := <-s.connectChan:
			dup := false
			for _, client := range s.clients {
				if client.addr == msg.addr {
					ack := NewAck(client.id, msg.message.SeqNum)
					msg = &Msg{msg.addr, ack}
					s.writeChan <- msg
					dup = true
					break
				}
			}
			if !dup {
				s.id += 1
				//fmt.Println("creat", s.id)
				s.clients[s.id] = &serverClient{s.id,
					msg.message.SeqNum,
					msg.addr,
					make(map[int]*Message),
					false,
					0,
					make([]*unAckMsg, 0),
					make([]*Msg, 0),
					0,
					false,
					0}
				ack := NewAck(s.id, msg.message.SeqNum)
				msg = &Msg{msg.addr, ack}
				s.writeChan <- msg
			}
		}
	}
}

func (s *server) readRoutine() {
	for {
		select {
		case <-s.closeRead:
			return
		default:
			buffer := make([]byte, Maxsize)
			n, addr, err := s.conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			var message Message
			err = json.Unmarshal(buffer[:n], &message)
			if err != nil {
				return
			}
			msg := &Msg{addr, &message}
			//fmt.Println(message.Payload)
			//fmt.Println(len(s.readChan), cap(s.readChan))
			s.readChan <- msg
			//fmt.Println("sent to read")
		}
	}
}

func (s *server) writeRoutine() {
	for {
		select {
		case msg := <-s.writeChan:
			client := s.clients[msg.message.ConnID]
			// fmt.Println(msg.addr, msg.message.Payload)
			buffer, err := json.Marshal(msg.message)
			if err != nil {
				continue
			}
			s.conn.WriteToUDP(buffer, msg.addr)
			//fmt.Println(msg.message.ConnID)
			client.sent = true
		case <-s.closeWrite:
			return
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	select {
	case message := <-s.outChan:
		return message.ConnID, message.Payload, nil
	} // Blocks indefinitely.
}

func (s *server) Write(connId int, payload []byte) error {
	if _, ok := s.clients[connId]; !ok {
		return errors.New("connId does not exist")
	}
	data := &writeData{connId, payload}
	s.preWriteChan <- data
	//fmt.Println("sent to write")
	return nil
}

func (s *server) CloseConn(connId int) error {
	if _, ok := s.clients[connId]; !ok {
		return errors.New("connId does not exist")
	}
	client := s.clients[connId]
	client.closed = true
	return nil
}

func (s *server) Close() error {
	s.conn.Close()
	s.closeMain <- true
	s.closeRead <- true
	s.closeWrite <- true
	s.closeBuffer <- true
	s.closeConnect <- true
	s.closeAck <- true
	s.closeSeqNum <- true
	s.closeMsg <- true
	s.closeunAckRoutine <- true
	s.closehandleWrite <- true
	s.closeEpoch <- true
	return nil
}
