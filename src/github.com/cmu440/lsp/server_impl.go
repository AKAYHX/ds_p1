// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
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
}

type serverClient struct {
	id           int
	SeqNum       int
	addr         *lspnet.UDPAddr
	msgList      map[int]*Message
	closed       bool
	serverSeqNum int
	unAcked      []*Message
	buffer       []*Msg
	left         int
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
	client  *serverClient
	seq     int
	message *Message
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
			switch message.Type {
			case MsgConnect:
				s.connectChan <- msg
			case MsgAck, MsgCAck:
				//fmt.Println("2")
				s.unAckReq <- &unAckMsg{client, -1, message}
				//fmt.Println("222")
			case MsgData:
				s.ackChan <- msg
				seq := msg.message.SeqNum
				s.seqRead <- client
				currSeq := <-s.seqRes
				if seq == currSeq+1 {
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
			if seq == -1 {
				for i := 0; i < len(client.unAcked); i++ {
					//fmt.Println(i)
					if client.unAcked[i].SeqNum == message.SeqNum {
						//fmt.Println("!!")
						client.unAcked = append(client.unAcked[:i], client.unAcked[i+1:]...)
						//fmt.Println("callbuff")
						break
					}
				}
				//fmt.Println("wait1")
				if len(client.unAcked) == 0 {
					if len(client.buffer) > 0 {
						//fmt.Println("wait11")
						client.left = client.buffer[0].message.SeqNum - 1
						//fmt.Println("wait11 done")
					} else {
						//fmt.Println("wait12")
						s.serverseqRead <- client
						num := <-s.serverseqRes
						client.left = num
						//fmt.Println("wait12 done")
					}
				} else {
					//fmt.Println("wait13")
					client.left = client.unAcked[0].SeqNum
					//fmt.Println("wait13 done")
				}
				left := client.left
				for {
					if len(client.buffer) > 0 && len(client.unAcked) < s.MaxUnackedMessages && left+s.WindowSize >= client.buffer[0].message.SeqNum {
						s.writeChan <- client.buffer[0]
						client.buffer = client.buffer[1:]
					} else {
						break
					}
				}
				//fmt.Println("wait2 done")
			} else {
				//fmt.Println("wait3")
				left := client.left
				msg := &Msg{client.addr, message}
				//fmt.Println(client.addr, len(client.unAcked), s.MaxUnackedMessages, left, s.WindowSize, seq)
				if len(client.unAcked) < s.MaxUnackedMessages && left+s.WindowSize >= seq {
					//fmt.Println("towrite")
					client.unAcked = append(client.unAcked, message)
					s.writeChan <- msg
					//fmt.Println("wrote")
				} else {
					//fmt.Println("tobuff")
					client.buffer = append(client.buffer, msg)
				}
				//fmt.Println("wait3 done")
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
			s.unAckReq <- &unAckMsg{client, SeqNum, message}
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
			s.id += 1
			s.clients[s.id] = &serverClient{s.id,
				msg.message.SeqNum,
				msg.addr,
				make(map[int]*Message),
				false,
				0,
				make([]*Message, 0),
				make([]*Msg, 0),
				0}
			ack := NewAck(s.id, msg.message.SeqNum)
			msg = &Msg{msg.addr, ack}
			s.writeChan <- msg
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
			// fmt.Println(msg.addr, msg.message.Payload)
			buffer, err := json.Marshal(msg.message)
			if err != nil {
				continue
			}
			s.conn.WriteToUDP(buffer, msg.addr)
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
	return nil
}
