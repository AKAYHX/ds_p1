// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

const Maxsize = 2000

type server struct {
	conn               *lspnet.UDPConn
	addr               *lspnet.UDPAddr
	readChan           chan *readMsg
	writeChan          chan *Msg
	preWriteChan       chan *writeData
	outChan            chan *Message
	connectChan        chan *readMsg
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
	closeclient        chan int
	closed             bool
	closeReply         chan bool
	closeunAck         chan bool
	hadclient          bool
	buffer             []*Message
	readRequest        chan bool
	readResponse       chan *Message
	closebeforeRead    chan bool
	closeclientRoutine chan bool
	clientModify       chan *clientInfo
	clientRes          chan int
	clientsChan        chan *serverClient
	epochChan          chan *serverClient
	towriteChan        chan *unAckMsg
	closeconnChan      chan bool
	getClient          chan int
	returnClient       chan *serverClient
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

type clientInfo struct {
	client *serverClient
	action string
}

type readMsg struct {
	addr    *lspnet.UDPAddr
	message *Message
}

type Msg struct {
	client  *serverClient
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
		addr,
		make(chan *readMsg),
		make(chan *Msg),
		make(chan *writeData),
		make(chan *Message),
		make(chan *readMsg),
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
		make(chan int),
		false,
		make(chan bool),
		make(chan bool),
		false,
		make([]*Message, 0),
		make(chan bool),
		make(chan *Message),
		make(chan bool),
		make(chan bool),
		make(chan *clientInfo),
		make(chan int),
		make(chan *serverClient),
		make(chan *serverClient),
		make(chan *unAckMsg),
		make(chan bool),
		make(chan int),
		make(chan *serverClient),
	}
	go s.mainRoutine()
	go s.readRoutine()
	go s.writeRoutine()
	go s.seqNumRoutine()
	go s.msgRoutine()
	go s.handleWrite()
	go s.beforeRead()
	return s, nil
}

func (s *server) mainRoutine() {
	ticker := time.NewTicker(time.Duration(s.EpochMillis * 1000000))
	for {
		select {
		case <-s.closeMain:
			ticker.Stop()
			return
		case id := <-s.getClient:
			s.returnClient <- s.clients[id]
		case req := <-s.towriteChan:
			client := req.client
			message := req.message
			SeqNum := req.seq
			left := client.left
			msg := &Msg{client, client.addr, message}
			//fmt.Println("!", message.String())
			if len(client.unAcked) < s.MaxUnackedMessages && left+s.WindowSize >= SeqNum {
				client.unAcked = append(client.unAcked, req)
				if client != nil {
					client.sent = true
				}
				s.writeChan <- msg
			} else {
				//fmt.Println(len(client.unAcked), s.MaxUnackedMessages, left, s.WindowSize, seq)
				//fmt.Println("toBuffer", message.SeqNum)
				client.buffer = append(client.buffer, msg)
			}
			if client.closed && len(client.unAcked) == 0 && len(client.buffer) == 0 {
				delete(s.clients, client.id)
			}
		case <-s.closeunAckRoutine:
			for _, client := range s.clients {
				if len(client.unAcked) == 0 && len(client.buffer) == 0 {
					delete(s.clients, client.id)
				}
			}
			if len(s.clients) == 0 {
				s.closeReply <- true
			} else {
				s.closeReply <- false
			}
		case id := <-s.closeclient:
			client := s.clients[id]
			if client == nil {
				s.closeconnChan <- false
				continue
			}
			client.closed = true
			if len(client.unAcked) == 0 && len(client.buffer) == 0 {
				delete(s.clients, client.id)
			}
			s.closeconnChan <- true
		case <-ticker.C:
			for _, client := range s.clients {
				client.heard += 1
				if client.heard >= s.EpochLimit {
					delete(s.clients, client.id)
					s.outChan <- nil
					continue
				}
				for _, msg := range client.unAcked {
					if msg.epoch == msg.CurrentBackoff {
						if client != nil {
							client.sent = true
						}
						s.writeChan <- &Msg{client, client.addr, msg.message}
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
					if client != nil {
						client.sent = true
					}
					s.writeChan <- &Msg{client, client.addr, ack}
				}
			}
		case msg := <-s.readChan:
			//fmt.Println("readChan")
			message := msg.message
			id := msg.message.ConnID
			client := s.clients[id]
			if client != nil {
				client.heard = 0
			}
			switch message.Type {
			case MsgConnect:
				dup := false
				for _, client := range s.clients {
					if client.addr == msg.addr {
						ack := NewAck(client.id, msg.message.SeqNum)
						newmsg := &Msg{client, msg.addr, ack}
						if client != nil {
							client.sent = true
						}
						s.writeChan <- newmsg
						dup = true
						break
					}
				}
				if !dup {
					s.id += 1
					client := &serverClient{s.id,
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
					s.clients[client.id] = client
					s.hadclient = true
					ack := NewAck(s.id, msg.message.SeqNum)
					newmsg := &Msg{client, msg.addr, ack}
					if client != nil {
						client.sent = true
					}
					s.writeChan <- newmsg
				}
			case MsgAck, MsgCAck:
				//fmt.Println("ack?")
				if client == nil {
					continue
				}
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
						if client != nil {
							client.sent = true
						}
						s.writeChan <- client.buffer[0]
						client.unAcked = append(client.unAcked, &unAckMsg{client, client.buffer[0].message.SeqNum, client.buffer[0].message, 0, 0, 1})
						client.buffer = client.buffer[1:]
					} else {
						break
					}
				}
				if client.closed && len(client.unAcked) == 0 && len(client.buffer) == 0 {
					delete(s.clients, client.id)
				}
			case MsgData:
				//fmt.Println("msg?")
				if message.Size != len(message.Payload) {
					if message.Size > len(message.Payload) {
						continue
					}
					message.Payload = message.Payload[:message.Size]
				}
				if message.Checksum != CalculateChecksum(message.ConnID, message.SeqNum, message.Size, message.Payload) {
					continue
				}
				ack := NewAck(msg.message.ConnID, msg.message.SeqNum)
				newmsg := &Msg{client, msg.addr, ack}
				if client != nil {
					client.sent = true
					s.writeChan <- newmsg
					seq := msg.message.SeqNum
					s.seqRead <- client
					currSeq := <-s.seqRes
					//fmt.Println("server: ", message)
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
				//fmt.Println("msg!")
			}
			//fmt.Println("processed")
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
			s.getClient <- connId
			client := <-s.returnClient
			if client == nil {
				continue
			}
			s.serverseqAdd <- client
			s.serverseqRead <- client
			SeqNum := <-s.serverseqRes
			size := len(writedata.payload)
			sum := CalculateChecksum(connId, SeqNum, size, payload)
			message := NewData(connId, SeqNum, size, payload, sum)
			//fmt.Println("towrite: ", message.String())
			req := &unAckMsg{client, SeqNum, message, 0, 0, 1}
			s.towriteChan <- req
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

func (s *server) readRoutine() {
	for {
		select {
		case <-s.closeRead:
			return
		default:
			buffer := make([]byte, Maxsize)
			n, addr, err := s.conn.ReadFromUDP(buffer)
			if err != nil {
				continue
			}
			var message Message
			err = json.Unmarshal(buffer[:n], &message)
			//fmt.Println("!server received: ", message.String())
			if err != nil {
				return
			}
			msg := &readMsg{addr, &message}
			s.readChan <- msg
			//fmt.Println("!sent to read chan")
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

func (s *server) beforeRead() {
	for {
		select {
		case <-s.closebeforeRead:
			return
		case message := <-s.outChan:
			s.buffer = append(s.buffer, message)
		case <-s.readRequest:
			if len(s.buffer) > 0 {
				s.readResponse <- s.buffer[0]
				s.buffer = s.buffer[1:]
			} else {
				select {
				case <-s.closebeforeRead:
					return
				case msg := <-s.outChan:
					s.readResponse <- msg
				}
			}
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	if s.closed {
		return 0, nil, errors.New("closed")
	}
	s.readRequest <- true
	message := <-s.readResponse
	if message == nil {
		return 0, nil, errors.New("client closed")
	}
	//fmt.Println("!server read: ", message.String())
	return message.ConnID, message.Payload, nil
}

func (s *server) Write(connId int, payload []byte) error {
	if s.closed {
		return errors.New("closed")
	}
	if _, ok := s.clients[connId]; !ok {
		return errors.New("connId does not exist")
	}
	data := &writeData{connId, payload}
	s.preWriteChan <- data
	//fmt.Println("sent to write")
	return nil
}

func (s *server) CloseConn(connId int) error {
	//fmt.Println(connId)
	//time.Sleep(time.Duration(s.EpochMillis * 1000000))
	s.closeclient <- connId
	res := <-s.closeconnChan
	if !res {
		return errors.New("no client")
	}
	//fmt.Println(connId)
	return nil
}

func (s *server) Close() error {
	//fmt.Println("called close")
	s.closed = true
	closed := false
	for {
		time.Sleep(time.Duration(s.EpochMillis * 1000000))
		s.closeunAckRoutine <- true
		closed = <-s.closeReply
		if closed {
			break
		}
	}
	//fmt.Println("done1")
	s.conn.Close()
	s.closebeforeRead <- true
	s.closeRead <- true
	//fmt.Println("done2")
	s.closeMain <- true
	//fmt.Println("done3")
	s.closeWrite <- true
	//fmt.Println("done6")
	s.closeSeqNum <- true
	//fmt.Println("done7")
	s.closeMsg <- true
	//fmt.Println("done8")
	s.closehandleWrite <- true
	//fmt.Println("!!!closed")
	return nil
}
