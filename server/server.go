package server

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

type Message struct {
	client *client
	packet packets.ControlPacket
}

type server struct {
	port      string
	infoTopic string
	info      map[string]string
	mu        sync.Mutex
	clients   sync.Map
	pool      chan *Message
}

func NewServer(port, topic string) *server {
	return &server{
		port:      port,
		infoTopic: topic,
		info:      make(map[string]string),
		pool:      make(chan *Message, 100),
	}
}

func (s *server) handleMessage() {
	if s.pool == nil {
		s.pool = make(chan *Message, 100)
	}

	for {
		select {
		case msg := <-s.pool:
			processMessage(msg)
		}
	}

}

func (s *server) Start() {

	go s.handleMessage()

	addr := "0.0.0.0:" + s.port
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("New Listener error: ", err)
		return
	}
	log.Info("start listener on ", addr)
	var tempDelay time.Duration
	for {
		conn, err := l.Accept()
		if err != nil {
			// check if the error is a temporary error
			if acceptErr, ok := err.(net.Error); ok && acceptErr.Temporary() {
				if 0 == tempDelay {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}

				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}

				log.Error("Accept error %s , retry after %d ms", acceptErr.Error(), tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			log.Error("accept routine quit.error:%s", err.Error())
			// t.listener = nil
			return
		}
		go s.handleConnection(conn)
	}
}

func (s *server) handleConnection(conn net.Conn) {
	//process connect packet
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		log.Error("client read connect packet error: ", err)
		return
	}
	if packet == nil {
		log.Error("received nil packet")
		return
	}
	msg, ok := packet.(*packets.ConnectPacket)
	if !ok {
		log.Error("received msg that was not Connect")
		return
	}

	log.Info("receive conn from client , clientID = ", msg.ClientIdentifier, " keepAlive: ", msg.Keepalive)

	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.ReturnCode = packets.Accepted
	connack.SessionPresent = msg.CleanSession

	err = connack.Write(conn)
	if err != nil {
		log.Error("send to client connack error, clientID = ", msg.ClientIdentifier, " error : ", err)
		return
	}

	info := info{
		clientID:  msg.ClientIdentifier,
		username:  msg.Username,
		password:  msg.Password,
		keepAlive: msg.Keepalive,
	}

	c := &client{
		srv:  s,
		conn: conn,
		info: info,
	}

	c.init()

	cid := c.info.clientID
	old, exist := s.clients.Load(cid)
	if exist {
		log.Warn("client exist, close old, clientID = ", cid)
		ol, ok := old.(*client)
		if ok {
			ol.close()
		}
	}
	s.clients.Store(cid, c)
	pool := s.pool
	go s.sendBrokerInfo(c)

	go c.readLoop(pool)

}

func (s *server) removeBrokerInfo(id string) map[string]string {

	s.mu.Lock()
	delete(s.info, id)
	s.mu.Unlock()

	return s.info
}

func (s *server) getBrokerInfo() map[string]string {
	return s.info
}

func (s *server) addBrokerInfo(id, url string) map[string]string {

	s.mu.Lock()
	s.info[id] = url
	s.mu.Unlock()

	return s.info
}

func (s *server) broadcastBorkerInfo(data map[string]string) {
	info := &BrokerInfo{
		Data: data,
	}

	buf, err := json.Marshal(info)
	if err != nil {
		log.Error("marshal info error: ", err)
		return
	}

	infoPackage := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	infoPackage.Qos = 0
	infoPackage.TopicName = s.infoTopic
	infoPackage.Retain = false
	infoPackage.Payload = buf
	s.clients.Range(func(key, value interface{}) bool {
		r, ok := value.(*client)
		if ok {
			r.writerPacket(infoPackage)
		}
		return true
	})

	return
}

func (s *server) sendBrokerInfo(c *client) {
	data := s.getBrokerInfo()

	if len(data) > 0 {
		info := &BrokerInfo{
			Data: data,
		}

		buf, err := json.Marshal(info)
		if err != nil {
			log.Error("marshal info error: ", err)
			c.close()
			return
		}

		infoPackage := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		infoPackage.Qos = 0
		infoPackage.TopicName = s.infoTopic
		infoPackage.Retain = false
		infoPackage.Payload = buf
		e := c.writerPacket(infoPackage)
		if e != nil {
			log.Error("send broker info error")
		}
	}
}
