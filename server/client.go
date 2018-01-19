package server

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	log "github.com/cihub/seelog"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	Connected    = 1
	Disconnected = 2
)

type client struct {
	conn   net.Conn
	mu     sync.Mutex
	srv    *server
	status int
	closed chan int
	info   info
}

type info struct {
	clientID  string
	username  string
	password  []byte
	keepAlive uint16
	localIP   string
	remoteIP  string
}

var (
	DisconnectdPacket = packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
)

func (c *client) init() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = Connected
	c.closed = make(chan int, 1)
	c.info.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.info.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]
}

func (c *client) keepAlive(ch chan int, msgPool chan *Message) {
	keepalive := time.Duration(c.info.keepAlive * 3 / 2)
	timeTicker := time.NewTimer(keepalive * time.Second)

	for {
		select {
		case <-ch:
			timeTicker.Reset(keepalive * time.Second)
		case <-timeTicker.C:
			log.Errorf("Client %s has exceeded timeout, disconnecting.\n", c.info.clientID)
			msg := &Message{
				client: c,
				packet: DisconnectdPacket,
			}
			msgPool <- msg
			timeTicker.Stop()
			return
		case _, ok := <-c.closed:
			if !ok {
				return
			}
		}
	}

}

func (c *client) readLoop(msgPool chan *Message) {

	if c.status == Disconnected {
		log.Error("broker disconnected", "  brokerID = ", c.info.clientID)
		return
	}
	nc := c.conn

	if nc == nil || msgPool == nil {
		return
	}

	ch := make(chan int, 1000)
	go c.keepAlive(ch, msgPool)

	for {
		packet, err := packets.ReadPacket(nc)
		if err != nil {
			log.Error("read packet err: ", err, "   brokerID = ", c.info.clientID)
			break
		}
		ch <- 1

		msg := &Message{
			client: c,
			packet: packet,
		}
		msgPool <- msg

		//then read buffer
		if nil == nc {
			break
		}

	}
	msg := &Message{
		client: c,
		packet: DisconnectdPacket,
	}
	msgPool <- msg
}

func (c *client) close() {
	c.mu.Lock()
	if c.status == Disconnected {
		c.mu.Unlock()
		return
	}

	c.status = Disconnected

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
	cid := c.info.clientID

	log.Info("client close with brokerID = ", cid)

	close(c.closed)

	s := c.srv
	if s != nil {
		log.Info("client close remove ", cid)
		s.clients.Delete(cid)
		data := s.removeBrokerInfo(cid)
		s.broadcastBorkerInfo(data)

	}

}

type BrokerInfo struct {
	Data map[string]string `json:"data"`
}

func (c *client) processPublish(packet *packets.PublishPacket) {
	srv := c.srv
	if srv == nil {
		log.Error("server is null ,return ")
		return
	}

	// cid := c.info.clientID

	topic := packet.TopicName
	if topic != srv.infoTopic {
		log.Debug("recv unknow message")
		return
	}

	js, e := simplejson.NewJson(packet.Payload)
	if e != nil {
		log.Error("palyload format is error: ", e)
		return
	}

	bid := js.Get("brokerID").MustString()
	burl := js.Get("brokerUrl").MustString()

	if bid == "" || burl == "" {
		log.Debug("broker info is null")
		return
	}

	data := srv.addBrokerInfo(bid, burl)

	if len(data) > 0 {
		srv.broadcastBorkerInfo(data)
	}

}

func (c *client) processPing() {
	resp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	err := c.writerPacket(resp)
	if err != nil {
		log.Error("send PingResponse error, ", err, "   brokerID = ", c.info.clientID)
		return
	}
}

func (c *client) writerPacket(packet packets.ControlPacket) error {
	if packet == nil {
		return nil
	}

	c.mu.Lock()
	if c.conn == nil {
		c.mu.Unlock()
		return errors.New("conn is nil")
	}
	err := packet.Write(c.conn)
	c.mu.Unlock()
	return err
}
