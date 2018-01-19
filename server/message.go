package server

import (
	log "github.com/cihub/seelog"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

func processMessage(msg *Message) {
	c := msg.client
	ca := msg.packet
	if c == nil || ca == nil {
		return
	}

	switch ca.(type) {
	case *packets.ConnackPacket:
	case *packets.ConnectPacket:
	case *packets.PublishPacket:
		packet := ca.(*packets.PublishPacket)
		log.Debug("recv broker info: ", msg.packet.String(), "   brokerID = ", c.info.clientID)
		c.processPublish(packet)
	case *packets.PubackPacket:
	case *packets.PubrecPacket:
	case *packets.PubrelPacket:
	case *packets.PubcompPacket:
	case *packets.SubscribePacket:
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
	case *packets.UnsubackPacket:
	case *packets.PingreqPacket:
		c.processPing()
	case *packets.PingrespPacket:
	case *packets.DisconnectPacket:
		c.close()
	default:
		log.Error("Recv Unknow message. brokerID = ", c.info.clientID)
	}
}
