package connect

import (
	"github.com/gorilla/websocket"
	"goim/proto"
	"net"
)

// 事实上，Channel它是一个用户连接会话
// 在同一个房间内，多个用户链接会话构成一个双向链表
type Channel struct {
	Room      *Room
	Next      *Channel
	Prev      *Channel
	broadcast chan *proto.Msg
	userId    int
	conn      *websocket.Conn
	connTcp   *net.TCPConn
}

func NewChannel(size int) (c *Channel) {
	c = new(Channel)
	c.broadcast = make(chan *proto.Msg, size)
	c.Next = nil
	c.Prev = nil
	return
}

// 将msg 传到broadcast中
func (ch *Channel) Push(msg *proto.Msg) (err error) {
	select {
	case ch.broadcast <- msg:
	default:
	}
	return
}
