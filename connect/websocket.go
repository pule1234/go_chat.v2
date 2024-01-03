package connect

import (
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"goim/config"
	"net/http"
	"time"
)

func (c *Connect) InitWebsocket() error {
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		//请求升级为Websocket连接
		c.serveWs(DefaultServer, w, r)
	})
	err := http.ListenAndServe(config.Conf.Connect.ConnectWebsocket.Bind, nil)
	return err
}

func (c *Connect) serveWs(server *Server, w http.ResponseWriter, r *http.Request) {
	//创建一个websocket.Upgrader类型的变量upGrader，用于将HTTP连接升级为Websocket连接。
	var upGrader = websocket.Upgrader{
		ReadBufferSize:  server.Options.ReadBufferSize,
		WriteBufferSize: server.Options.WriteBufferSize,
	}
	//cross origin domain support CheckOrigin设置为一个匿名函数，用于处理跨域请求，始终返回true。
	upGrader.CheckOrigin = func(r *http.Request) bool { return true }
	conn, err := upGrader.Upgrade(w, r, nil)
	if err != nil {
		logrus.Errorf("serverWs err:%s", err.Error())
		return
	}
	var ch *Channel
	//default broadcast size eq 512//用于管理Websocket连接的广播消息
	ch = NewChannel(server.Options.BroadcastSize)
	//设置为升级后的Websocket连接
	ch.conn = conn

	go func() {
		time.Sleep(5 * time.Second)
		if isServerDown(ch.conn) {
			logrus.Warn("Server is down, switching to backup server...")
			NewServerId, err := GetOtherServer("ws")
			if err != nil {
				logrus.Errorf("Get ws NewServerId fail err : %s", err)
			}
			if NewServerId == "" {
				logrus.Panicf("No ws NewServerId %s", NewServerId)
			}
			c.ServerId = NewServerId
		}
	}()

	//send data to websocket conn  启动一个协程，向websocket中读写信息
	go server.writePump(ch, c)
	//get data from websocket conn
	go server.readPump(ch, c)
}

func isServerDown(conn *websocket.Conn) bool {
	// 发送心跳消息
	err := conn.WriteMessage(websocket.TextMessage, []byte("ping"))
	if err != nil {
		// 发送心跳消息失败，表示服务器宕机
		return true
	}
	return false
}
