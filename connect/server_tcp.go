package connect

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/rpcxio/libkv/store"
	etcdV3 "github.com/rpcxio/rpcx-etcd/client"
	"github.com/sirupsen/logrus"
	"goim/api/rpc"
	"goim/config"
	"goim/pkg/stickpackage"
	"goim/proto"
	"goim/task"
	"net"
	"strings"
	"time"
)

const maxInt = 1<<31 - 1

func init() {
	rpc.InitLogicRpcClient()
}

// 初始化Rpc服务器
func (c *Connect) InitTcpServer() error {
	aTcpAddr := strings.Split(config.Conf.Connect.ConnectTcp.Bind, ",")
	cpuNum := config.Conf.Connect.ConnectBucket.CpuNum

	var (
		addr     *net.TCPAddr
		listener *net.TCPListener
		err      error
	)
	for _, ipPort := range aTcpAddr {
		//使用net.ResolveTCPAddr方法将IP和端口解析为TCP地址。
		if addr, err = net.ResolveTCPAddr("tcp", ipPort); err != nil {
			logrus.Errorf("server_tcp ResolveTCPAddr error:%s", err.Error())
			return err
		}
		if listener, err = net.ListenTCP("tcp", addr); err != nil {
			logrus.Errorf("net.ListenTCP(tcp, %s),error(%v)", ipPort, err)
			return err
		}
		//如果监听成功，则使用for循环启动多个goroutine，每个goroutine调用acceptTcp方法接受TCP连接。
		logrus.Infof("start tcp listen at:%s", ipPort)
		for i := 0; i < cpuNum; i++ {
			go c.acceptTcp(listener)
		}
	}
	return nil
}

// 处理TCP连接
func (c *Connect) acceptTcp(listener *net.TCPListener) {
	var (
		conn *net.TCPConn
		err  error
		r    int
	)
	ConnectTcpConfig := config.Conf.Connect.ConnectTcp
	for {
		//使用listener.AcceptTCP方法接受TCP连接。
		if conn, err = listener.AcceptTCP(); err != nil {
			logrus.Errorf("listener.Accept(\"%s\") error(%v)", listener.Addr().String(), err)
			return
		}
		// set keep alive，client==server ping package check
		//使用conn.SetKeepAlive方法设置TCP连接的保活机制，用于检测连接是否断开。
		if err = conn.SetKeepAlive(ConnectTcpConfig.KeepAlive); err != nil {
			logrus.Errorf("conn.SetKeepAlive() error:%s", err.Error())
			return
		}
		//使用conn.SetReadBuffer方法设置TCP连接的读缓冲区大小。
		if err = conn.SetReadBuffer(ConnectTcpConfig.ReadBufSize); err != nil {
			logrus.Errorf("conn.SetReadBuffer() error:%s", err.Error())
			return
		}
		//set SendBuf 写缓冲区大小
		if err = conn.SetWriteBuffer(ConnectTcpConfig.SendBuf); err != nil {
			logrus.Errorf("conn.SetWriteBuffer() error:%s", err.Error())
			return
		}

		// 检测服务器是否宕机
		go func() {
			buf := make([]byte, 1)
			_, err := conn.Read(buf)
			if err != nil {
				logrus.Errorf("Server is down: %s", err.Error())
				// 执行宕机处理逻辑
				NewServerId, err := GetOtherServer("tcp")
				if err != nil {
					logrus.Errorf("Get NewServerId fail %s ", err)
				}
				if NewServerId == "" {
					logrus.Panicf("No tcp NewServerId %s", NewServerId)
				}
				c.ServerId = NewServerId
			}
		}()

		go c.ServerTcp(DefaultServer, conn, r)
		if r++; r == maxInt {
			logrus.Infof("conn.acceptTcp num is:%d", r)
			r = 0
		}
	}
}

// 获取备用服务器serverid
func GetOtherServer(ServerType string) (string, error) {
	etcdConfigOption := &store.Config{
		ClientTLS:         nil,
		TLS:               nil,
		ConnectionTimeout: time.Duration(config.Conf.Common.CommonEtcd.ConnectionTimeout) * time.Second,
		Bucket:            "",
		PersistConnection: true,
		Username:          config.Conf.Common.CommonEtcd.UserName,
		Password:          config.Conf.Common.CommonEtcd.Password,
	}
	etcdConfig := config.Conf.Common.CommonEtcd
	d, e := etcdV3.NewEtcdV3Discovery(
		etcdConfig.BasePath,
		etcdConfig.ServerPathConnect,
		[]string{etcdConfig.Host},
		true,
		etcdConfigOption,
	)
	if e != nil {
		logrus.Fatalf("init task rpc etcd discovery client fail:%s", e.Error())
		return "", e
	}
	if len(d.GetServices()) <= 0 {
		logrus.Panicf("no etcd server find!")
		return "", errors.New("no etcd server find!")
	}

	for _, connectConf := range d.GetServices() {
		serverId := task.GetParamByKey(connectConf.Value, "serverId")
		if ServerType == task.GetParamByKey(connectConf.Value, "serverType") {
			if serverId == "" {
				continue
			} else {
				return serverId, nil
			}
		}
	}
	return "", nil
}

func (c *Connect) ServerTcp(server *Server, conn *net.TCPConn, r int) {
	var ch *Channel
	ch = NewChannel(server.Options.BroadcastSize)
	ch.connTcp = conn
	go c.writeDataToTcp(server, ch)
	go c.readDataFromTcp(server, ch)
}

// 用于从TCP连接中读取数据并进行处理
func (c *Connect) readDataFromTcp(s *Server, ch *Channel) {
	defer func() { //在函数执行完毕后关闭TCP连接，并执行一些清理操作。
		logrus.Infof("start exec disConnect ...")
		if ch.Room == nil || ch.userId == 0 {
			logrus.Infof("roomId and userId eq 0")
			_ = ch.connTcp.Close()
			return
		}
		logrus.Infof("exec disConnect ...")
		disConnectRequest := new(proto.DisConnectRequest)
		disConnectRequest.RoomId = ch.Room.Id
		disConnectRequest.UserId = ch.userId
		s.Bucket(ch.userId).DeleteChannel(ch)
		if err := s.operator.DisConnect(disConnectRequest); err != nil {
			logrus.Warnf("DisConnect rpc err :%s", err.Error())
		}
		if err := ch.connTcp.Close(); err != nil {
			logrus.Warnf("DisConnect close tcp conn err :%s", err.Error())
		}
		return
	}()

	//创建一个bufio.Scanner对象，用于从TCP连接中读取数据
	scannerPackage := bufio.NewScanner(ch.connTcp)
	//定义一个自定义的分隔函数，用于将收到的数据拆分成完整的消息包。
	scannerPackage.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if !atEOF && data[0] == 'v' { //data[0] == 'v' 用于检查第一个字节是否是标识 TCP 消息包的标志位
			if len(data) > stickpackage.TcpHeaderLength { //检查数据长度是否足够读取头部信息。
				packSumLength := int16(0)
				_ = binary.Read(bytes.NewBuffer(data[stickpackage.LengthStartIndex:stickpackage.LengthStopIndex]), binary.BigEndian, &packSumLength)
				//将读取到的长度 packSumLength 与数据长度比较,判断是否包含完整的一条消息
				if int(packSumLength) <= len(data) {
					return int(packSumLength), data[:packSumLength], nil
				}
			}
		}
		return
	})
	scanTimes := 0
	for {
		scanTimes++
		if scanTimes > 3 {
			logrus.Infof("scannedPack times is:%d", scanTimes)
			break
		}
		//使用for循环不断调用scannerPackage.Scan方法读取分隔后的消息包。
		for scannerPackage.Scan() {
			scannedPack := new(stickpackage.StickPackage)
			//解析数据
			err := scannedPack.Unpack(bytes.NewReader(scannerPackage.Bytes()))
			if err != nil {
				logrus.Errorf("scan tcp package err:%s", err.Error())
				break
			}
			//get a full package
			var connReq proto.ConnectRequest
			logrus.Infof("get a tcp message :%s", scannedPack)
			var rawTcpMsg proto.SendTcp
			if err := json.Unmarshal([]byte(scannedPack.Msg), &rawTcpMsg); err != nil {
				logrus.Errorf("tcp message struct %+v", rawTcpMsg)
				break
			}
			logrus.Infof("json unmarshal,raw tcp msg is:%+v", rawTcpMsg)
			if rawTcpMsg.AuthToken == "" {
				logrus.Errorf("tcp s.operator.Connect no authToken")
				return
			}
			if rawTcpMsg.RoomId <= 0 {
				logrus.Errorf("tcp roomId not allow lgt 0")
				return
			}

			switch rawTcpMsg.Op {
			case config.OpBuildTcpConn:
				//表示建立TCP连接请求，需要进行连接验证和将连接放入对应的Bucket中。
				connReq.AuthToken = rawTcpMsg.AuthToken
				connReq.RoomId = rawTcpMsg.RoomId

				//connReq.ServerId = config.Conf.Connect.ConnectTcp.ServerId
				connReq.ServerId = c.ServerId
				userId, err := s.operator.Connect(&connReq)
				logrus.Infof("tcp s.operator.Connect userId is :%d", userId)
				if err != nil {
					logrus.Errorf("tcp s.operator.Connect error %s", err.Error())
					return
				}
				if userId == 0 {
					logrus.Error("tcp Invalid AuthToken ,userId empty")
					return
				}
				b := s.Bucket(userId)
				//insert into a bucket
				//信息存入到桶的chs和rooms中
				err = b.Put(userId, connReq.RoomId, ch)
				if err != nil {
					logrus.Errorf("tcp conn put room err: %s", err.Error())
					_ = ch.connTcp.Close()
					return
				}
			case config.OpRoomSend:
				//表示向房间发送消息，需要调用相关的RPC接口进行消息推送操作。
				//send tcp msg to room
				req := &proto.Send{
					Msg:          rawTcpMsg.Msg,
					FromUserId:   rawTcpMsg.FromUserId,
					FromUserName: rawTcpMsg.FromUserName,
					RoomId:       rawTcpMsg.RoomId,
					Op:           config.OpRoomSend,
				}
				code, msg := rpc.RpcLogicObj.PushRoom(req)
				logrus.Infof("tcp conn push msg to room,err code is:%d,err msg is:%s", code, msg)
			case config.OpRoomSend_File:
				//表示向房间发送消息，需要调用相关的RPC接口进行消息推送操作。
				//send tcp msg to room
				req := &proto.Send{
					Msg:          rawTcpMsg.Msg,
					FromUserId:   rawTcpMsg.FromUserId,
					FromUserName: rawTcpMsg.FromUserName,
					RoomId:       rawTcpMsg.RoomId,
					Op:           config.OpRoomSend_File,
				}
				code, msg := rpc.RpcLogicObj.PushRoom_File(req)
				logrus.Infof("tcp conn push msg to room,err code is:%d,err msg is:%s", code, msg)
			}
		}
		if err := scannerPackage.Err(); err != nil {
			logrus.Errorf("tcp get a err package:%s", err.Error())
			return
		}
	}
}

// 从服务器向TCP连接写入数据
func (c *Connect) writeDataToTcp(s *Server, ch *Channel) {
	//ping time default 54s创建一个定时器,每54s发送一次ping消息。
	ticker := time.NewTicker(DefaultServer.Options.PingPeriod)
	defer func() { //使用defer放在函数结束关闭定时器和TCP连接。
		ticker.Stop()
		_ = ch.connTcp.Close()
		return
	}()
	//定义一个StickPackage消息对象。
	pack := stickpackage.StickPackage{
		Version: stickpackage.VersionContent,
	}

	for {
		select { //从中接收到其他进程发送来的消息,写入TCP连接
		case message, ok := <-ch.broadcast:
			if !ok {
				_ = ch.connTcp.Close()
				return
			}
			//设置消息的长度
			pack.Msg = message.Body
			pack.Length = pack.GetPackageLength()
			//send msg
			logrus.Infof("send tcp msg to conn:%s", pack.String())
			//将pack打包写到TCP连接,错误直接返回。
			if err := pack.Pack(ch.connTcp); err != nil {
				logrus.Errorf("connTcp.write message err:%s", err.Error())
				return
			}
		case <-ticker.C: //定时器触发，写入ping消息
			logrus.Infof("connTcp.ping message,send")
			//send a ping msg ,if error , return
			pack.Msg = []byte("ping msg")
			pack.Length = pack.GetPackageLength()
			if err := pack.Pack(ch.connTcp); err != nil {
				//send ping msg to tcp conn
				return
			}
		}
	}

}
