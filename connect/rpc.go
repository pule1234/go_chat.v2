package connect

import (
	"context"
	"errors"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"github.com/rpcxio/libkv/store"
	etcdV3 "github.com/rpcxio/rpcx-etcd/client"
	"github.com/rpcxio/rpcx-etcd/serverplugin"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/server"
	"goim/config"
	"goim/proto"
	"goim/tools"
	"strings"
	"sync"
	"time"
)

var logicRpcClient client.XClient
var once sync.Once

type RpcConnect struct {
}

type RpcConnectPush struct {
}

// 初始化rpc逻辑客户端
func (c *Connect) InitLogicRpcClient() (err error) {
	etcdConfigOption := &store.Config{
		ClientTLS: nil,
		TLS:       nil,
		//连接的超时时间
		ConnectionTimeout: time.Duration(config.Conf.Common.CommonEtcd.ConnectionTimeout) * time.Second,
		//Bucket设置为空，表示不使用Bucket
		Bucket:            "",
		PersistConnection: true, // 持久化连接
		Username:          config.Conf.Common.CommonEtcd.UserName,
		Password:          config.Conf.Common.CommonEtcd.Password,
	}

	once.Do(func() {
		//创建d用于进行etcd服务发现
		d, e := etcdV3.NewEtcdV3Discovery(
			//基础路径，存储服务发现相关的信息
			config.Conf.Common.CommonEtcd.BasePath,
			//存储逻辑服务器信息的路径
			config.Conf.Common.CommonEtcd.ServerPathLogic,
			[]string{config.Conf.Common.CommonEtcd.Host},
			true, //自动同步
			etcdConfigOption,
		)
		if e != nil {
			logrus.Fatalf("init connect rpc etcd discovery client fail:%s", e.Error())
		}
		//创建一个client.NewXClient类型的变量logicRpcClient，用于进行逻辑RPC调用。
		logicRpcClient = client.NewXClient(config.Conf.Common.CommonEtcd.ServerPathLogic, client.Failtry, client.RandomSelect, d, client.DefaultOption)
	})
	if logicRpcClient == nil {
		return errors.New("get rpc client nil")
	}
	return
}

func (rpc *RpcConnect) Connect(connReq *proto.ConnectRequest) (uid int, err error) {
	reply := &proto.ConnectReply{}
	err = logicRpcClient.Call(context.Background(), "Connect", connReq, reply)
	if err != nil {
		logrus.Fatalf("failed to call: %v", err)
	}
	uid = reply.UserId
	logrus.Infof("connect logic userId :%d", reply.UserId)
	return
}

func (rpc *RpcConnect) DisConnect(disConnReq *proto.DisConnectRequest) (err error) {
	reply := &proto.DisConnectReply{}
	err = logicRpcClient.Call(context.Background(), "DisConnect", disConnReq, reply)
	if err != nil {
		logrus.Fatalf("failed to call: %v", err)
	}
	return
}

func (c *Connect) InitConnectWebsocketRpcServer() (err error) {
	var network, addr string
	connectRpcAddress := strings.Split(config.Conf.Connect.ConnectRpcAddressWebSockts.Address, ",")
	for _, bind := range connectRpcAddress {
		if network, addr, err = tools.ParseNetwork(bind); err != nil {
			logrus.Panicf("InitConnectWebsocketRpcServer ParseNetwork error : %s", err)
		}
		logrus.Infof("Connect start run at-->%s:%s", network, addr)
		//创建一个Connect Websockets Rpc服务器，并将其注册到Etcd中
		go c.createConnectWebsocktsRpcServer(network, addr)
	}
	return
}

// Connect Websockets RPC服务器
func (c *Connect) createConnectWebsocktsRpcServer(network, addr string) {
	//用于创建rpc服务器
	s := server.NewServer()
	//向rpc服务器中添加插件
	addRegistryPlugin(s, network, addr)
	//注册一个RPC服务   new(RpcConnectPush)表示RPC服务对象。  将serverid 注册在其中
	s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathConnect, new(RpcConnect), fmt.Sprintf("serverId=%s&serverType=ws", c.ServerId))
	//回调函数，在服务器关闭时执行
	s.RegisterOnShutdown(func(s *server.Server) {
		s.UnregisterAll()
	})
	s.Serve(network, addr)
}

func addRegistryPlugin(s *server.Server, network, addr string) {
	r := &serverplugin.EtcdV3RegisterPlugin{
		//表示rpc服务器的地址
		ServiceAddress: network + "@" + addr,
		//表示Etcd服务器的地址
		EtcdServers: []string{config.Conf.Common.CommonEtcd.Host},
		//表示注册的根路径
		BasePath:       config.Conf.Common.CommonEtcd.BasePath,
		Metrics:        metrics.NewRegistry(),
		UpdateInterval: time.Minute,
	}
	//启动注册插件
	err := r.Start()
	if err != nil {
		logrus.Fatal(err)
	}
	s.Plugins.Add(r)
}

func (c *Connect) InitConnectTcpRpcServer() (err error) {
	var network, addr string

	connectRpcAddress := strings.Split(config.Conf.Connect.ConnectRpcAddressTcp.Address, ",")
	for _, bind := range connectRpcAddress {
		if network, addr, err = tools.ParseNetwork(bind); err != nil {
			logrus.Info("bind ================", bind)
			logrus.Panicf("InitConnectTcpRpcServer ParseNetwork error : %s  ||||||", err)
		}
		logrus.Infof("Connect start run at-->%s:%s", network, addr)
		go c.createConnectTcpRpcServer(network, addr)
	}
	return
}

func (c *Connect) createConnectTcpRpcServer(network, addr string) {
	s := server.NewServer()
	addRegistryPlugin(s, network, addr)

	s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathConnect, new(RpcConnectPush), fmt.Sprintf("serverId=%s&serverType=tcp", c.ServerId))

	s.RegisterOnShutdown(func(s *server.Server) {
		s.UnregisterAll()
	})
	s.Serve(network, addr)
}

func (rpc *RpcConnectPush) PushSingleMsg(ctx context.Context, pushMsgReq *proto.PushMsgRequest, successReply *proto.SuccessReply) (err error) {
	var (
		bucket  *Bucket
		channel *Channel
	)
	logrus.Info("rpc PushMsg :%v ", pushMsgReq)
	if pushMsgReq == nil {
		logrus.Errorf("rpc PushSingleMsg() args:(%v)", pushMsgReq)
		return
	}

	// 按照userid获取这个桶
	bucket = DefaultServer.Bucket(pushMsgReq.UserId)
	// 获取到会话连接
	if channel = bucket.Channel(pushMsgReq.UserId); channel != nil {
		err = channel.Push(&pushMsgReq.Msg)
		logrus.Infof("DefaultServer Channel err nil ,args: %v", pushMsgReq)
		return
	}
	successReply.Code = config.SuccessReplyCode
	successReply.Msg = config.SuccessReplyMsg
	logrus.Infof("successReply:%v", successReply)
	return
}

func (rpc *RpcConnectPush) PushRoomMsg(ctx context.Context, pushRoomMsgReq *proto.PushRoomMsgRequest, successReply *proto.SuccessReply) (err error) {
	successReply.Code = config.SuccessReplyCode
	successReply.Msg = config.SuccessReplyMsg
	logrus.Infof("PushRoomMsg msg %+v", pushRoomMsgReq)
	for _, bucket := range DefaultServer.Buckets {
		bucket.BroadcastRoom(pushRoomMsgReq)
	}
	return
}

func (rpc *RpcConnectPush) PushRoomCount(ctx context.Context, pushRoomMsgReq *proto.PushRoomMsgRequest, successReply *proto.SuccessReply) (err error) {
	successReply.Code = config.SuccessReplyCode
	successReply.Msg = config.SuccessReplyMsg
	logrus.Infof("PushRoomCount msg %v", pushRoomMsgReq)
	for _, bucket := range DefaultServer.Buckets {
		bucket.BroadcastRoom(pushRoomMsgReq)
	}
	return
}

func (rpc *RpcConnectPush) PushRoomInfo(ctx context.Context, pushRoomMsgReq *proto.PushRoomMsgRequest, successReply *proto.SuccessReply) (err error) {
	successReply.Code = config.SuccessReplyCode
	successReply.Msg = config.SuccessReplyMsg
	logrus.Infof("connect,PushRoomInfo msg %+v", pushRoomMsgReq)
	for _, bucket := range DefaultServer.Buckets {
		bucket.BroadcastRoom(pushRoomMsgReq)
	}
	return
}
