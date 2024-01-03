package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rpcxio/libkv/store"
	etcdV3 "github.com/rpcxio/rpcx-etcd/client"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/rpcx/client"
	"goim/config"
	"goim/logic"
	"goim/proto"
	"goim/tools"
	"strings"
	"sync"
	"time"
)

var RClient = &RpcConnectClient{
	ServerInsMap: make(map[string][]Instance),
	IndexMap:     make(map[string]int),
}

type Instance struct {
	ServerType string
	ServerId   string
	Client     client.XClient
}

type RpcConnectClient struct {
	lock         sync.Mutex
	ServerInsMap map[string][]Instance //serverId--[]ins存储每个serverId对应的多个RPC客户端实例
	IndexMap     map[string]int        //serverId--index存储每个serverId对应的当前使用索引
}

func (rc *RpcConnectClient) GetAllConnectTypeRpcClient() (rpcClientList []client.XClient) {
	for serverId, _ := range rc.ServerInsMap {
		//	//根据serverid选择rpc客户端
		c, err := rc.GetRpcClientByServerId(serverId)
		if err != nil {
			logrus.Infof("GetAllConnectTypeRpcClient err:%s", err.Error())
			continue
		}
		rpcClientList = append(rpcClientList, c)
	}
	return
}

func (rc *RpcConnectClient) GetRpcClientByServerId(serverId string) (c client.XClient, err error) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	//检查rc.IndexMap中是否存在给定的serverId。如果不存在，则将其初始化为0
	if _, ok := rc.ServerInsMap[serverId]; !ok || len(rc.ServerInsMap[serverId]) <= 0 {
		return nil, errors.New("no connect layer ip:" + serverId)
	}

	if _, ok := rc.IndexMap[serverId]; !ok {
		rc.IndexMap = map[string]int{
			serverId: 0,
		}
	}
	//使用取模运算来实现循环轮询选择RPC客户端。
	idx := rc.IndexMap[serverId] % len(rc.ServerInsMap[serverId])
	//根据获取到的idx从rc.ServerInsMap[serverId]中获取对应的RPC客户端实例ins
	ins := rc.ServerInsMap[serverId][idx]
	//更新rc.IndexMap[serverId]的值，使其指向下一个RPC客户端，实现轮询选择。
	rc.IndexMap[serverId] = (rc.IndexMap[serverId] + 1) % len(rc.ServerInsMap[serverId])
	return ins.Client, nil
}

// 遍历列表中的每个RPC客户端
// ，通过RPC调用远程服务的PushRoomMsg方法，
// 将pushRoomMsgReq作为参数传递给每个客户端，并获取每个客户端的回复结果。
func (task *Task) broadcastRoomToConnect(roomId int, msg []byte) {
	pushRoomMsgReq := &proto.PushRoomMsgRequest{
		RoomId: roomId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpRoomSend,
			SeqId:     tools.GetSnowflakeId(),
			Body:      msg,
		},
	}
	reply := &proto.SuccessReply{}
	//获取所有的rpc客户端     使用RPC广播消息给所有连接
	rpcList := RClient.GetAllConnectTypeRpcClient()
	for _, rpc := range rpcList {
		logrus.Infof("broadcastRoomToConnect rpc  %v", rpc)
		//通过RPC调用远程服务的PushRoomMsg方法
		//，传递pushRoomMsgReq作为参数，
		//并将回复结果保存在reply中
		rpc.Call(context.Background(), "PushRoomMsg", pushRoomMsgReq, reply)
		logrus.Infof("reply %s", reply.Msg)
	}
}

func (task *Task) broadcastRoomToConnect_File(roomId int, msg []byte) {
	pushRoomMsgReq := &proto.PushRoomMsgRequest{
		RoomId: roomId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpRoomSend_File,
			SeqId:     tools.GetSnowflakeId(),
			Body:      msg,
		},
	}
	reply := &proto.SuccessReply{}
	//获取所有的rpc客户端     使用RPC广播消息给所有连接
	rpcList := RClient.GetAllConnectTypeRpcClient()
	for _, rpc := range rpcList {
		logrus.Infof("broadcastRoomToConnect rpc  %v", rpc)
		//通过RPC调用远程服务的PushRoomMsg方法
		//，传递pushRoomMsgReq作为参数，
		//并将回复结果保存在reply中
		rpc.Call(context.Background(), "PushRoomMsg", pushRoomMsgReq, reply)
		logrus.Infof("reply %s", reply.Msg)
	}
}

func (task *Task) broadcastRoomCountToConnect(roomId, count int) {
	msg := &proto.RedisRoomCountMsg{
		Count: count,
		Op:    config.OpRoomCountSend,
	}
	var body []byte
	var err error
	if body, err = json.Marshal(msg); err != nil {
		logrus.Warnf("broadcastRoomCountToConnect  json.Marshal err :%s", err.Error())
		return
	}
	pushRoomMsgReq := &proto.PushRoomMsgRequest{
		RoomId: roomId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpRoomCountSend,
			SeqId:     tools.GetSnowflakeId(),
			Body:      body,
		},
	}
	reply := &proto.SuccessReply{}
	rpcList := RClient.GetAllConnectTypeRpcClient()
	for _, rpc := range rpcList {
		logrus.Infof("broadcastRoomCountToConnect rpc  %v", rpc)
		rpc.Call(context.Background(), "PushRoomCount", pushRoomMsgReq, reply)
		logrus.Infof("reply %s", reply.Msg)
	}
}

func (task *Task) broadcastRoomInfoToConnect(roomId int, roomUserInfo map[string]string) {
	msg := &proto.RedisRoomInfo{
		Count:        len(roomUserInfo),
		Op:           config.OpRoomInfoSend,
		RoomUserInfo: roomUserInfo,
		RoomId:       roomId,
	}
	var body []byte
	var err error
	if body, err = json.Marshal(msg); err != nil {
		logrus.Warnf("broadcastRoomInfoToConnect  json.Marshal err :%s", err.Error())
		return
	}
	pushRoomMsgReq := &proto.PushRoomMsgRequest{
		RoomId: roomId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpRoomInfoSend,
			SeqId:     tools.GetSnowflakeId(),
			Body:      body,
		},
	}
	reply := &proto.SuccessReply{}
	rpcList := RClient.GetAllConnectTypeRpcClient()
	for _, rpc := range rpcList {
		logrus.Infof("broadcastRoomInfoToConnect rpc  %v", rpc)
		rpc.Call(context.Background(), "PushRoomInfo", pushRoomMsgReq, reply)
		logrus.Infof("broadcastRoomInfoToConnect rpc  reply %v", reply)
	}
}

func GetParamByKey(s string, key string) string {
	params := strings.Split(s, "&")
	for _, p := range params {
		kv := strings.Split(p, "=")
		if len(kv) == 2 && kv[0] == key {
			return kv[1]
		}
	}
	return ""
}

func (task *Task) InitConnectRpcClient() (err error) {
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
	}
	if len(d.GetServices()) <= 0 {
		logrus.Panicf("no etcd server find!")
	}
	//调用d.GetServices方法获取Etcd中注册的所有服务。
	for _, connectConf := range d.GetServices() {
		logrus.Infof("key is:%s,value is:%s", connectConf.Key, connectConf.Value)
		//RpcConnectClients
		//"id=1&serverType=rpc&address=127.0.0.1:8080"
		serverType := GetParamByKey(connectConf.Value, "serverType")
		serverId := GetParamByKey(connectConf.Value, "serverId")
		logrus.Infof("serverType is:%s,serverId is:%s", serverType, serverId)
		if serverType == "" || serverId == "" {
			continue
		}
		//创建一个client.NewPeer2PeerDiscovery对象，用于根据服务键(key)进行RPC客户端的发现。
		//创建一个Peer to Peer模式下的服务发现客户端实例。
		//初始化时指定connectConf.Key作为路径前缀,后续服务查询和注册都会使用这个前缀。
		d, e := client.NewPeer2PeerDiscovery(connectConf.Key, "")
		if e != nil {
			logrus.Errorf("init task client.NewPeer2PeerDiscovery client fail:%s", e.Error())
			continue
		}
		//使用client.NewXClient方法创建一个RPC客户端c
		c := client.NewXClient(etcdConfig.ServerPathConnect, client.Failtry, client.RandomSelect, d, client.DefaultOption)
		ins := Instance{
			ServerType: serverType,
			ServerId:   serverId,
			Client:     c,
		}
		//如果RClient.ServerInsMap中不存在serverId对应的RPC客户端实例列表
		if _, ok := RClient.ServerInsMap[serverId]; !ok {
			RClient.ServerInsMap[serverId] = []Instance{ins}
		} else {
			RClient.ServerInsMap[serverId] = append(RClient.ServerInsMap[serverId], ins)
		}
	}
	// watch connect server change && update RpcConnectClientList
	go task.watchServicesChange(d)
	return
}

// 用于监视服务变化并更新RPC客户端实例的函数。
// 无法监视服务器宕机   用于监视服务注册注册信息的变化
func (task *Task) watchServicesChange(d client.ServiceDiscovery) {
	etcdconfig := config.Conf.Common.CommonEtcd
	//当有服务注册信息发生变化时，kvChan通道会收到相应的通知，并且循环会进入下一次迭代
	//一旦有变更,就可以获取最新的服务信息
	//当没有变化时，kvChan会阻塞
	for kvChan := range d.WatchService() {
		if len(kvChan) <= 0 {
			logrus.Errorf("connect services change, connect alarm, no abailable ip")
		}
		logrus.Infof("connect services change trigger...")
		insMap := make(map[string][]Instance) // 存储rpc实例
		for _, kv := range kvChan {
			logrus.Infof("connect sensMap := make(map[string][]Instance)\n\t\tfor _, kv := range kvChan {\n\t\t\tlogrus.Infof(\"connect services change,key is:%s,value is:%s", kv.Key, kv.Value)
			serverType := GetParamByKey(kv.Value, "serverType")
			serverId := GetParamByKey(kv.Value, "serverId")
			logrus.Infof("serverType is:%s,serverId is:%s", serverType, serverId)
			if serverType == "" || serverId == "" {
				continue
			}
			d, e := client.NewPeer2PeerDiscovery(kv.Key, "")
			if e != nil {
				logrus.Errorf("init task client.NewPeer2PeerDiscovery watch client fail:%s", e.Error())
				continue
			}
			c := client.NewXClient(etcdconfig.ServerPathConnect, client.Failtry, client.RandomSelect, d, client.DefaultOption)
			ins := Instance{
				ServerType: serverType,
				ServerId:   serverId,
				Client:     c,
			}
			if _, ok := insMap[serverId]; !ok {
				insMap[serverId] = []Instance{ins}
			} else {
				insMap[serverId] = append(insMap[serverId], ins)
			}
		}
		RClient.lock.Lock()
		RClient.ServerInsMap = insMap
		RClient.lock.Unlock()
	}
}

// 将单个推送消息发送到连接层   将消息推送到指定serverId的服务器
func (task *Task) pushSingleToConnect(serverId string, userId int, msg []byte) {
	logrus.Infof("pushSingleToConnect Body %s", string(msg))
	pushMsgReq := &proto.PushMsgRequest{
		UserId: userId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpSingleSend,
			SeqId:     tools.GetSnowflakeId(), //雪花算法生成消息id
			Body:      msg,
		},
	}
	reply := &proto.SuccessReply{}
	connectRpc, err := RClient.GetRpcClientByServerId(serverId)

	if err != nil {
		logrus.Infof("get rpc client err %v  ", err)
		logrus.Info("重新获取serverid")
		userKey := GetUserKey(fmt.Sprintf("%d", userId))

		serverId, err = logic.RedisClient.Get(context.Background(), userKey)
		if err != nil {
			logrus.Errorf("Reacquire serverid fail err = ", err)
		}
		connectRpc, err = RClient.GetRpcClientByServerId(serverId)
		if err != nil {
			logrus.Errorf("重新获取ConnectRpc失败 err = ", err)
		}
	}

	err = connectRpc.Call(context.Background(), "PushSingleMsg", pushMsgReq, reply)
	if err != nil {
		logrus.Infof("pushSingleToConnect Call err %v", err)
	}
	logrus.Infof("reply %s", reply.Msg)
}

func (task *Task) pushSingleToConnect_File(serverId string, userId int, msg []byte) {
	logrus.Infof("pushSingleToConnect Body %s", string(msg))
	pushMsgReq := &proto.PushMsgRequest{
		UserId: userId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpSingleSend_File,
			SeqId:     tools.GetSnowflakeId(), //雪花算法生成消息id
			Body:      msg,
		},
	}
	reply := &proto.SuccessReply{}
	connectRpc, err := RClient.GetRpcClientByServerId(serverId)

	if err != nil {
		logrus.Infof("get rpc client err %v  ", err)
		logrus.Info("重新获取serverid")
		userKey := GetUserKey(fmt.Sprintf("%d", userId))

		serverId, err = logic.RedisClient.Get(context.Background(), userKey)
		if err != nil {
			logrus.Errorf("Reacquire serverid fail err = ", err)
		}
		connectRpc, err = RClient.GetRpcClientByServerId(serverId)
		if err != nil {
			logrus.Errorf("重新获取ConnectRpc失败 err = ", err)
		}
	}

	err = connectRpc.Call(context.Background(), "PushSingleMsg", pushMsgReq, reply)
	if err != nil {
		logrus.Infof("pushSingleToConnect Call err %v", err)
	}
	logrus.Infof("reply %s", reply.Msg)
}

func GetUserKey(authKey string) string {
	var returnKey bytes.Buffer
	returnKey.WriteString(config.RedisPrefix)
	returnKey.WriteString(authKey)
	return returnKey.String()
}
