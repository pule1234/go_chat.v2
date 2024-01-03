package logic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"github.com/rpcxio/rpcx-etcd/serverplugin"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/rpcx/server"
	"goim/config"
	"goim/proto"
	RedMQ "goim/redMQ"
	"goim/redMQ/redis"
	"goim/tools"
	"strings"
	"time"
)

// 启动redis
var RedisClient *redis.Client
var RedisSessClient *redis.Client // 实际上用到的客户端

func (logic *Logic) InitPublishRedisClient() (err error) {
	redisOpt := tools.RedisOption{
		Address:  config.Conf.Common.CommonRedis.RedisAddress,
		Password: config.Conf.Common.CommonRedis.RedisPassword,
		Db:       config.Conf.Common.CommonRedis.Db,
	}

	//创建redis连接
	RedisClient = tools.GetRedisInstance(redisOpt)
	// ping一下，检查连接是否正常 如果返回的pong不为空，则表示连接成功
	//if pong, err := RedisClient.Ping().Result(); err != nil {
	//	logrus.Infof("RedisCli Ping Result pong: %s,  err: %s", pong, err)
	//}
	//this can change use another redis save session data
	RedisSessClient = RedisClient
	return err
}

func (logic *Logic) InitRpcServer() (err error) {
	var network, addr string
	//解析字段，分割存储到list中
	rpcAddressList := strings.Split(config.Conf.Logic.LogicBase.RpcAddress, ",")
	for _, bind := range rpcAddressList {
		// 解析bind获取地址和端口
		if network, addr, err = tools.ParseNetwork(bind); err != nil {
			logrus.Panicf("InitLogicRpc ParseNetwork error : %s", err.Error())
		}
		logrus.Infof("logic start run at-->%s:%s", network, addr)
		go logic.createRpcServer(network, addr)
	}
	return
}

func (logic *Logic) createRpcServer(network, addr string) {
	s := server.NewServer()
	//向v3中添加插件
	logic.addRegistryPlugin(s, network, addr)
	// logic.ServerId 为logic中创建
	//注册一个rpc对象  new(RpcLogic) 是一个RPC服务的实现对象，用于处理RPC请求  logic.ServerId 是RPC服务的唯一标识
	err := s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathLogic, new(RpcLogic), fmt.Sprintf("%s", logic.ServerId))

	if err != nil {
		logrus.Errorf("register error:%s", err.Error())
	} else {
		logrus.Info("RegisterName : RpcLogic success")
	}
	//使用s.RegisterOnShutdown()方法注册一个在关闭时执行的回调函数。
	s.RegisterOnShutdown(func(s *server.Server) {
		s.UnregisterAll()
	})

	//启动rpc服务器
	err = s.Serve(network, addr)
	if err != nil {
		logrus.Errorf("Server fail, error is %s", err.Error())
	}

	logrus.Info("RPC server has been started")

}

// 向服务器添加一个基于Etcd V3的服务注册插件   将服务器的网络地址注册到Etcd V3中
func (logic *Logic) addRegistryPlugin(s *server.Server, network, addr string) {
	//创建一个基于Etcd V3的服务注册插件对象r
	r := &serverplugin.EtcdV3RegisterPlugin{
		//网络类型(network)和地址(addr)的拼接字符串。
		//通过设置ServiceAddress属性，用于将服务的网络地址注册到指定的Etcd路径下
		ServiceAddress: network + "@" + addr,
		//用于指定Etcd服务器的地址
		EtcdServers: []string{config.Conf.Common.CommonEtcd.Host},
		//设置BasePath属性为一个Etcd注册路径
		BasePath:       config.Conf.Common.CommonEtcd.BasePath,
		Metrics:        metrics.NewRegistry(),
		UpdateInterval: time.Minute, // 更行间隔
	}
	//启动插件
	err := r.Start()
	if err != nil {
		logrus.Fatal(err)
	}
	// 将插件添加到服务器的插件列表中
	s.Plugins.Add(r)
}

func (logic *Logic) GetUserKey(authKey string) string {
	var returnKey bytes.Buffer
	returnKey.WriteString(config.RedisPrefix)
	returnKey.WriteString(authKey)
	return returnKey.String()
}

func (logic *Logic) RedisPublishChannel(serverId string, toUserId int, msg []byte) (err error) {
	redisMsg := proto.RedisMsg{
		Op:       config.OpSingleSend,
		ServerId: serverId,
		UserId:   toUserId,
		Msg:      msg,
	}

	producer := RedMQ.NewProducer(RedisClient, RedMQ.WithMsgQueueLen(10))
	ctx := context.Background()
	redisMsgStr, err := json.Marshal(redisMsg)
	if err != nil {
		logrus.Errorf("logic,RedisPublishChannel Marshal err:%s", err.Error())
		return err
	}
	redisChannel := config.QueueName
	//if err := RedisClient.LPush(redisChannel, redisMsgStr).Err(); err != nil {
	//	logrus.Errorf("logic,lpush err:%s", err.Error())
	//	return err
	//}

	msgID, err := producer.SendMsg(ctx, redisChannel, "SingleSend", string(redisMsgStr))
	if err != nil {
		logrus.Errorf("logic,lpush err:%s", err.Error())
		return err
	}
	logrus.Infof("msgid : ", msgID)

	return
}

func (logic *Logic) RedisPublishChannel_File(serverId string, toUserId int, msg []byte) (err error) {
	redisMsg := proto.RedisMsg{
		Op:       config.OpSingleSend_File,
		ServerId: serverId,
		UserId:   toUserId,
		Msg:      msg,
	}

	producer := RedMQ.NewProducer(RedisClient, RedMQ.WithMsgQueueLen(10))
	ctx := context.Background()

	redisMsgStr, err := json.Marshal(redisMsg)
	if err != nil {
		logrus.Errorf("logic,RedisPublishChannel Marshal err:%s", err.Error())
		return err
	}

	redisChannel := config.QueueName
	//if err := RedisClient.LPush(redisChannel, redisMsgStr).Err(); err != nil {
	//	logrus.Errorf("logic,lpush err:%s", err.Error())
	//	return err
	//}

	msgID, err := producer.SendMsg(ctx, redisChannel, "OpSingleSend_File", string(redisMsgStr))
	if err != nil {
		logrus.Errorf("logic,lpush err:%s", err.Error())
		return err
	}
	logrus.Infof("msgid : ", msgID)

	return
}

// 用于生成Redis中存储房间用户信息的键名
func (logic *Logic) getRoomUserKey(authkey string) string {
	var returnKey bytes.Buffer
	returnKey.WriteString(config.RedisRoomPrefix)
	returnKey.WriteString(authkey)
	return returnKey.String()
}

// 向群发送消息
func (logic *Logic) RedisPublishRoomInfo(roomId int, count int, RoomUserInfo map[string]string, msg []byte) (err error) {
	var redisMsg = &proto.RedisMsg{
		Op:           config.OpRoomSend,
		RoomId:       roomId,
		Count:        count,
		Msg:          msg,
		RoomUserInfo: RoomUserInfo,
	}

	producer := RedMQ.NewProducer(RedisClient, RedMQ.WithMsgQueueLen(10))
	ctx := context.Background()

	redisMsgByte, err := json.Marshal(redisMsg)
	if err != nil {
		logrus.Errorf("logic,RedisPublishRoomInfo redisMsg error : %s", err.Error())
		return
	}
	//将redisMsgStr字符串作为元素插入到config.QueueName对应的Redis列表中
	//err = RedisClient.LPush(config.QueueName, redisMsgByte).Err()
	//if err != nil {
	//	logrus.Errorf("logic,RedisPublishRoomInfo redisMsg error : %s", err.Error())
	//	return
	//}

	redisChannel := config.QueueName

	msgID, err := producer.SendMsg(ctx, redisChannel, "OpRoomSend", string(redisMsgByte))
	if err != nil {
		logrus.Errorf("logic,lpush err:%s", err.Error())
		return err
	}
	logrus.Infof("msgid : ", msgID)

	return
}

func (logic *Logic) RedisPublishRoomInfo_File(roomId int, count int, RoomUserInfo map[string]string, msg []byte) (err error) {
	var redisMsg = &proto.RedisMsg{
		Op:           config.OpRoomSend_File,
		RoomId:       roomId,
		Count:        count,
		Msg:          msg,
		RoomUserInfo: RoomUserInfo,
	}

	producer := RedMQ.NewProducer(RedisClient, RedMQ.WithMsgQueueLen(10))
	ctx := context.Background()

	redisMsgByte, err := json.Marshal(redisMsg)
	if err != nil {
		logrus.Errorf("logic,RedisPublishRoomInfo redisMsg error : %s", err.Error())
		return
	}
	//将redisMsgStr字符串作为元素插入到config.QueueName对应的Redis列表中
	//err = RedisClient.LPush(config.QueueName, redisMsgByte).Err()
	//if err != nil {
	//	logrus.Errorf("logic,RedisPublishRoomInfo redisMsg error : %s", err.Error())
	//	return
	//}

	redisChannel := config.QueueName

	msgID, err := producer.SendMsg(ctx, redisChannel, "OpRoomSend_File", string(redisMsgByte))
	if err != nil {
		logrus.Errorf("logic,lpush err:%s", err.Error())
		return err
	}
	logrus.Infof("msgid : ", msgID)

	return
}

func (logic *Logic) getRoomOnlineCountKey(authKey string) string {
	var returnKey bytes.Buffer
	returnKey.WriteString(config.RedisRoomOnlinePrefix)
	returnKey.WriteString(authKey)
	return returnKey.String()
}

func (logic *Logic) RedisPushRoomCount(roomId int, count int) (err error) {
	var redisMsg = &proto.RedisMsg{
		Op:     config.OpRoomCountSend,
		RoomId: roomId,
		Count:  count,
	}

	producer := RedMQ.NewProducer(RedisClient, RedMQ.WithMsgQueueLen(10))
	ctx := context.Background()

	redisMsgByte, err := json.Marshal(redisMsg)
	if err != nil {
		logrus.Errorf("logic,RedisPushRoomCount redisMsg error : %s", err.Error())
		return
	}
	//err = RedisClient.LPush(config.QueueName, redisMsgByte).Err()
	//if err != nil {
	//	logrus.Errorf("logic,RedisPushRoomCount redisMsg error : %s", err.Error())
	//	return
	//}

	redisChannel := config.QueueName

	msgID, err := producer.SendMsg(ctx, redisChannel, "OpRoomCountSend", string(redisMsgByte))
	if err != nil {
		logrus.Errorf("logic,lpush err:%s", err.Error())
		return err
	}
	logrus.Infof("msgid : ", msgID)

	return
}

func (logic *Logic) RedisPushRoomInfo(roomId int, count int, roomUserInfo map[string]string) (err error) {
	var redisMsg = &proto.RedisMsg{
		Op:           config.OpRoomCountSend,
		RoomId:       roomId,
		Count:        count,
		RoomUserInfo: roomUserInfo,
	}

	producer := RedMQ.NewProducer(RedisClient, RedMQ.WithMsgQueueLen(10))
	ctx := context.Background()

	redisMsgByte, err := json.Marshal(redisMsg)
	if err != nil {
		logrus.Errorf("logic,RedisPushRoomInfo redisMsg error : %s", err.Error())
		return
	}
	//err = RedisClient.LPush(config.QueueName, redisMsgByte).Err()
	//if err != nil {
	//	logrus.Errorf("logic,RedisPushRoomInfo redisMsg error : %s", err.Error())
	//	return
	//}

	redisChannel := config.QueueName

	msgID, err := producer.SendMsg(ctx, redisChannel, "OpRoomCountSend", string(redisMsgByte))
	if err != nil {
		logrus.Errorf("logic,lpush err:%s", err.Error())
		return err
	}
	logrus.Infof("msgid : ", msgID)

	return
}
