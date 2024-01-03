package task

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"goim/config"
	"goim/proto"
	"math/rand"
)

type PushParams struct {
	ServerId string
	UserId   int
	Msg      []byte
	RoomId   int
}

var pushChannel []chan *PushParams

func init() {
	pushChannel = make([]chan *PushParams, config.Conf.Task.TaskBase.PushChan)
}

func (task *Task) GoPush() {
	for i := 0; i < len(pushChannel); i++ {
		//为每个通道创建一个带有缓冲区的通道，并将其赋值给pushChannel[i]。
		pushChannel[i] = make(chan *PushParams, config.Conf.Task.TaskBase.PushChanSize)
		go task.processSinglePush(pushChannel[i])
	}
}

func (task *Task) processSinglePush(ch chan *PushParams) {
	arg := &PushParams{
		Msg: make([]byte, 1024),
	}
	for {
		arg = <-ch
		//当指定的arg.ServerId对应的服务器宕机时,用户应该能够重连到其他服务器。但是消息队列中的消息可能没有被消费。
		//@todo when arg.ServerId server is down, user could be reconnect other serverId but msg in queue no consume
		var rawTcpMsg proto.SendTcp
		err := json.Unmarshal([]byte(arg.Msg), &rawTcpMsg)
		if err != nil {
			logrus.Errorf("tcp message struct %+v", rawTcpMsg)
			logrus.Errorf("processSinglePush err %+v", err)
			break
		}

		switch rawTcpMsg.Op {
		case config.OpSingleSend:
			task.pushSingleToConnect(arg.ServerId, arg.UserId, arg.Msg)
			logrus.Info("arg.msg", arg.Msg)
		case config.OpSingleSend_File:
			task.pushSingleToConnect_File(arg.ServerId, arg.UserId, arg.Msg)
			logrus.Info("arg.msg", arg.Msg)
		}

	}
}

func (task *Task) Push(msg string) {
	m := &proto.RedisMsg{}
	if err := json.Unmarshal([]byte(msg), m); err != nil {
		logrus.Infof(" json.Unmarshal err:%v ", err)
	}
	logrus.Infof("push msg info %d,op is:%d", m.RoomId, m.Op)
	switch m.Op {
	case config.OpSingleSend:
		//将消息通过pushChannel通道发送给一个随机选择的PushParams对象
		pushChannel[rand.Int()%config.Conf.Task.TaskBase.PushChan] <- &PushParams{
			ServerId: m.ServerId,
			UserId:   m.UserId,
			RoomId:   m.RoomId,
		}
	case config.OpSingleSend_File:
		pushChannel[rand.Int()%config.Conf.Task.TaskBase.PushChan] <- &PushParams{
			ServerId: m.ServerId,
			UserId:   m.UserId,
			RoomId:   m.RoomId,
		}
	case config.OpRoomSend:
		task.broadcastRoomToConnect(m.RoomId, m.Msg)
	case config.OpRoomSend_File:
		task.broadcastRoomToConnect_File(m.RoomId, m.Msg)
	case config.OpRoomCountSend:
		task.broadcastRoomCountToConnect(m.RoomId, m.Count)
	case config.OpRoomInfoSend:
		task.broadcastRoomInfoToConnect(m.RoomId, m.RoomUserInfo)
	}
}
