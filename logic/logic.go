package logic

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"goim/config"
	"runtime"
)

type Logic struct {
	ServerId string
}

func New() *Logic {
	return new(Logic)
}

func (logic *Logic) Run() {
	//读取config中的内容
	logicConfig := config.Conf.Logic

	//获取最大线程数
	runtime.GOMAXPROCS(logicConfig.LogicBase.CpuNum)

	//设置serverId
	logic.ServerId = fmt.Sprintf("logic-%s", uuid.New().String())

	//init pubish redis
	if err := logic.InitPublishRedisClient(); err != nil {
		logrus.Panicf("logic init publishRedisClient fail,err:%s", err.Error())
	}

	if err := logic.InitRpcServer(); err != nil {
		logrus.Panicf("logic init rpc server fail")
	}
}
