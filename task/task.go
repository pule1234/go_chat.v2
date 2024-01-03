package task

import (
	"github.com/sirupsen/logrus"
	"goim/config"
	"runtime"
)

type Task struct {
}

func New() *Task {
	return new(Task)
}

func (task *Task) Run() {
	taskconfig := config.Conf.Task

	runtime.GOMAXPROCS(taskconfig.TaskBase.CpuNum)

	//read from redis queue
	if err := task.InitQueueRedisClient(); err != nil {
		logrus.Panicf("task init publishRedisClient fail,err:%s", err.Error())
	}
	//rpc call connect layer send msg // 创建rpc客户端
	if err := task.InitConnectRpcClient(); err != nil {
		logrus.Panicf("task init InitConnectRpcClient fail,err:%s", err.Error())
	}
	task.GoPush()
}
