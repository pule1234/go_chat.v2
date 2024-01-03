package task

import (
	"context"
	"goim/config"
	RedMQ "goim/redMQ"
	"goim/redMQ/redis"
	"goim/tools"
	"log"
	"time"
)

var RedisClient *redis.Client

// 不断从指定的队列中获取消息，并将消息传递给task.Push方法进行处理
func (task *Task) InitQueueRedisClient() (err error) {
	redisOpt := tools.RedisOption{
		Address:  config.Conf.Common.CommonRedis.RedisAddress,
		Password: config.Conf.Common.CommonRedis.RedisPassword,
		Db:       config.Conf.Common.CommonRedis.Db,
	}
	//根据RedisOption对象获取Redis客户端实例
	RedisClient = tools.GetRedisInstance(redisOpt)

	//go func() {
	//	for {
	//		var result []string
	//		//10s timeout 使用RedisClient.BRPop方法从阻塞地指定的队列中获取消息
	//		result, err = RedisClient.BRPop(time.Second*10, config.QueueName).Result()
	//		if err != nil {
	//			logrus.Infof("task queue block timeout,no msg err:%s", err.Error())
	//		}
	//		if len(result) >= 2 { //当result长度大于等于2，将消息的第二个元素（消息内容）传递给task.push
	//			task.Push(result[1])
	//		}
	//	}
	//}()

	// consumer 接收到消息后执行的callback回调处理函数
	callbackFunc := func(ctx context.Context, msg *redis.MsgEntity) error {
		log.Printf("receive msg, msg id: %s, msg key: %s, msg val: %s", msg.MsgID, msg.Key, msg.Value)
		task.Push(msg.Value)
		return nil
	}

	// 创建自定义死信队列实例
	demoDeadLetterMailbox := NewDemoDeadLetterMailbox(func(msg *redis.MsgEntity) {
		log.Printf("receive dead letter, msg id: %s, msg key: %s, msg val: %s", msg.MsgID, msg.Key, msg.Value)
	})

	//启动consumer实例
	consumer, err := RedMQ.NewCusumer(RedisClient, config.QueueName, config.ConsumerGroup, config.ConsumerID, callbackFunc,
		//每条消息最多重试处理2次
		RedMQ.WithMaxRetryLimit(2),
		// 每轮接收消息的阻塞等待超时时间为2s
		RedMQ.WithReceiveTimeout(2*time.Second),
		// 注入自定义实现的死信队列
		RedMQ.WithDeadLetterMailbox(demoDeadLetterMailbox))

	if err != nil {
		log.Panic(err)
		return
	}

	defer consumer.Stop()
	return
}

// 自定义实现的死信队列
type DemoDeadLetterMailbox struct {
	do func(msg *redis.MsgEntity)
}

func NewDemoDeadLetterMailbox(do func(msg *redis.MsgEntity)) *DemoDeadLetterMailbox {
	return &DemoDeadLetterMailbox{
		do: do,
	}
}

// 死信队列的处理方法
func (d *DemoDeadLetterMailbox) Deliver(ctx context.Context, msg *redis.MsgEntity) error {
	d.do(msg)
	return nil
}
