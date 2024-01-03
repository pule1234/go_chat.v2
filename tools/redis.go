package tools

import (
	"fmt"
	"goim/redMQ/redis"
	"sync"
)

// map中存储着redis的客户端
var RedisClientMap = map[string]*redis.Client{}
var syncLock sync.Mutex

type RedisOption struct {
	Address  string
	Password string
	Db       int
}

func GetRedisInstance(redisOpt RedisOption) *redis.Client {
	address := redisOpt.Address
	//db := redisOpt.Db
	password := redisOpt.Password
	addr := fmt.Sprintf("%s", address)

	syncLock.Lock()
	//如果该地址存在就直接返回这个连接
	if redisCli, ok := RedisClientMap[addr]; ok {
		return redisCli
	}

	//不存在
	client := redis.NewClient("tcp", addr, password)

	//将新创建的redis客户端记录到map中
	RedisClientMap[addr] = client
	syncLock.Unlock()
	return RedisClientMap[addr]
}
