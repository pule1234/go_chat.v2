package config

import (
	"github.com/spf13/viper"
	"os"
	"runtime"
	"strings"
	"sync"
)

var once sync.Once
var realPath string
var Conf *Config

const (
	SuccessReplyCode      = 0
	FailReplyCode         = 1
	SuccessReplyMsg       = "success"
	QueueName             = "gochat_queue"
	ConsumerGroup         = "请输入消费者组名称"
	ConsumerID            = "请输入消费者名称"
	RedisBaseValidTime    = 86400
	RedisPrefix           = "gochat_"
	RedisRoomPrefix       = "gochat_room_"
	RedisRoomOnlinePrefix = "gochat_room_online_count_"
	MsgVersion            = 1
	OpSingleSend          = 2 // single user
	OpSingleSend_File     = 7
	OpRoomSend            = 3 // send to room
	OpRoomSend_File       = 8
	OpRoomCountSend       = 4 // get online user count
	OpRoomInfoSend        = 5 // send info to room
	OpBuildTcpConn        = 6 // build tcp conn
	MaxQueueLen           = 1000
)

var TencentSecretKey = os.Getenv("7FH7xq6AzeGgssnX4PfkcHqdGgv2zlMy")
var TencentSecretID = os.Getenv("AKIDZ8XkolY8ZntoeJpAOwJVVMzRispeKxOb")
var CosBucket = "https://1-1319901250.cos.ap-guangzhou.myqcloud.com"

type Config struct {
	Db      DbConfig
	Common  Common
	Connect ConnectConfig
	Logic   LogicConfig
	Task    TaskConfig
	Api     ApiConfig
	Site    SiteConfig
}

func init() {
	Init()
}

func getCurrentDir() string {
	_, fileName, _, _ := runtime.Caller(1) // 获取当前的调试栈信息，参数为1表示获取调用该函数的上一层函数的信息
	aPath := strings.Split(fileName, "/")
	dir := strings.Join(aPath[:len(aPath)-1], "/")
	return dir
}

// 根据当前的运行模式和配置文件路径，读取并合并各个配置文件的内容，并将其反序列化到相应的结构体中
func Init() {
	once.Do(func() {
		//env := GetMode()
		realPath := getCurrentDir()                     // 调用函数getCurrentDir()获取当前文件所在的目录路径
		configFilePath := realPath + "/" + "conf" + "/" //根据当前运行模式和目录路径构建配置文件的路径。
		viper.SetConfigType("toml")
		//viper.SetConfigName("/db")
		viper.SetConfigName("/connect")     //设置主配置文件的名称
		viper.AddConfigPath(configFilePath) // 添加配置文件的搜索路径
		err := viper.ReadInConfig()         // 读取主配置文件
		if err != nil {
			panic(err)
		}
		//读取并合并其他配置文件（common、task、logic、api、site）。
		viper.SetConfigName("common")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/task")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/logic")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/api")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/site")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		Conf = new(Config)
		//反序列化配置文件中的内容
		viper.Unmarshal(&Conf.Common)
		viper.Unmarshal(&Conf.Connect)
		viper.Unmarshal(&Conf.Task)
		viper.Unmarshal(&Conf.Logic)
		viper.Unmarshal(&Conf.Api)
		viper.Unmarshal(&Conf.Site)
	})
}

// //读取环境变量RUN_MODE来获取当前的运行模式。如果环境变量不存在或为空，则默认为开发模式
//func GetMode() string {
//	env := os.Getenv("RUN_MODE")
//	if env == "" {
//		env = "dev"
//	}
//	return env
//}

//func GetGinRunMode() string {
//	env := GetMode()
//	//gin have debug,test,release mode
//	if env == "dev" {
//		return "debug"
//	}
//	if env == "test" {
//		return "debug"
//	}
//	if env == "prod" {
//		return "release"
//	}
//	return "release"
//}

// mapstructure是Go语言中一个常用的结构体映射库。
//
// 它允许你使用结构体标签将结构体字段映射到配置文件中的字段。
type CommonEtcd struct {
	Host              string `mapstructure:"host"`              //服务器主机地址
	BasePath          string `mapstructure:"basePath"`          //是Etcd的基础路径，该项目相关的数据会存储在该路径下
	ServerPathLogic   string `mapstructure:"serverPathLogic"`   //逻辑服务器在Etcd中的路径，逻辑服务器相关的信息将存储在路径下
	ServerPathConnect string `mapstructure:"serverPathConnect"` //连接服务器在Etcd中的路径,连接服务器的信息存储在该路径下
	UserName          string `mapstructure:"userName"`          //
	Password          string `mapstructure:"password"`
	ConnectionTimeout int    `mapstructure:"connectionTimeout"` //连接服务器超时时间
}

type DbBase struct {
	Link string `mapstructure:"link"`
}

type DbConfig struct {
	DbBase DbBase `mapstructure:"db-base"`
}

type CommonRedis struct {
	RedisAddress  string `mapstructure:"redisAddress"`
	RedisPassword string `mapstructure:"redisPassword"`
	Db            int    `mapstructure:"db"`
}
type Common struct {
	CommonEtcd  CommonEtcd  `mapstructure:"common-etcd"`
	CommonRedis CommonRedis `mapstructure:"common-redis"`
}

type ConnectBase struct {
	CertPath string `mapstructure:"certPath"`
	KeyPath  string `mapstructure:"keyPath"`
}

type ConnectRpcAddressWebsockts struct {
	Address string `mapstructure:"address"`
}

type ConnectRpcAddressTcp struct {
	Address string `mapstructure:"address"`
}

type ConnectBucket struct {
	CpuNum        int    `mapstructure:"cpuNum"`
	Channel       int    `mapstructure:"channel"`
	Room          int    `mapstructure:"room"`
	SrvProto      int    `mapstructure:"svrProto"`
	RoutineAmount uint64 `mapstructure:"routineAmount"`
	RoutineSize   int    `mapstructure:"routineSize"`
}

type ConnectWebsocket struct {
	ServerId string `mapstructure:"serverId"`
	Bind     string `mapstructure:"bind"`
}

type ConnectTcp struct {
	ServerId      string `mapstructure:"serverId"`
	Bind          string `mapstructure:"bind"`        //绑定的主机地址和端口号
	SendBuf       int    `mapstructure:"sendbuf"`     //发送缓冲区的大小，
	ReceiveBuf    int    `mapstructure:"receivebuf"`  //接收缓冲区的大小
	KeepAlive     bool   `mapstructure:"keepalive"`   //这是是否启用TCP的KeepAlive功能
	Reader        int    `mapstructure:"reader"`      //读取数据的并发数，
	ReadBuf       int    `mapstructure:"readBuf"`     //读取操作使用的缓冲区
	ReadBufSize   int    `mapstructure:"readBufSize"` //读取缓冲区的总大小
	Writer        int    `mapstructure:"writer"`      //写入数据的并发数，
	WriterBuf     int    `mapstructure:"writerBuf"`
	WriterBufSize int    `mapstructure:"writeBufSize"` //写入缓冲区的总大小，
}

type ConnectConfig struct {
	ConnectBase                ConnectBase                `mapstructure:"connect-base"`
	ConnectRpcAddressWebSockts ConnectRpcAddressWebsockts `mapstructure:"connect-rpcAddress-websockts"`
	ConnectRpcAddressTcp       ConnectRpcAddressTcp       `mapstructure:"connect-rpcAddress-tcp"`
	ConnectBucket              ConnectBucket              `mapstructure:"connect-bucket"`
	ConnectWebsocket           ConnectWebsocket           `mapstructure:"connect-websocket"`
	ConnectTcp                 ConnectTcp                 `mapstructure:"connect-tcp"`
}

type LoginBase struct {
	ServerId   string `mapstructure:"serverId"`
	CpuNum     int    `mapstructure:"cpuNum"`
	RpcAddress string `mapstructure:"rpcAddress"` //提供了两个地址，表示逻辑服务器可以通过这两个地址提供RPC服务
	CertPath   string `mapstructure:"certPath"`
	KeyPath    string `mapstructure:"keyPath"`
}

type LogicConfig struct {
	LogicBase LoginBase `mapstructure:"logic-base"`
}

type TaskBase struct {
	CpuNum        int    `mapstructure:"cpuNum"`
	RedisAddr     string `mapstructure:"redisAddr"`
	RedisPassword string `mapstructure:"redisPassword"`
	RpcAddress    string `mapstructure:"rpcAddress"`
	PushChan      int    `mapstructure:"pushChan"`
	PushChanSize  int    `mapstructure:"pushChanSize"`
}

type TaskConfig struct {
	TaskBase TaskBase `mapstructure:"task-base"`
}

type ApiBase struct {
	ListenPort int `mapstructure:"listenPort"`
}

type ApiConfig struct {
	ApiBase ApiBase `mapstructure:"api-base"`
}

type SiteBase struct {
	ListenPort int `mapstructure:"listenPort"`
}

type SiteConfig struct {
	SiteBase SiteBase `mapstructure:"site-base"`
}
