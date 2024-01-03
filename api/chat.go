package api

import (
	"context"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"goim/api/router"
	"goim/api/rpc"
	"goim/config"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Chat struct {
}

func New() *Chat {
	return &Chat{}
}

// 启动Chat应用，初始化RPC客户端，设置运行模式，创建HTTP服务器并启动
// ，处理终止信号，并在接收到终止信号后优雅地关闭服务器
func (c *Chat) Run() {
	//init rpc CLient
	rpc.InitLogicRpcClient()

	r := router.Register()
	//runMode := config.GetGinRunMode()
	//logrus.Info("server start , now run mode is ", runMode)
	////设置当前运行模式
	//gin.SetMode(runMode)
	apiconfig := config.Conf.Api
	port := apiconfig.ApiBase.ListenPort
	flag.Parse()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: r,
	}

	//在启动HTTP服务器之前，使用go关键字开启一个新的goroutine，用于异步启动服务器。
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Errorf("start listen : %s\n", err)
		}
	}()

	quit := make(chan os.Signal)
	//调用signal.Notify方法注册一些常见的终止信号
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-quit
	logrus.Infof("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logrus.Errorf("Server Shutdown:", err)
	}
	logrus.Infof("Server exiting")
	os.Exit(0)
}
