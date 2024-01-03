package main

import (
	"flag"
	"fmt"
	"goim/api"
	"goim/connect"
	"goim/logic"
	"goim/task"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var module string
	//flag.StringVar定义的是命令行参数-module的处理逻辑。
	//module是定义的命令行参数名。
	//
	//第二个参数""是这个参数的默认值。
	//
	//第三个参数"assign run module"是对这个参数的描述文字。
	flag.StringVar(&module, "module", "", "assign run module")
	flag.Parse()
	fmt.Println(fmt.Sprintf("start run %s module", module))
	switch module {
	case "logic":
		logic.New().Run()
	case "connect_websocket":
		connect.New().Run()
	case "connect_tcp":
		connect.New().RunTcp()
	case "task":
		task.New().Run()
	case "api":
		api.New().Run()
	default:
		fmt.Println("exiting,module param error!")
		return
	}

	fmt.Println(fmt.Sprintf("run %s module done!", module))
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-quit
	fmt.Println("Server exiting")
}
