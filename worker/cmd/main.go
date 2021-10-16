package main

import (
	"flag"
	"golang-crontab/worker"
	"runtime"
	"time"
)

var configFile string

func initArgs() {

	flag.StringVar(&configFile, "config", "./worker.json", "配置文件位置")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	initArgs()

	initEnv()

	// 加载配置
	err := worker.InitConfig(configFile)
	if err != nil {
		panic(err)
	}

	// 服务注册
	if err = worker.InitRegister(); err != nil {
		panic(err)
	}

	// 启动日志协程
	if err = worker.InitLogSink(); err != nil {
		panic(err)
	}

	// 启动执行器
	if err = worker.InitExecutor(); err != nil {
		panic(err)
	}

	// 启动调度器
	worker.InitScheduler()

	// 任务管理器
	err = worker.InitJobMgr()
	if err != nil {
		panic(err)
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
