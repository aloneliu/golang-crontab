package main

import (
	"flag"
	"golang-crontab/master"
	"runtime"
)

var configFile string

func initArgs() {

	flag.StringVar(&configFile, "config", "./master.json", "配置文件位置")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	initArgs()

	initEnv()

	// 加载配置
	err := master.InitConfig(configFile)
	if err != nil {
		panic(err)
	}

	// 任务管理器
	err = master.InitJobMgr()
	if err != nil {
		panic(err)
	}

	if err := master.InitApiServer(); err != nil {
		panic(err)
	}

	return
}
