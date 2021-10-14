package common

import (
	"encoding/json"
	"log"
)

// 定时任务
type Job struct {
	Name     string `json:"name"`     //  任务名
	Command  string `json:"command"`  // shell命令
	CronExpr string `json:"cronExpr"` // cron表达式
}

type Response struct {
	ErrNo int         `json:"errNo"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

func BuildResponse(errNo int, msg string, data interface{}) (resp []byte, err error) {
	response := Response{
		ErrNo: errNo,
		Msg:   msg,
		Data:  data,
	}

	bytes, err := json.Marshal(response)
	if err != nil {
		log.Println("BuildResponse json Marshal err: ", err)
	}

	return bytes, nil
}
