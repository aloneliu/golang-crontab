package master

import (
	"encoding/json"
	"fmt"
	"golang-crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var (
	G_apiServer *ApiServer
)

func InitApiServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/job/save", handleJobSave)

	listen, err := net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort))
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		ReadTimeout:  time.Duration(G_config.ReadTimeout),
		WriteTimeout: time.Duration(G_config.WriteTimeout),
		Handler:      mux,
	}

	G_apiServer = &ApiServer{httpServer: httpServer}

	go httpServer.Serve(listen)

	return nil
}

// 保存任务接口, 保存到etcd
// POST job={"name": "job1", "command": "echo hello", "cronExpr": "* * * * *"}
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		fmt.Println(`ParseForm err: `, err)
	}

	var job common.Job
	postJob := r.PostForm.Get("job")
	err = json.Unmarshal([]byte(postJob), &job)
	if err != nil {
		fmt.Println(`ParseForm err: `, err)
	}

	old, err := G_jobMgr.SaveJob(&job)
	if err != nil {
		response, _ := common.BuildResponse(1, err.Error(), old)
		w.Write(response)
		return
	}

	response, _ := common.BuildResponse(0, "ok", old)
	w.Write(response)
	return
}
