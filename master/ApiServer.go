package master

import (
	"encoding/json"
	"fmt"
	"golang-crontab/common"
	"log"
	"net/http"
	"strconv"
)

func InitApiServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)

	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	// 静态文件
	dir := http.Dir("./webroot")
	handler := http.FileServer(dir)
	mux.Handle("/", http.StripPrefix("/", handler))

	go http.ListenAndServe(":"+strconv.Itoa(G_config.ApiPort), mux)
	log.Println("ApiPort: ", G_config.ApiPort)

	return nil
}

// 查询任务日志
func handleJobLog(resp http.ResponseWriter, req *http.Request) {
	var (
		err        error
		name       string // 任务名字
		skipParam  string // 从第几条开始
		limitParam string // 返回多少条
		skip       int
		limit      int
		logArr     []*common.JobLog
		bytes      []byte
	)

	// 解析GET参数
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 获取请求参数 /job/log?name=job10&skip=0&limit=10
	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}

	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}

	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		resp.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

// 获取健康worker节点列表
func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err       error
		bytes     []byte
	)

	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}

	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		resp.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

func handleJobKill(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/json")

	err := r.ParseForm()
	if err != nil {
		fmt.Println(`ParseForm err: `, err)
		response, _ := common.BuildResponse(1, err.Error(), nil)
		_, err = w.Write(response)
		return
	}

	name := r.PostForm.Get("name")

	err = G_jobMgr.KillJob(name)
	if err != nil {
		response, _ := common.BuildResponse(1, err.Error(), nil)
		w.Write(response)
		return
	}

	response, _ := common.BuildResponse(0, "ok", name)
	w.Write(response)
	return
}

func handleJobList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/json")

	list, err := G_jobMgr.ListJobs()

	if err != nil {
		response, _ := common.BuildResponse(1, err.Error(), nil)
		w.Write(response)
		return
	}

	response, _ := common.BuildResponse(0, "ok", list)
	w.Write(response)
	return
}

// 删除任务
func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/json")

	err := r.ParseForm()
	if err != nil {
		fmt.Println(`ParseForm err: `, err)
		response, _ := common.BuildResponse(1, err.Error(), nil)
		_, err = w.Write(response)
		return
	}

	name := r.PostForm.Get("name")

	deleteJob, err := G_jobMgr.DeleteJob(name)
	if err != nil {
		response, _ := common.BuildResponse(1, err.Error(), deleteJob)
		w.Write(response)
		return
	}

	response, _ := common.BuildResponse(0, "ok", deleteJob)
	w.Write(response)
	return

}

// 保存任务接口, 保存到etcd
// POST job={"name": "job1", "command": "echo hello", "cronExpr": "* * * * *"}
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/json")

	err := r.ParseForm()
	if err != nil {
		fmt.Println(`ParseForm err: `, err)
		response, _ := common.BuildResponse(1, err.Error(), nil)
		_, err = w.Write(response)
		return
	}

	var job common.Job
	postJob := r.PostForm.Get("job")
	err = json.Unmarshal([]byte(postJob), &job)
	if err != nil {
		fmt.Println(`ParseForm err: `, err)
		response, _ := common.BuildResponse(1, err.Error(), nil)
		_, err = w.Write(response)
		return
	}

	old, err := G_jobMgr.SaveJob(&job)
	if err != nil {
		response, _ := common.BuildResponse(1, err.Error(), old)
		_, err := w.Write(response)
		fmt.Println(err)
		return
	}

	response, _ := common.BuildResponse(0, "ok", old)
	_, err = w.Write(response)
	return
}
