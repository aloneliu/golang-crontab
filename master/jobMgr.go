package master

import (
	"context"
	"encoding/json"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang-crontab/common"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var G_jobMgr *JobMgr

func InitJobMgr() error {
	config := clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return err
	}

	G_jobMgr = &JobMgr{
		client: client,
		kv:     clientv3.NewKV(client),
		lease:  clientv3.NewLease(client),
	}

	return nil
}

func (jobMgr *JobMgr) SaveJob(job *common.Job) (old *common.Job, err error) {

	jobKey := `/cron/jobs/` + job.Name
	jobVal, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}

	response, err := jobMgr.kv.Put(context.TODO(), jobKey, string(jobVal), clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}

	oldJob := common.Job{}
	if response.PrevKv != nil {
		err := json.Unmarshal(response.PrevKv.Value, &oldJob)
		if err != nil {
			return nil, err
		}
	}

	return &oldJob, err
}
