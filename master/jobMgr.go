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
	jobKey := common.JOB_SAVE_DIR + job.Name
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

func (jobMgr *JobMgr) DeleteJob(name string) (old *common.Job, err error) {
	jobKey := common.JOB_SAVE_DIR + name

	response, err := jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}

	oldJob := common.Job{}
	if len(response.PrevKvs) != 0 {
		err := json.Unmarshal(response.PrevKvs[0].Value, &oldJob)
		if err != nil {
			return nil, err
		}
	}

	return &oldJob, err
}

func (jobMgr *JobMgr) ListJobs() (list []*common.Job, err error) {
	dirKey := common.JOB_SAVE_DIR

	response, err := jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	for _, kv := range response.Kvs {
		job := &common.Job{}
		err = json.Unmarshal(kv.Value, &job)
		if err == nil {
			list = append(list, &common.Job{
				Name:     job.Name,
				Command:  job.Command,
				CronExpr: job.CronExpr,
			})
		}
	}

	return list, err
}

func (jobMgr *JobMgr) KillJob(name string) (err error) {
	key := common.JOB_KILLER_DIR + name

	// 创建一个自动过期的租约
	grant, err := jobMgr.lease.Grant(context.TODO(), 1)
	if err != nil {
		return err
	}

	leaseID := grant.ID

	_, err = jobMgr.kv.Put(context.TODO(), key, "", clientv3.WithLease(leaseID))

	return err
}
