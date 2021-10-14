package master

import (
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

func (this *JobMgr) SaveJob(job *common.Job) (old *common.Job, err error) {

	return nil, err
}
