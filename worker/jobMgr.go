package worker

import (
	"context"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang-crontab/common"
	"log"
	"time"
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
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
		client:  client,
		kv:      clientv3.NewKV(client),
		lease:   clientv3.NewLease(client),
		watcher: clientv3.NewWatcher(client),
	}

	// 启动任务监听
	err = G_jobMgr.watchJobs()

	// 启动监听killer
	G_jobMgr.watchKiller()

	return err
}

// 监听强杀任务通知
func (jobMgr *JobMgr) watchKiller() {
	var (
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent   *common.JobEvent
		jobName    string
		job        *common.Job
	)
	// 监听/cron/killer目录
	go func() { // 监听协程
		// 监听/cron/killer/目录的变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 杀死任务事件
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					// 事件推给scheduler
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // killer标记过期, 被自动删除
				}
			}
		}
	}()
}

// 监听任务变化
func (jobMgr *JobMgr) watchJobs() error {
	// 1. get /cron/jobs/目录下的所有任务, 并且获取当前集群的revision
	// 2. 从该revision向后监听变化事件

	response, err := jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	// 当前有哪些任务
	for _, kv := range response.Kvs {
		job, err := common.UnpackJob(kv.Value)
		if err != nil {
			return err
		}

		// job同步到调度协程(scheduler)
		log.Println(`当前存在的任务: `, job.Name)
		jobEvent := common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
		G_scheduler.PushJobEvent(jobEvent)
	}

	// 任务变化后 从该revision向后监听变化事件
	go func() {
		revision := response.Header.GetRevision() + 1

		// 监听/cron/jobs/目录后续变化, 返回chan
		watchChan := jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(revision), clientv3.WithPrefix())

		// 处理监听事件
		for watchResponse := range watchChan {
			for _, event := range watchResponse.Events {
				switch event.Type {
				case mvccpb.PUT: // 保存任务事件
					// 推送更新事件给 scheduler
					if job, err := common.UnpackJob(event.Kv.Value); err != nil {
						log.Println(`common.UnpackJob err: `, err)
						continue
					} else {
						log.Println(`获取到保存任务事件: `, job.Name)

						// 构建一个更新event事件
						jobEvent := common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
						G_scheduler.PushJobEvent(jobEvent)

					}
				case mvccpb.DELETE: // 任务被删除了
					// 推送删除事件给 scheduler
					jobName := common.ExtractJobName(string(event.Kv.Key))
					log.Println(`获取到删除任务事件: `, jobName)

					// 构建一个删除event事件
					jobEvent := common.BuildJobEvent(common.JOB_EVENT_DELETE, &common.Job{
						Name: jobName,
					})
					G_scheduler.PushJobEvent(jobEvent)
				}
			}
		}
	}()

	return nil
}

// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}
