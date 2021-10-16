package worker

import (
	"fmt"
	"golang-crontab/common"
	"log"
	"time"
)

// 任务调度
type Scheduler struct {
	jobEventChan      chan *common.JobEvent
	jobPlanTable      map[string]*common.JobSchedulePlan
	jobExecutingTable map[string]*common.JobExecuteInfo // 任务执行表
	jobResultChan     chan *common.JobExecuteResult     // 任务结果队列
}

var G_scheduler *Scheduler

func InitScheduler() {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}

	go G_scheduler.scheduleLoop()
}

// 重新计算任务调度状态
func (s *Scheduler) TryScheduler() (schedulerAfter time.Duration) {
	// 1. 遍历所有任务
	// 2. 过期任务立即执行
	// 3. 统计最近要过期的任务

	// 如果任务表为空话，随便睡眠多久
	if len(s.jobPlanTable) == 0 {
		schedulerAfter = 1 * time.Second
		return
	}

	now := time.Now()
	var nearTime *time.Time
	for _, plan := range s.jobPlanTable {
		if plan.NextTime.Before(now) || plan.NextTime.Equal(now) {
			// 尝试执行任务
			//fmt.Println("执行任务: ", time.Now().Format(`2006-01-02 15:04:05`), plan.Job.Name)
			s.TryStartJob(plan)

			plan.NextTime = plan.Expr.Next(now) // 更新下次执行时间
		}

		// 统计最近要过期的任务
		if nearTime == nil || plan.NextTime.Before(*nearTime) {
			nearTime = &plan.NextTime
		}
	}

	if nearTime != nil {
		schedulerAfter = (*nearTime).Sub(time.Now())
	}

	return
}

// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	// 调度 和 执行 是2件事情
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)

	// 执行的任务可能运行很久, 1分钟会调度60次，但是只能执行1次, 防止并发！

	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		// fmt.Println("尚未退出,跳过执行:", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)
}

// 调度协程
func (s *Scheduler) scheduleLoop() {

	schedulerAfter := s.TryScheduler()
	schedulerTimer := time.NewTimer(schedulerAfter)

	for {
		select {
		case jobEvent := <-s.jobEventChan: // 获取到任务变化事件
			s.handleJobEvent(jobEvent)
		case <-schedulerTimer.C: // 最近的任务到期了
		case jobResult := <-s.jobResultChan:
			s.handleJobResult(jobResult)
		}

		// 调度一次任务
		schedulerAfter = s.TryScheduler()
		// 重置调度间隔
		schedulerTimer.Reset(schedulerAfter)
	}
}

// 向scheduler推送任务变化事件
func (s *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	s.jobEventChan <- jobEvent
}

// 处理任务事件
func (s *Scheduler) handleJobEvent(event *common.JobEvent) {
	switch event.EventType {
	case common.JOB_EVENT_SAVE:
		plan, err := common.BuildJobSchedulePlan(event.Job)
		if err != nil {
			log.Println(`BuildJobSchedulePlanL err: `, err)
		}
		s.jobPlanTable[event.Job.Name] = plan
	case common.JOB_EVENT_DELETE:
		if _, ok := s.jobPlanTable[event.Job.Name]; ok {
			delete(s.jobPlanTable, event.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		// 取消掉Command执行, 判断任务是否在执行中
		if jobExecuteInfo, exist := s.jobExecutingTable[event.Job.Name]; exist {
			jobExecuteInfo.CancelFunc() // 触发command杀死shell子进程, 任务得到退出
		}
	}
}

// 回传任务执行结果
func (s *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	s.jobResultChan <- jobResult
}

// 处理任务结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	// 删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)
	fmt.Println("任务执行完成:", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)

	var (
		jobLog *common.JobLog
	)
	// 删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		G_logSink.Append(jobLog)
	}
}
