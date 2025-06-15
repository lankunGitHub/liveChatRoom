package pool

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// GoPool 协程池
type GoPool struct {
	// 配置
	options *GoPoolOption

	// 任务队列
	taskQueue chan func()

	// 工作协程数量
	workerCount int32

	// 运行状态
	running int32

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 统计信息
	stats *GoPoolStats
}

// GoPoolOption 协程池配置选项
type GoPoolOption struct {
	// 核心配置
	MinWorkers  int           // 最小工作协程数
	MaxWorkers  int           // 最大工作协程数
	Capacity    int           // 协程池容量（兼容字段，等同于MaxWorkers）
	QueueSize   int           // 任务队列大小
	IdleTimeout time.Duration // 空闲超时时间

	// 性能配置
	SpawnTimeout time.Duration // 协程创建超时

	// 监控配置
	EnableStats bool // 是否启用统计
}

// GoPoolStats 协程池统计信息
type GoPoolStats struct {
	// 任务统计
	TasksSubmitted int64 // 提交的任务数
	TasksCompleted int64 // 完成的任务数
	TasksFailed    int64 // 失败的任务数

	// 协程统计
	WorkersCreated   int64 // 创建的协程数
	WorkersDestroyed int64 // 销毁的协程数
	CurrentWorkers   int32 // 当前协程数

	// 队列统计
	QueueLength int32 // 当前队列长度
	QueuePeak   int32 // 队列峰值长度

	// 时间统计
	AvgTaskDuration time.Duration // 平均任务执行时间
	TotalDuration   time.Duration // 总执行时间
}

// NewGoPool 创建协程池
func NewGoPool(options *GoPoolOption) (*GoPool, error) {
	if options == nil {
		options = DefaultGoPoolOption()
	}

	// 验证配置
	if err := validateGoPoolOption(options); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &GoPool{
		options:   options,
		taskQueue: make(chan func(), options.QueueSize),
		ctx:       ctx,
		cancel:    cancel,
		stats:     &GoPoolStats{},
	}

	return pool, nil
}

// DefaultGoPoolOption 默认协程池配置
func DefaultGoPoolOption() *GoPoolOption {
	maxWorkers := runtime.NumCPU() * 2
	return &GoPoolOption{
		MinWorkers:   runtime.NumCPU(),
		MaxWorkers:   maxWorkers,
		Capacity:     maxWorkers,
		QueueSize:    1000,
		IdleTimeout:  30 * time.Second,
		SpawnTimeout: 5 * time.Second,
		EnableStats:  true,
	}
}

// validateGoPoolOption 验证配置
func validateGoPoolOption(options *GoPoolOption) error {
	if options.MinWorkers <= 0 {
		options.MinWorkers = 1
	}

	// 处理Capacity字段兼容性
	if options.Capacity > 0 && options.MaxWorkers == 0 {
		options.MaxWorkers = options.Capacity
	}
	if options.MaxWorkers == 0 && options.Capacity == 0 {
		options.MaxWorkers = runtime.NumCPU() * 2
		options.Capacity = options.MaxWorkers
	}
	if options.MaxWorkers < options.MinWorkers {
		options.MaxWorkers = options.MinWorkers
	}

	// 保持Capacity与MaxWorkers同步
	if options.Capacity == 0 {
		options.Capacity = options.MaxWorkers
	}

	if options.QueueSize <= 0 {
		options.QueueSize = 100
	}
	if options.IdleTimeout <= 0 {
		options.IdleTimeout = 30 * time.Second
	}
	if options.SpawnTimeout <= 0 {
		options.SpawnTimeout = 5 * time.Second
	}
	return nil
}

// Start 启动协程池
func (p *GoPool) Start() error {
	if !atomic.CompareAndSwapInt32(&p.running, 0, 1) {
		return fmt.Errorf("pool already started")
	}

	// 启动最小数量的工作协程
	for i := 0; i < p.options.MinWorkers; i++ {
		p.spawnWorker()
	}

	// 启动监控协程
	if p.options.EnableStats {
		p.wg.Add(1)
		go p.monitor()
	}

	return nil
}

// Stop 停止协程池
func (p *GoPool) Stop() error {
	if !atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		return fmt.Errorf("pool not running")
	}

	// 取消上下文
	p.cancel()

	// 关闭任务队列
	close(p.taskQueue)

	// 等待所有协程结束
	p.wg.Wait()

	return nil
}

// Submit 提交任务
func (p *GoPool) Submit(task func()) error {
	if atomic.LoadInt32(&p.running) == 0 {
		return fmt.Errorf("pool not running")
	}

	if task == nil {
		return fmt.Errorf("task is nil")
	}

	select {
	case p.taskQueue <- task:
		if p.options.EnableStats {
			atomic.AddInt64(&p.stats.TasksSubmitted, 1)
			atomic.AddInt32(&p.stats.QueueLength, 1)

			// 更新队列峰值
			current := atomic.LoadInt32(&p.stats.QueueLength)
			for {
				peak := atomic.LoadInt32(&p.stats.QueuePeak)
				if current <= peak || atomic.CompareAndSwapInt32(&p.stats.QueuePeak, peak, current) {
					break
				}
			}
		}

		// 检查是否需要扩容
		p.trySpawnWorker()

		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("pool is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// spawnWorker 创建工作协程
func (p *GoPool) spawnWorker() {
	if atomic.LoadInt32(&p.workerCount) >= int32(p.options.MaxWorkers) {
		return
	}

	atomic.AddInt32(&p.workerCount, 1)
	if p.options.EnableStats {
		atomic.AddInt64(&p.stats.WorkersCreated, 1)
		atomic.StoreInt32(&p.stats.CurrentWorkers, atomic.LoadInt32(&p.workerCount))
	}

	p.wg.Add(1)
	go p.worker()
}

// trySpawnWorker 尝试创建工作协程
func (p *GoPool) trySpawnWorker() {
	queueLen := atomic.LoadInt32(&p.stats.QueueLength)
	workerCount := atomic.LoadInt32(&p.workerCount)

	// 如果队列长度超过当前工作协程数，且未达到最大协程数，则创建新协程
	if queueLen > workerCount && workerCount < int32(p.options.MaxWorkers) {
		p.spawnWorker()
	}
}

// worker 工作协程
func (p *GoPool) worker() {
	defer func() {
		p.wg.Done()
		atomic.AddInt32(&p.workerCount, -1)
		if p.options.EnableStats {
			atomic.AddInt64(&p.stats.WorkersDestroyed, 1)
			atomic.StoreInt32(&p.stats.CurrentWorkers, atomic.LoadInt32(&p.workerCount))
		}
	}()

	idleTimer := time.NewTimer(p.options.IdleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case task, ok := <-p.taskQueue:
			if !ok {
				return // 任务队列已关闭
			}

			// 重置空闲定时器
			if !idleTimer.Stop() {
				<-idleTimer.C
			}
			idleTimer.Reset(p.options.IdleTimeout)

			// 执行任务
			p.executeTask(task)

		case <-idleTimer.C:
			// 空闲超时，检查是否可以退出
			if atomic.LoadInt32(&p.workerCount) > int32(p.options.MinWorkers) {
				return
			}
			idleTimer.Reset(p.options.IdleTimeout)

		case <-p.ctx.Done():
			return
		}
	}
}

// executeTask 执行任务
func (p *GoPool) executeTask(task func()) {
	var startTime time.Time
	if p.options.EnableStats {
		startTime = time.Now()
		atomic.AddInt32(&p.stats.QueueLength, -1)
	}

	defer func() {
		if r := recover(); r != nil {
			if p.options.EnableStats {
				atomic.AddInt64(&p.stats.TasksFailed, 1)
			}
		} else {
			if p.options.EnableStats {
				atomic.AddInt64(&p.stats.TasksCompleted, 1)

				// 更新平均执行时间
				duration := time.Since(startTime)
				totalDuration := time.Duration(atomic.LoadInt64((*int64)(&p.stats.TotalDuration)))
				totalTasks := atomic.LoadInt64(&p.stats.TasksCompleted)
				avgDuration := (totalDuration + duration) / time.Duration(totalTasks)
				atomic.StoreInt64((*int64)(&p.stats.AvgTaskDuration), int64(avgDuration))
				atomic.StoreInt64((*int64)(&p.stats.TotalDuration), int64(totalDuration+duration))
			}
		}
	}()

	task()
}

// monitor 监控协程
func (p *GoPool) monitor() {
	defer p.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 可以在这里添加监控逻辑，如日志记录、指标上报等
		case <-p.ctx.Done():
			return
		}
	}
}

// GetStats 获取统计信息
func (p *GoPool) GetStats() *GoPoolStats {
	if !p.options.EnableStats {
		return nil
	}

	// 返回统计信息的拷贝
	return &GoPoolStats{
		TasksSubmitted:   atomic.LoadInt64(&p.stats.TasksSubmitted),
		TasksCompleted:   atomic.LoadInt64(&p.stats.TasksCompleted),
		TasksFailed:      atomic.LoadInt64(&p.stats.TasksFailed),
		WorkersCreated:   atomic.LoadInt64(&p.stats.WorkersCreated),
		WorkersDestroyed: atomic.LoadInt64(&p.stats.WorkersDestroyed),
		CurrentWorkers:   atomic.LoadInt32(&p.stats.CurrentWorkers),
		QueueLength:      atomic.LoadInt32(&p.stats.QueueLength),
		QueuePeak:        atomic.LoadInt32(&p.stats.QueuePeak),
		AvgTaskDuration:  time.Duration(atomic.LoadInt64((*int64)(&p.stats.AvgTaskDuration))),
		TotalDuration:    time.Duration(atomic.LoadInt64((*int64)(&p.stats.TotalDuration))),
	}
}

// IsRunning 检查是否运行中
func (p *GoPool) IsRunning() bool {
	return atomic.LoadInt32(&p.running) == 1
}

// GetWorkerCount 获取当前工作协程数
func (p *GoPool) GetWorkerCount() int {
	return int(atomic.LoadInt32(&p.workerCount))
}

// GetQueueLength 获取当前队列长度
func (p *GoPool) GetQueueLength() int {
	return int(atomic.LoadInt32(&p.stats.QueueLength))
}
