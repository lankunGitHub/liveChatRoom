package timer

import (
	"container/heap"
	"sync"
	"time"
)

// Timer 高精度定时器
type Timer struct {
	id       uint64
	deadline time.Time
	callback func()
	repeat   time.Duration
	canceled bool
}

// TimerWheel 时间轮定时器
type TimerWheel struct {
	mu      sync.RWMutex
	heap    *timerHeap
	nextID  uint64
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// timerHeap 定时器堆
type timerHeap []*Timer

// NewTimerWheel 创建时间轮
func NewTimerWheel() *TimerWheel {
	tw := &TimerWheel{
		heap:   &timerHeap{},
		nextID: 1,
		stopCh: make(chan struct{}),
	}
	heap.Init(tw.heap)
	return tw
}

// Start 启动定时器轮询
func (tw *TimerWheel) Start() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.running {
		return
	}

	tw.running = true
	tw.wg.Add(1)
	go tw.run()
}

// Stop 停止定时器
func (tw *TimerWheel) Stop() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if !tw.running {
		return
	}

	tw.running = false
	close(tw.stopCh)
	tw.wg.Wait()
}

// AddTimer 添加一次性定时器
func (tw *TimerWheel) AddTimer(delay time.Duration, callback func()) uint64 {
	return tw.addTimer(delay, 0, callback)
}

// AddRepeatingTimer 添加重复定时器
func (tw *TimerWheel) AddRepeatingTimer(delay, interval time.Duration, callback func()) uint64 {
	return tw.addTimer(delay, interval, callback)
}

// CancelTimer 取消定时器
func (tw *TimerWheel) CancelTimer(id uint64) bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 在堆中查找并标记为已取消
	for _, timer := range *tw.heap {
		if timer.id == id {
			timer.canceled = true
			return true
		}
	}
	return false
}

// addTimer 添加定时器
func (tw *TimerWheel) addTimer(delay, repeat time.Duration, callback func()) uint64 {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	id := tw.nextID
	tw.nextID++

	timer := &Timer{
		id:       id,
		deadline: time.Now().Add(delay),
		callback: callback,
		repeat:   repeat,
		canceled: false,
	}

	heap.Push(tw.heap, timer)
	return id
}

// run 运行定时器循环
func (tw *TimerWheel) run() {
	defer tw.wg.Done()

	ticker := time.NewTicker(10 * time.Millisecond) // 10ms精度
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			tw.processTick(now)
		case <-tw.stopCh:
			return
		}
	}
}

// processTick 处理定时器tick
func (tw *TimerWheel) processTick(now time.Time) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	for tw.heap.Len() > 0 {
		timer := (*tw.heap)[0]

		// 如果最近的定时器还没到时间，退出
		if timer.deadline.After(now) {
			break
		}

		// 弹出定时器
		heap.Pop(tw.heap)

		// 如果定时器已被取消，跳过
		if timer.canceled {
			continue
		}

		// 执行回调（在goroutine中以避免阻塞）
		go timer.callback()

		// 如果是重复定时器，重新添加
		if timer.repeat > 0 {
			timer.deadline = now.Add(timer.repeat)
			timer.canceled = false
			heap.Push(tw.heap, timer)
		}
	}
}

// ==================== 堆接口实现 ====================

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *timerHeap) Push(x interface{}) {
	*h = append(*h, x.(*Timer))
}

func (h *timerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// ==================== 简单定时器接口 ====================

var globalTimerWheel *TimerWheel
var globalTimerOnce sync.Once

// getGlobalTimer 获取全局定时器实例
func getGlobalTimer() *TimerWheel {
	globalTimerOnce.Do(func() {
		globalTimerWheel = NewTimerWheel()
		globalTimerWheel.Start()
	})
	return globalTimerWheel
}

// After 在指定时间后执行回调
func After(delay time.Duration, callback func()) uint64 {
	return getGlobalTimer().AddTimer(delay, callback)
}

// Every 每隔指定时间执行回调
func Every(interval time.Duration, callback func()) uint64 {
	return getGlobalTimer().AddRepeatingTimer(interval, interval, callback)
}

// Cancel 取消定时器
func Cancel(id uint64) bool {
	return getGlobalTimer().CancelTimer(id)
}

// ==================== 超时控制 ====================

// TimeoutContext 超时上下文
type TimeoutContext struct {
	timeout  time.Duration
	timer    uint64
	callback func()
	canceled bool
	mu       sync.Mutex
}

// NewTimeoutContext 创建超时上下文
func NewTimeoutContext(timeout time.Duration, callback func()) *TimeoutContext {
	ctx := &TimeoutContext{
		timeout:  timeout,
		callback: callback,
	}

	ctx.timer = After(timeout, func() {
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		if !ctx.canceled && ctx.callback != nil {
			ctx.callback()
		}
	})

	return ctx
}

// Cancel 取消超时
func (ctx *TimeoutContext) Cancel() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if !ctx.canceled {
		ctx.canceled = true
		Cancel(ctx.timer)
	}
}

// Reset 重置超时时间
func (ctx *TimeoutContext) Reset(timeout time.Duration) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.canceled {
		return
	}

	// 取消旧的定时器
	Cancel(ctx.timer)

	// 创建新的定时器
	ctx.timeout = timeout
	ctx.timer = After(timeout, func() {
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		if !ctx.canceled && ctx.callback != nil {
			ctx.callback()
		}
	})
}

// ==================== 性能监控定时器 ====================

// Ticker 高精度Ticker
type Ticker struct {
	C        chan time.Time
	interval time.Duration
	timer    uint64
	stopped  bool
	mu       sync.Mutex
}

// NewTicker 创建高精度Ticker
func NewTicker(interval time.Duration) *Ticker {
	t := &Ticker{
		C:        make(chan time.Time, 1),
		interval: interval,
	}

	t.timer = Every(interval, func() {
		t.mu.Lock()
		defer t.mu.Unlock()

		if !t.stopped {
			select {
			case t.C <- time.Now():
			default:
				// 非阻塞发送
			}
		}
	})

	return t
}

// Stop 停止Ticker
func (t *Ticker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.stopped {
		t.stopped = true
		Cancel(t.timer)
		close(t.C)
	}
}

// ==================== 延迟执行器 ====================

// DelayExecutor 延迟执行器
type DelayExecutor struct {
	tasks map[string]uint64
	mu    sync.RWMutex
}

// NewDelayExecutor 创建延迟执行器
func NewDelayExecutor() *DelayExecutor {
	return &DelayExecutor{
		tasks: make(map[string]uint64),
	}
}

// Schedule 调度任务
func (de *DelayExecutor) Schedule(key string, delay time.Duration, task func()) {
	de.mu.Lock()
	defer de.mu.Unlock()

	// 取消已存在的任务
	if oldTimer, exists := de.tasks[key]; exists {
		Cancel(oldTimer)
	}

	// 添加新任务
	timer := After(delay, func() {
		de.mu.Lock()
		delete(de.tasks, key)
		de.mu.Unlock()
		task()
	})

	de.tasks[key] = timer
}

// Cancel 取消任务
func (de *DelayExecutor) CancelTask(key string) bool {
	de.mu.Lock()
	defer de.mu.Unlock()

	if timer, exists := de.tasks[key]; exists {
		delete(de.tasks, key)
		return Cancel(timer)
	}
	return false
}

// CancelAll 取消所有任务
func (de *DelayExecutor) CancelAll() {
	de.mu.Lock()
	defer de.mu.Unlock()

	for key, timer := range de.tasks {
		Cancel(timer)
		delete(de.tasks, key)
	}
}
