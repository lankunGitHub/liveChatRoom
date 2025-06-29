package reactor

import (
	"fmt"
	"liveChatroom/util/net/base/epoll"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"sync"
	"sync/atomic"
	"time"
)

// EventType 事件类型
type EventType int

const (
	EventRead EventType = iota
	EventWrite
	EventError
	EventClose
	EventConnect
	EventAccept
)

// Event 事件结构
type Event struct {
	Type       EventType
	FD         int
	Connection *connection.Connection
	Data       interface{}
	Error      error
}

// EventHandler 事件处理器接口
type EventHandler interface {
	// OnRead 处理读取事件
	OnRead(conn *connection.Connection) error

	// OnWrite 处理写入事件
	OnWrite(conn *connection.Connection) error

	// OnError 处理错误事件
	OnError(conn *connection.Connection, err error)

	// OnClose 处理关闭事件
	OnClose(conn *connection.Connection)

	// OnConnect 处理连接事件（客户端）
	OnConnect(conn *connection.Connection) error

	// OnAccept 处理接受连接事件（服务端）
	OnAccept(conn *connection.Connection) error
}

// Reactor 事件反应器 - 高性能事件驱动核心
type Reactor struct {
	mu sync.RWMutex

	// epoll实例
	epoll *epoll.Epoll

	// 连接管理
	connManager *connection.Manager
	connections map[int]*connection.Connection // fd -> connection

	// 事件处理
	eventHandler EventHandler
	eventQueue   chan *Event

	// 状态管理
	running     int32
	stopping    int32
	workerCount int

	// 配置
	maxEvents      int
	eventQueueSize int
	epollTimeout   time.Duration

	// 统计
	eventCount      int64
	errorCount      int64
	connectionCount int64

	// 协程管理
	wg       sync.WaitGroup
	stopChan chan struct{}
}

// NewReactor 创建事件反应器
func NewReactor(workerCount int) (*Reactor, error) {
	if workerCount <= 0 {
		workerCount = 4 // 默认4个工作协程
	}

	// 创建epoll实例
	ep, err := epoll.New()
	if err != nil {
		return nil, fmt.Errorf("create epoll failed: %v", err)
	}

	reactor := &Reactor{
		epoll:          ep,
		connManager:    connection.NewManager(10000),
		connections:    make(map[int]*connection.Connection),
		eventQueue:     make(chan *Event, 1024),
		workerCount:    workerCount,
		maxEvents:      1024,
		eventQueueSize: 1024,
		epollTimeout:   10 * time.Millisecond,
		stopChan:       make(chan struct{}),
	}

	// 启动连接管理器
	reactor.connManager.Start()

	return reactor, nil
}

// SetEventHandler 设置事件处理器
func (r *Reactor) SetEventHandler(handler EventHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.eventHandler = handler
}

// SetConfig 设置配置
func (r *Reactor) SetConfig(maxEvents, queueSize int, timeout time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if maxEvents > 0 {
		r.maxEvents = maxEvents
	}
	if queueSize > 0 {
		r.eventQueueSize = queueSize
	}
	if timeout > 0 {
		r.epollTimeout = timeout
	}
}

// Start 启动反应器
func (r *Reactor) Start() error {
	if !atomic.CompareAndSwapInt32(&r.running, 0, 1) {
		return fmt.Errorf("reactor already running")
	}

	// 启动事件循环
	r.wg.Add(1)
	go r.eventLoop()

	// 启动工作协程
	for i := 0; i < r.workerCount; i++ {
		r.wg.Add(1)
		go r.worker(i)
	}

	return nil
}

// Stop 停止反应器
func (r *Reactor) Stop() error {
	if !atomic.CompareAndSwapInt32(&r.stopping, 0, 1) {
		return nil // 已经在停止
	}

	// 停止事件循环
	close(r.stopChan)

	// 等待所有协程结束
	r.wg.Wait()

	// 清理资源
	r.cleanup()

	atomic.StoreInt32(&r.running, 0)
	return nil
}

// AddConnection 添加连接到反应器
func (r *Reactor) AddConnection(conn *connection.Connection) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if atomic.LoadInt32(&r.stopping) == 1 {
		return fmt.Errorf("reactor is stopping")
	}

	fd := conn.FD()

	// 检查连接是否已存在
	if _, exists := r.connections[fd]; exists {
		return fmt.Errorf("connection fd %d already exists", fd)
	}

	// 添加到epoll
	err := r.epoll.Add(fd, epoll.EPOLLIN|epoll.EPOLLET)
	if err != nil {
		return fmt.Errorf("add fd to epoll failed: %v", err)
	}

	// 添加到连接映射
	r.connections[fd] = conn

	// 添加到连接管理器
	err = r.connManager.AddConnection(conn)
	if err != nil {
		// 回滚操作
		r.epoll.Del(fd)
		delete(r.connections, fd)
		return fmt.Errorf("add connection to manager failed: %v", err)
	}

	atomic.AddInt64(&r.connectionCount, 1)
	return nil
}

// RemoveConnection 从反应器移除连接
func (r *Reactor) RemoveConnection(conn *connection.Connection) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	fd := conn.FD()

	// 检查连接是否存在
	if _, exists := r.connections[fd]; !exists {
		return fmt.Errorf("connection fd %d not found", fd)
	}

	// 从epoll移除
	r.epoll.Del(fd)

	// 从连接映射移除
	delete(r.connections, fd)

	// 从连接管理器移除
	r.connManager.RemoveConnection(conn)

	atomic.AddInt64(&r.connectionCount, -1)
	return nil
}

// EnableWrite 启用写事件监听
func (r *Reactor) EnableWrite(conn *connection.Connection) error {
	fd := conn.FD()
	return r.epoll.Mod(fd, epoll.EPOLLIN|epoll.EPOLLOUT|epoll.EPOLLET)
}

// DisableWrite 禁用写事件监听
func (r *Reactor) DisableWrite(conn *connection.Connection) error {
	fd := conn.FD()
	return r.epoll.Mod(fd, epoll.EPOLLIN|epoll.EPOLLET)
}

// GetConnection 获取连接
func (r *Reactor) GetConnection(fd int) *connection.Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.connections[fd]
}

// GetAllConnections 获取所有连接
func (r *Reactor) GetAllConnections() []*connection.Connection {
	return r.connManager.GetAllConnections()
}

// GetConnectionCount 获取连接数量
func (r *Reactor) GetConnectionCount() int64 {
	return atomic.LoadInt64(&r.connectionCount)
}

// ==================== 事件循环 ====================

// eventLoop 主事件循环
func (r *Reactor) eventLoop() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopChan:
			return
		default:
		}

		// 等待事件
		n, err := r.epoll.Wait(int(r.epollTimeout.Milliseconds()))
		if err != nil {
			if atomic.LoadInt32(&r.stopping) == 0 {
				atomic.AddInt64(&r.errorCount, 1)
				// 处理epoll错误
				r.handleEpollError(err)
			}
			continue
		}

		// 处理事件
		events := r.epoll.GetEvents()
		for i := 0; i < n; i++ {
			r.processEvent(&events[i])
		}
	}
}

// processEvent 处理单个epoll事件
func (r *Reactor) processEvent(epEvent *epoll.Event) {
	fd := int(epEvent.Data) // fd存储在Data字段中

	// 获取连接
	r.mu.RLock()
	conn, exists := r.connections[fd]
	r.mu.RUnlock()

	if !exists {
		// 连接不存在，可能已经被关闭
		return
	}

	// 检查事件类型
	if epEvent.Events&(epoll.EPOLLERR|epoll.EPOLLHUP) != 0 {
		// 错误或挂起事件
		event := &Event{
			Type:       EventError,
			FD:         fd,
			Connection: conn,
			Error:      fmt.Errorf("socket error or hangup"),
		}
		r.enqueueEvent(event)
		return
	}

	// 读事件
	if epEvent.Events&epoll.EPOLLIN != 0 {
		event := &Event{
			Type:       EventRead,
			FD:         fd,
			Connection: conn,
		}
		r.enqueueEvent(event)
	}

	// 写事件
	if epEvent.Events&epoll.EPOLLOUT != 0 {
		event := &Event{
			Type:       EventWrite,
			FD:         fd,
			Connection: conn,
		}
		r.enqueueEvent(event)
	}

	atomic.AddInt64(&r.eventCount, 1)
}

// enqueueEvent 将事件加入队列
func (r *Reactor) enqueueEvent(event *Event) {
	select {
	case r.eventQueue <- event:
		// 事件入队成功
	default:
		// 队列满，丢弃事件并记录错误
		atomic.AddInt64(&r.errorCount, 1)
	}
}

// ==================== 工作协程 ====================

// worker 事件处理工作协程
func (r *Reactor) worker(id int) {
	defer r.wg.Done()

	for {
		select {
		case event := <-r.eventQueue:
			r.handleEvent(event)
		case <-r.stopChan:
			return
		}
	}
}

// handleEvent 处理事件
func (r *Reactor) handleEvent(event *Event) {
	if r.eventHandler == nil {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			atomic.AddInt64(&r.errorCount, 1)
			// 记录panic，但不中断服务
		}
	}()

	switch event.Type {
	case EventRead:
		err := r.eventHandler.OnRead(event.Connection)
		if err != nil {
			r.eventHandler.OnError(event.Connection, err)
		}

	case EventWrite:
		err := r.eventHandler.OnWrite(event.Connection)
		if err != nil {
			r.eventHandler.OnError(event.Connection, err)
		}

	case EventError:
		r.eventHandler.OnError(event.Connection, event.Error)

	case EventClose:
		r.eventHandler.OnClose(event.Connection)

	case EventConnect:
		err := r.eventHandler.OnConnect(event.Connection)
		if err != nil {
			r.eventHandler.OnError(event.Connection, err)
		}

	case EventAccept:
		err := r.eventHandler.OnAccept(event.Connection)
		if err != nil {
			r.eventHandler.OnError(event.Connection, err)
		}
	}
}

// handleEpollError 处理epoll错误
func (r *Reactor) handleEpollError(err error) {
	// 可以在这里记录日志或进行错误恢复
	// 暂时简单处理
}

// ==================== 清理和统计 ====================

// cleanup 清理资源
func (r *Reactor) cleanup() {
	// 关闭epoll
	if r.epoll != nil {
		r.epoll.Close()
	}

	// 停止连接管理器
	if r.connManager != nil {
		r.connManager.Stop()
	}

	// 清理连接映射
	r.mu.Lock()
	for fd, conn := range r.connections {
		conn.Close()
		delete(r.connections, fd)
	}
	r.mu.Unlock()

	// 清理事件队列
	close(r.eventQueue)
	for range r.eventQueue {
		// 清空队列
	}
}

// GetStats 获取反应器统计信息
func (r *Reactor) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return map[string]interface{}{
		"running":          atomic.LoadInt32(&r.running) == 1,
		"stopping":         atomic.LoadInt32(&r.stopping) == 1,
		"worker_count":     r.workerCount,
		"connection_count": atomic.LoadInt64(&r.connectionCount),
		"event_count":      atomic.LoadInt64(&r.eventCount),
		"error_count":      atomic.LoadInt64(&r.errorCount),
		"max_events":       r.maxEvents,
		"event_queue_size": r.eventQueueSize,
		"epoll_timeout_ms": r.epollTimeout.Milliseconds(),
		"queue_length":     len(r.eventQueue),
	}
}

// IsRunning 检查反应器是否运行中
func (r *Reactor) IsRunning() bool {
	return atomic.LoadInt32(&r.running) == 1
}

// IsStopping 检查反应器是否正在停止
func (r *Reactor) IsStopping() bool {
	return atomic.LoadInt32(&r.stopping) == 1
}

// ==================== 便利方法 ====================

// Broadcast 广播数据到所有连接
func (r *Reactor) Broadcast(data []byte, filter func(*connection.Connection) bool) {
	r.connManager.Broadcast(data, filter)
}

// BroadcastByProtocol 根据协议类型广播消息
func (r *Reactor) BroadcastByProtocol(protocolType protocol.ProtocolType, msg protocol.Message, filter func(*connection.Connection) bool) {
	r.connManager.BroadcastByProtocol(protocolType, msg, filter)
}

// CloseAllConnections 关闭所有连接
func (r *Reactor) CloseAllConnections() {
	r.connManager.CloseAllConnections()
}
