package engine

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/net/listener"
	"liveChatroom/util/net/net/reactor"
	"liveChatroom/util/net/protocol"
	"sync"
	"sync/atomic"
	"time"
)

// EventHandler 事件处理器接口
type EventHandler interface {
	// OnConnection 新连接建立
	OnConnection(conn *connection.Connection) error

	// OnMessage 收到消息
	OnMessage(conn *connection.Connection, msg protocol.Message) error

	// OnClose 连接关闭
	OnClose(conn *connection.Connection)

	// OnError 发生错误
	OnError(conn *connection.Connection, err error)

	// OnUpgrade 协议升级
	OnUpgrade(conn *connection.Connection, newProtocol protocol.Protocol) error
}

// Config 引擎配置
type Config struct {
	// 网络配置
	ListenerCount int // 监听器数量（多端口）
	ReactorCount  int // 反应器数量
	WorkerCount   int // 每个反应器的工作协程数

	// 连接配置
	MaxConnections    int           // 最大连接数
	ConnectionTimeout time.Duration // 连接超时

	// 性能配置
	EventQueueSize int           // 事件队列大小
	EpollTimeout   time.Duration // Epoll超时
	MaxEvents      int           // 最大事件数

	// 缓冲区配置
	ReadBufferSize  int // 读缓冲区大小
	WriteBufferSize int // 写缓冲区大小
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		ListenerCount:     1,
		ReactorCount:      4,
		WorkerCount:       4,
		MaxConnections:    10000,
		ConnectionTimeout: 30 * time.Second,
		EventQueueSize:    1024,
		EpollTimeout:      10 * time.Millisecond,
		MaxEvents:         1024,
		ReadBufferSize:    8192,
		WriteBufferSize:   8192,
	}
}

// Engine 网络引擎 - 整合所有网络组件的高级抽象
type Engine struct {
	mu sync.RWMutex

	// 配置
	config *Config

	// 核心组件
	listeners []*listener.Listener
	reactors  []*reactor.Reactor

	// 负载均衡
	nextReactor int64 // 轮询索引

	// 事件处理
	eventHandler EventHandler

	// 状态管理
	running          int32
	stopping         int32
	totalConnections int64
	totalMessages    int64
	totalErrors      int64

	// 协程管理
	wg       sync.WaitGroup
	stopChan chan struct{}

	// 生命周期管理
	startTime time.Time
}

// NewEngine 创建网络引擎
func NewEngine(config *Config) (*Engine, error) {
	if config == nil {
		config = DefaultConfig()
	}

	engine := &Engine{
		config:    config,
		listeners: make([]*listener.Listener, 0, config.ListenerCount),
		reactors:  make([]*reactor.Reactor, 0, config.ReactorCount),
		stopChan:  make(chan struct{}),
	}

	// 初始化反应器
	err := engine.initReactors()
	if err != nil {
		return nil, fmt.Errorf("init reactors failed: %v", err)
	}

	return engine, nil
}

// SetEventHandler 设置事件处理器
func (e *Engine) SetEventHandler(handler EventHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.eventHandler = handler
}

// ==================== 启动和停止 ====================

// Start 启动引擎
func (e *Engine) Start() error {
	if !atomic.CompareAndSwapInt32(&e.running, 0, 1) {
		return fmt.Errorf("engine already running")
	}

	e.startTime = time.Now()

	// 启动反应器
	for _, r := range e.reactors {
		if err := r.Start(); err != nil {
			e.Stop()
			return fmt.Errorf("start reactor failed: %v", err)
		}
	}

	// 启动监听器
	for _, l := range e.listeners {
		if err := l.Listen(); err != nil {
			e.Stop()
			return fmt.Errorf("start listener failed: %v", err)
		}
	}

	return nil
}

// Stop 停止引擎
func (e *Engine) Stop() error {
	if !atomic.CompareAndSwapInt32(&e.stopping, 0, 1) {
		return nil // 已经在停止
	}

	// 关闭停止信号
	close(e.stopChan)

	// 停止监听器
	for _, l := range e.listeners {
		l.Stop()
	}

	// 停止反应器
	for _, r := range e.reactors {
		r.Stop()
	}

	// 等待协程结束
	e.wg.Wait()

	atomic.StoreInt32(&e.running, 0)
	return nil
}

// ==================== 监听器管理 ====================

// Listen 在指定地址开始监听
func (e *Engine) Listen(address string) error {
	config := listener.DefaultConfig()
	config.Address = address
	config.MaxConnections = e.config.MaxConnections / len(e.reactors) // 平均分配

	return e.ListenWithConfig(config)
}

// ListenWithConfig 使用配置监听
func (e *Engine) ListenWithConfig(config *listener.Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if atomic.LoadInt32(&e.running) == 1 {
		return fmt.Errorf("cannot add listener when engine is running")
	}

	// 创建监听器
	l, err := listener.NewListener(config)
	if err != nil {
		return fmt.Errorf("create listener failed: %v", err)
	}

	// 设置事件处理器
	l.SetAcceptHandler(&engineAcceptHandler{engine: e})

	// 添加到监听器列表
	e.listeners = append(e.listeners, l)

	return nil
}

// ==================== 初始化 ====================

// initReactors 初始化反应器
func (e *Engine) initReactors() error {
	for i := 0; i < e.config.ReactorCount; i++ {
		// 创建反应器
		r, err := reactor.NewReactor(e.config.WorkerCount)
		if err != nil {
			return fmt.Errorf("create reactor %d failed: %v", i, err)
		}

		// 配置反应器
		r.SetConfig(e.config.MaxEvents, e.config.EventQueueSize, e.config.EpollTimeout)

		// 设置事件处理器
		r.SetEventHandler(&engineEventHandler{engine: e})

		e.reactors = append(e.reactors, r)
	}

	return nil
}

// ==================== 负载均衡 ====================

// getNextReactor 获取下一个反应器（轮询）
func (e *Engine) getNextReactor() *reactor.Reactor {
	if len(e.reactors) == 0 {
		return nil
	}

	idx := atomic.AddInt64(&e.nextReactor, 1) % int64(len(e.reactors))
	return e.reactors[idx]
}

// ==================== 统计和信息 ====================

// GetStats 获取引擎统计信息
func (e *Engine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 汇总所有反应器的统计
	reactorStats := make([]map[string]interface{}, len(e.reactors))
	totalConnections := int64(0)
	for i, r := range e.reactors {
		stats := r.GetStats()
		reactorStats[i] = stats
		if count, ok := stats["connection_count"].(int64); ok {
			totalConnections += count
		}
	}

	// 汇总所有监听器的统计
	listenerStats := make([]map[string]interface{}, len(e.listeners))
	for i, l := range e.listeners {
		listenerStats[i] = l.GetStats()
	}

	uptime := time.Duration(0)
	if !e.startTime.IsZero() {
		uptime = time.Since(e.startTime)
	}

	return map[string]interface{}{
		"running":           e.IsRunning(),
		"stopping":          e.IsStopping(),
		"uptime_seconds":    uptime.Seconds(),
		"reactor_count":     len(e.reactors),
		"listener_count":    len(e.listeners),
		"total_connections": totalConnections,
		"total_messages":    atomic.LoadInt64(&e.totalMessages),
		"total_errors":      atomic.LoadInt64(&e.totalErrors),
		"reactor_stats":     reactorStats,
		"listener_stats":    listenerStats,
		"config":            e.config,
	}
}

// GetConnectionCount 获取总连接数
func (e *Engine) GetConnectionCount() int64 {
	total := int64(0)
	for _, r := range e.reactors {
		total += r.GetConnectionCount()
	}
	return total
}

// GetMessageCount 获取总消息数
func (e *Engine) GetMessageCount() int64 {
	return atomic.LoadInt64(&e.totalMessages)
}

// GetErrorCount 获取总错误数
func (e *Engine) GetErrorCount() int64 {
	return atomic.LoadInt64(&e.totalErrors)
}

// IsRunning 检查引擎是否运行中
func (e *Engine) IsRunning() bool {
	return atomic.LoadInt32(&e.running) == 1
}

// IsStopping 检查引擎是否正在停止
func (e *Engine) IsStopping() bool {
	return atomic.LoadInt32(&e.stopping) == 1
}

// GetUptime 获取运行时间
func (e *Engine) GetUptime() time.Duration {
	if e.startTime.IsZero() {
		return 0
	}
	return time.Since(e.startTime)
}

// ==================== 广播功能 ====================

// Broadcast 向所有连接广播数据
func (e *Engine) Broadcast(data []byte, filter func(*connection.Connection) bool) {
	for _, r := range e.reactors {
		r.Broadcast(data, filter)
	}
}

// BroadcastByProtocol 根据协议类型广播消息
func (e *Engine) BroadcastByProtocol(protocolType protocol.ProtocolType, msg protocol.Message, filter func(*connection.Connection) bool) {
	for _, r := range e.reactors {
		r.BroadcastByProtocol(protocolType, msg, filter)
	}
}

// BroadcastToReactor 向指定反应器广播
func (e *Engine) BroadcastToReactor(reactorIndex int, data []byte, filter func(*connection.Connection) bool) error {
	if reactorIndex < 0 || reactorIndex >= len(e.reactors) {
		return fmt.Errorf("reactor index out of range: %d", reactorIndex)
	}

	e.reactors[reactorIndex].Broadcast(data, filter)
	return nil
}

// ==================== 连接查找 ====================

// GetAllConnections 获取所有连接
func (e *Engine) GetAllConnections() []*connection.Connection {
	var allConnections []*connection.Connection

	for _, r := range e.reactors {
		connections := r.GetAllConnections()
		allConnections = append(allConnections, connections...)
	}

	return allConnections
}

// FindConnections 查找匹配的连接
func (e *Engine) FindConnections(filter func(*connection.Connection) bool) []*connection.Connection {
	var matchedConnections []*connection.Connection

	for _, conn := range e.GetAllConnections() {
		if filter(conn) {
			matchedConnections = append(matchedConnections, conn)
		}
	}

	return matchedConnections
}

// ==================== 事件处理器实现 ====================

// engineAcceptHandler 引擎接受连接处理器
type engineAcceptHandler struct {
	engine *Engine
}

// OnAccept 处理新连接
func (h *engineAcceptHandler) OnAccept(conn *connection.Connection) error {
	// 选择反应器
	r := h.engine.getNextReactor()
	if r == nil {
		return fmt.Errorf("no available reactor")
	}

	// 设置连接事件处理器
	h.setupConnectionHandlers(conn)

	// 添加到反应器
	if err := r.AddConnection(conn); err != nil {
		return fmt.Errorf("add connection to reactor failed: %v", err)
	}

	// 调用用户处理器
	if h.engine.eventHandler != nil {
		if err := h.engine.eventHandler.OnConnection(conn); err != nil {
			r.RemoveConnection(conn)
			return err
		}
	}

	atomic.AddInt64(&h.engine.totalConnections, 1)
	return nil
}

// OnError 处理错误
func (h *engineAcceptHandler) OnError(err error) {
	atomic.AddInt64(&h.engine.totalErrors, 1)

	if h.engine.eventHandler != nil {
		h.engine.eventHandler.OnError(nil, err)
	}
}

// setupConnectionHandlers 设置连接事件处理器
func (h *engineAcceptHandler) setupConnectionHandlers(conn *connection.Connection) {
	// 消息处理器
	conn.OnMessage(func(c *connection.Connection, msg protocol.Message) {
		atomic.AddInt64(&h.engine.totalMessages, 1)

		if h.engine.eventHandler != nil {
			if err := h.engine.eventHandler.OnMessage(c, msg); err != nil {
				h.engine.eventHandler.OnError(c, err)
			}
		}
	})

	// 关闭处理器
	conn.OnClose(func(c *connection.Connection) {
		if h.engine.eventHandler != nil {
			h.engine.eventHandler.OnClose(c)
		}
		atomic.AddInt64(&h.engine.totalConnections, -1)
	})

	// 错误处理器
	conn.OnError(func(c *connection.Connection, err error) {
		atomic.AddInt64(&h.engine.totalErrors, 1)

		if h.engine.eventHandler != nil {
			h.engine.eventHandler.OnError(c, err)
		}
	})

	// 协议升级处理器
	conn.OnUpgrade(func(c *connection.Connection, newProtocol protocol.Protocol) {
		if h.engine.eventHandler != nil {
			if err := h.engine.eventHandler.OnUpgrade(c, newProtocol); err != nil {
				h.engine.eventHandler.OnError(c, err)
			}
		}
	})
}

// engineEventHandler 引擎反应器事件处理器
type engineEventHandler struct {
	engine *Engine
}

// OnRead 处理读取事件
func (h *engineEventHandler) OnRead(conn *connection.Connection) error {
	return conn.Read()
}

// OnWrite 处理写入事件
func (h *engineEventHandler) OnWrite(conn *connection.Connection) error {
	// 写入缓冲区数据
	return conn.Flush()
}

// OnError 处理错误事件
func (h *engineEventHandler) OnError(conn *connection.Connection, err error) {
	atomic.AddInt64(&h.engine.totalErrors, 1)

	if h.engine.eventHandler != nil {
		h.engine.eventHandler.OnError(conn, err)
	}
}

// OnClose 处理关闭事件
func (h *engineEventHandler) OnClose(conn *connection.Connection) {
	if h.engine.eventHandler != nil {
		h.engine.eventHandler.OnClose(conn)
	}
}

// OnConnect 处理连接事件（客户端）
func (h *engineEventHandler) OnConnect(conn *connection.Connection) error {
	if h.engine.eventHandler != nil {
		return h.engine.eventHandler.OnConnection(conn)
	}
	return nil
}

// OnAccept 处理接受连接事件（服务端）
func (h *engineEventHandler) OnAccept(conn *connection.Connection) error {
	if h.engine.eventHandler != nil {
		return h.engine.eventHandler.OnConnection(conn)
	}
	return nil
}

// ==================== 便利创建方法 ====================

// NewTCPEngine 创建TCP引擎
func NewTCPEngine(address string) (*Engine, error) {
	engine, err := NewEngine(DefaultConfig())
	if err != nil {
		return nil, err
	}

	// 添加TCP监听
	err = engine.Listen(address)
	if err != nil {
		return nil, err
	}

	return engine, nil
}

// NewMultiPortEngine 创建多端口引擎
func NewMultiPortEngine(addresses []string) (*Engine, error) {
	config := DefaultConfig()
	config.ListenerCount = len(addresses)

	engine, err := NewEngine(config)
	if err != nil {
		return nil, err
	}

	// 添加多个监听器
	for _, address := range addresses {
		err = engine.Listen(address)
		if err != nil {
			return nil, err
		}
	}

	return engine, nil
}
