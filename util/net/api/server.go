package api

import (
	"context"
	"fmt"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/net/engine"
	"liveChatroom/util/net/protocol"
	"sync"
	"sync/atomic"
	"time"
)

// Server 网络服务器 - 纯底层网络功能
type Server struct {
	// 网络引擎
	engine *engine.Engine

	// 事件处理器
	eventHandler EventHandler

	// 配置
	config *ServerConfig

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 状态
	running int32 // 原子操作

	// 统计
	stats *ServerStats
}

// ServerConfig 服务器配置
type ServerConfig struct {
	// 网络配置
	Address        string        // 监听地址
	MaxConnections int           // 最大连接数
	ReadTimeout    time.Duration // 读取超时
	WriteTimeout   time.Duration // 写入超时
	IdleTimeout    time.Duration // 空闲超时

	// 引擎配置
	ReactorCount int // 反应器数量
	WorkerCount  int // 工作协程数

	// 缓冲区配置
	ReadBufferSize  int // 读取缓冲区大小
	WriteBufferSize int // 写入缓冲区大小
}

// ServerStats 服务器统计信息
type ServerStats struct {
	StartTime         time.Time
	ActiveConnections int64
	TotalConnections  int64
	MessagesReceived  int64
	MessagesSent      int64
	BytesReceived     int64
	BytesSent         int64
	LastActivity      time.Time
}

// EventHandler 事件处理器接口
type EventHandler interface {
	// 连接事件
	OnConnectionAccepted(conn *connection.Connection)
	OnConnectionClosed(conn *connection.Connection, err error)

	// 消息事件
	OnMessageReceived(conn *connection.Connection, msg protocol.Message) error

	// 错误事件
	OnError(err error)
}

// DefaultServerConfig 默认服务器配置
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Address:         ":8080",
		MaxConnections:  10000,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     5 * time.Minute,
		ReactorCount:    2,
		WorkerCount:     4,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}
}

// NewServer 创建网络服务器
func NewServer(config *ServerConfig, handler EventHandler) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}

	if handler == nil {
		return nil, fmt.Errorf("event handler is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 创建引擎配置
	engineConfig := &engine.Config{
		ReactorCount:      config.ReactorCount,
		WorkerCount:       config.WorkerCount,
		MaxConnections:    config.MaxConnections,
		ConnectionTimeout: config.ReadTimeout,
		ReadBufferSize:    config.ReadBufferSize,
		WriteBufferSize:   config.WriteBufferSize,
	}

	// 创建网络引擎
	eng, err := engine.NewEngine(engineConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create engine: %v", err)
	}

	server := &Server{
		engine:       eng,
		eventHandler: handler,
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
		stats:        &ServerStats{StartTime: time.Now()},
	}

	// 设置引擎事件处理器
	eng.SetEventHandler(&engineEventHandler{server: server})

	return server, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	// 添加监听地址
	if err := s.engine.Listen(s.config.Address); err != nil {
		atomic.StoreInt32(&s.running, 0)
		return fmt.Errorf("failed to listen on %s: %v", s.config.Address, err)
	}

	// 启动引擎
	if err := s.engine.Start(); err != nil {
		atomic.StoreInt32(&s.running, 0)
		return fmt.Errorf("failed to start engine: %v", err)
	}

	// 启动统计更新
	s.wg.Add(1)
	go s.updateStats()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return fmt.Errorf("server not running")
	}

	// 取消上下文
	s.cancel()

	// 停止引擎
	err := s.engine.Stop()

	// 等待后台任务结束
	s.wg.Wait()

	return err
}

// updateStats 更新统计信息
func (s *Server) updateStats() {
	defer s.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return

		case <-ticker.C:
			s.collectStats()
		}
	}
}

// collectStats 收集统计信息
func (s *Server) collectStats() {
	if engineStats := s.engine.GetStats(); engineStats != nil {
		if val, ok := engineStats["active_connections"].(int64); ok {
			atomic.StoreInt64(&s.stats.ActiveConnections, val)
		}
		if val, ok := engineStats["total_connections"].(int64); ok {
			atomic.StoreInt64(&s.stats.TotalConnections, val)
		}
		if val, ok := engineStats["messages_received"].(int64); ok {
			atomic.StoreInt64(&s.stats.MessagesReceived, val)
		}
		if val, ok := engineStats["messages_sent"].(int64); ok {
			atomic.StoreInt64(&s.stats.MessagesSent, val)
		}
		if val, ok := engineStats["bytes_received"].(int64); ok {
			atomic.StoreInt64(&s.stats.BytesReceived, val)
		}
		if val, ok := engineStats["bytes_sent"].(int64); ok {
			atomic.StoreInt64(&s.stats.BytesSent, val)
		}
		if val, ok := engineStats["last_activity"].(time.Time); ok {
			s.stats.LastActivity = val
		}
	}
}

// IsRunning 检查是否运行中
func (s *Server) IsRunning() bool {
	return atomic.LoadInt32(&s.running) == 1
}

// GetStats 获取统计信息
func (s *Server) GetStats() *ServerStats {
	stats := *s.stats
	stats.ActiveConnections = atomic.LoadInt64(&s.stats.ActiveConnections)
	stats.TotalConnections = atomic.LoadInt64(&s.stats.TotalConnections)
	stats.MessagesReceived = atomic.LoadInt64(&s.stats.MessagesReceived)
	stats.MessagesSent = atomic.LoadInt64(&s.stats.MessagesSent)
	stats.BytesReceived = atomic.LoadInt64(&s.stats.BytesReceived)
	stats.BytesSent = atomic.LoadInt64(&s.stats.BytesSent)
	return &stats
}

// GetActiveConnections 获取活跃连接数
func (s *Server) GetActiveConnections() int64 {
	return atomic.LoadInt64(&s.stats.ActiveConnections)
}

// GetUptime 获取运行时间
func (s *Server) GetUptime() time.Duration {
	return time.Since(s.stats.StartTime)
}

// Broadcast 广播消息给所有连接
func (s *Server) Broadcast(data []byte) {
	if !s.IsRunning() {
		return
	}

	s.engine.Broadcast(data, nil) // nil filter means broadcast to all
}

// GetActiveConnectionCount 获取活跃连接数
func (s *Server) GetActiveConnectionCount() int64 {
	return s.GetActiveConnections()
}

// BroadcastWithFilter 带过滤器的广播
func (s *Server) BroadcastWithFilter(data []byte, filter func(*connection.Connection) bool) {
	if !s.IsRunning() {
		return
	}

	s.engine.Broadcast(data, filter)
}

// engineEventHandler 引擎事件处理器适配器
type engineEventHandler struct {
	server *Server
}

func (h *engineEventHandler) OnConnection(conn *connection.Connection) error {
	atomic.AddInt64(&h.server.stats.TotalConnections, 1)
	h.server.eventHandler.OnConnectionAccepted(conn)
	return nil
}

func (h *engineEventHandler) OnMessage(conn *connection.Connection, msg protocol.Message) error {
	atomic.AddInt64(&h.server.stats.MessagesReceived, 1)
	h.server.stats.LastActivity = time.Now()
	return h.server.eventHandler.OnMessageReceived(conn, msg)
}

func (h *engineEventHandler) OnClose(conn *connection.Connection) {
	h.server.eventHandler.OnConnectionClosed(conn, nil)
}

func (h *engineEventHandler) OnError(conn *connection.Connection, err error) {
	h.server.eventHandler.OnError(err)
}

func (h *engineEventHandler) OnUpgrade(conn *connection.Connection, newProtocol protocol.Protocol) error {
	// 协议升级暂不处理
	return nil
}
