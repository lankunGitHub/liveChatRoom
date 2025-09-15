package listener

import (
	"errors"
	"fmt"
	"liveChatroom/util/net/base/socket"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/net/reactor"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// AcceptHandler 接受连接处理器接口
type AcceptHandler interface {
	// OnAccept 处理新连接
	OnAccept(conn *connection.Connection) error

	// OnError 处理错误
	OnError(err error)
}

// Config 监听器配置
type Config struct {
	// 网络相关
	Network string // tcp, tcp4, tcp6
	Address string // 监听地址，如 ":8080"
	Backlog int    // 监听队列长度

	// Socket选项
	ReuseAddr bool // SO_REUSEADDR
	ReusePort bool // SO_REUSEPORT
	KeepAlive bool // SO_KEEPALIVE
	NoDelay   bool // TCP_NODELAY

	// 连接限制
	MaxConnections int           // 最大连接数
	AcceptTimeout  time.Duration // 接受连接超时

	// 缓冲区
	ReadBufferSize  int // 读缓冲区大小
	WriteBufferSize int // 写缓冲区大小
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Network:         "tcp",
		Backlog:         1024,
		ReuseAddr:       true,
		ReusePort:       false,
		KeepAlive:       true,
		NoDelay:         true,
		MaxConnections:  10000,
		AcceptTimeout:   30 * time.Second,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}
}

// Listener 网络监听器 - 高性能连接接受器
type Listener struct {
	mu sync.RWMutex

	// 配置
	config *Config

	// 监听socket
	socket   *socket.Socket
	listener net.Listener

	// 反应器
	reactor *reactor.Reactor

	// 连接处理
	acceptHandler AcceptHandler

	// 状态管理
	running         int32
	stopping        int32
	acceptCount     int64
	errorCount      int64
	connectionCount int64

	// 协程管理
	wg       sync.WaitGroup
	stopChan chan struct{}
}

// NewListener 创建网络监听器
func NewListener(config *Config) (*Listener, error) {
	if config == nil {
		config = DefaultConfig()
	}

	listener := &Listener{
		config:   config,
		stopChan: make(chan struct{}),
	}

	return listener, nil
}

// SetReactor 设置反应器
func (l *Listener) SetReactor(r *reactor.Reactor) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reactor = r
}

// SetAcceptHandler 设置接受连接处理器
func (l *Listener) SetAcceptHandler(handler AcceptHandler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acceptHandler = handler
}

// Listen 开始监听
func (l *Listener) Listen() error {
	if !atomic.CompareAndSwapInt32(&l.running, 0, 1) {
		return fmt.Errorf("listener already running")
	}

	// 创建监听socket
	err := l.createListener()
	if err != nil {
		atomic.StoreInt32(&l.running, 0)
		return fmt.Errorf("create listener failed: %v", err)
	}

	// 启动接受循环
	l.wg.Add(1)
	go l.acceptLoop()

	return nil
}

// Stop 停止监听
func (l *Listener) Stop() error {
	if !atomic.CompareAndSwapInt32(&l.stopping, 0, 1) {
		return nil // 已经在停止
	}

	// 停止接受新连接
	close(l.stopChan)

	// 关闭监听socket
	if l.socket != nil {
		l.socket.Close()
	}
	if l.listener != nil {
		l.listener.Close()
	}

	// 等待协程结束
	l.wg.Wait()

	atomic.StoreInt32(&l.running, 0)
	return nil
}

// ==================== 监听创建 ====================

// createListener 创建监听器
func (l *Listener) createListener() error {
	// 创建socket
	sock, err := socket.NewTCPSocket()
	if err != nil {
		return fmt.Errorf("create socket failed: %v", err)
	}

	// 配置socket选项
	if l.config.ReuseAddr {
		if err := sock.SetReuseAddr(true); err != nil {
			sock.Close()
			return fmt.Errorf("set reuse addr failed: %v", err)
		}
	}

	if l.config.ReusePort {
		if err := sock.SetReusePort(true); err != nil {
			sock.Close()
			return fmt.Errorf("set reuse port failed: %v", err)
		}
	}

	// 绑定地址
	if err := sock.Bind(l.config.Address); err != nil {
		sock.Close()
		return fmt.Errorf("bind address %s failed: %v", l.config.Address, err)
	}

	// 开始监听
	backlog := l.config.Backlog
	if backlog <= 0 {
		backlog = 1024
	}

	if err := sock.Listen(backlog); err != nil {
		sock.Close()
		return fmt.Errorf("listen failed: %v", err)
	}

	l.socket = sock
	return nil
}

// ==================== 接受循环 ====================

// acceptLoop 接受连接循环
func (l *Listener) acceptLoop() {
	defer l.wg.Done()

	for {
		select {
		case <-l.stopChan:
			return
		default:
		}

		// 接受新连接
		clientSocket, remoteAddr, err := l.socket.Accept()
		if err != nil {
			if atomic.LoadInt32(&l.stopping) == 0 {
				// 监听socket是非阻塞的：EAGAIN表示当前没有待处理连接，
				// 属于正常情况，短暂退避避免100% CPU空转
				if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
					time.Sleep(time.Millisecond)
					continue
				}
				atomic.AddInt64(&l.errorCount, 1)
				l.handleError(fmt.Errorf("accept failed: %v", err))
			}
			continue
		}

		// 异步处理新连接
		go l.handleNewConnection(clientSocket, remoteAddr)
		atomic.AddInt64(&l.acceptCount, 1)
	}
}

// handleNewConnection 处理新连接
func (l *Listener) handleNewConnection(clientSocket *socket.Socket, remoteAddr string) {
	defer func() {
		if err := recover(); err != nil {
			atomic.AddInt64(&l.errorCount, 1)
			clientSocket.Close()
		}
	}()

	// 检查连接数限制
	if l.config.MaxConnections > 0 && atomic.LoadInt64(&l.connectionCount) >= int64(l.config.MaxConnections) {
		clientSocket.Close()
		return
	}

	// 配置客户端socket
	l.configureClientSocket(clientSocket)

	// 创建连接对象
	conn := l.createConnection(clientSocket, remoteAddr)
	if conn == nil {
		clientSocket.Close()
		return
	}

	atomic.AddInt64(&l.connectionCount, 1)

	// 设置连接关闭回调
	conn.OnClose(func(c *connection.Connection) {
		atomic.AddInt64(&l.connectionCount, -1)
	})

	// 添加到反应器
	if l.reactor != nil {
		if err := l.reactor.AddConnection(conn); err != nil {
			conn.Close()
			l.handleError(fmt.Errorf("add connection to reactor failed: %v", err))
			return
		}
	}

	// 调用接受处理器
	if l.acceptHandler != nil {
		if err := l.acceptHandler.OnAccept(conn); err != nil {
			conn.Close()
			l.handleError(fmt.Errorf("accept handler failed: %v", err))
			return
		}
	}
}

// configureClientSocket 配置客户端socket
func (l *Listener) configureClientSocket(clientSocket *socket.Socket) {
	if l.config.KeepAlive {
		clientSocket.SetKeepAlive(true)
	}

	if l.config.NoDelay {
		clientSocket.SetTCPNoDelay(true)
	}

	// 设置非阻塞
	clientSocket.SetNonblock()
}

// createConnection 创建连接
func (l *Listener) createConnection(clientSocket *socket.Socket, remoteAddr string) *connection.Connection {
	localAddr := l.config.Address

	// 创建连接
	return connection.NewConnection(clientSocket, localAddr, remoteAddr)
}

// handleError 处理错误
func (l *Listener) handleError(err error) {
	if l.acceptHandler != nil {
		l.acceptHandler.OnError(err)
	}
}

// ==================== 信息获取 ====================

// GetConfig 获取配置
func (l *Listener) GetConfig() *Config {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// 返回配置的副本
	config := *l.config
	return &config
}

// GetLocalAddr 获取监听地址
func (l *Listener) GetLocalAddr() string {
	if l.socket != nil {
		// 这里简单返回配置中的地址，实际可以从socket获取
		return l.config.Address
	}
	return ""
}

// IsRunning 检查是否运行中
func (l *Listener) IsRunning() bool {
	return atomic.LoadInt32(&l.running) == 1
}

// IsStopping 检查是否正在停止
func (l *Listener) IsStopping() bool {
	return atomic.LoadInt32(&l.stopping) == 1
}

// GetStats 获取监听器统计信息
func (l *Listener) GetStats() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return map[string]interface{}{
		"running":          l.IsRunning(),
		"stopping":         l.IsStopping(),
		"accept_count":     atomic.LoadInt64(&l.acceptCount),
		"error_count":      atomic.LoadInt64(&l.errorCount),
		"connection_count": atomic.LoadInt64(&l.connectionCount),
		"local_addr":       l.GetLocalAddr(),
		"network":          l.config.Network,
		"max_connections":  l.config.MaxConnections,
		"backlog":          l.config.Backlog,
	}
}

// ==================== 便利创建方法 ====================

// NewTCPListener 创建TCP监听器
func NewTCPListener(address string) (*Listener, error) {
	config := DefaultConfig()
	config.Network = "tcp"
	config.Address = address

	return NewListener(config)
}

// NewTCPListenerWithConfig 使用配置创建TCP监听器
func NewTCPListenerWithConfig(address string, maxConn int, backlog int) (*Listener, error) {
	config := DefaultConfig()
	config.Network = "tcp"
	config.Address = address
	config.MaxConnections = maxConn
	config.Backlog = backlog

	return NewListener(config)
}

// ==================== 高级功能 ====================

// ListenWithReactor 监听并绑定到反应器
func (l *Listener) ListenWithReactor(r *reactor.Reactor) error {
	l.SetReactor(r)
	return l.Listen()
}

// GetConnectionCount 获取当前连接数
func (l *Listener) GetConnectionCount() int64 {
	return atomic.LoadInt64(&l.connectionCount)
}

// GetAcceptCount 获取接受连接总数
func (l *Listener) GetAcceptCount() int64 {
	return atomic.LoadInt64(&l.acceptCount)
}

// GetErrorCount 获取错误总数
func (l *Listener) GetErrorCount() int64 {
	return atomic.LoadInt64(&l.errorCount)
}

// UpdateConfig 更新配置（部分配置项）
func (l *Listener) UpdateConfig(maxConn int, timeout time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if maxConn > 0 {
		l.config.MaxConnections = maxConn
	}
	if timeout > 0 {
		l.config.AcceptTimeout = timeout
	}
}
