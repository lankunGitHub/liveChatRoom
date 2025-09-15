package api

import (
	"context"
	"fmt"
	"liveChatroom/util/net/base/epoll"
	"liveChatroom/util/net/base/socket"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"sync"
	"sync/atomic"
	"time"
)

// Client 网络客户端 - 纯底层网络功能
type Client struct {
	// 连接
	conn *connection.Connection

	// 事件处理器
	eventHandler ClientEventHandler

	// 配置
	config *ClientConfig

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 状态
	connected int32 // 原子操作

	// 统计
	stats *ClientStats
}

// ClientConfig 客户端配置
type ClientConfig struct {
	// 连接配置
	ConnectTimeout time.Duration // 连接超时
	ReadTimeout    time.Duration // 读取超时
	WriteTimeout   time.Duration // 写入超时

	// 缓冲区配置
	ReadBufferSize  int // 读取缓冲区大小
	WriteBufferSize int // 写入缓冲区大小

	// 重连配置
	EnableReconnect   bool          // 是否启用自动重连
	ReconnectInterval time.Duration // 重连间隔
	MaxReconnectTries int           // 最大重连次数

	// 心跳配置
	EnableHeartbeat   bool          // 是否启用心跳
	HeartbeatInterval time.Duration // 心跳间隔
	HeartbeatTimeout  time.Duration // 心跳超时
}

// ClientStats 客户端统计信息
type ClientStats struct {
	ConnectTime      time.Time
	LastActivity     time.Time
	MessagesSent     int64
	MessagesReceived int64
	BytesSent        int64
	BytesReceived    int64
	ReconnectCount   int64
	LastReconnect    time.Time
}

// ClientEventHandler 客户端事件处理器接口
type ClientEventHandler interface {
	// 连接事件
	OnConnected(client *Client)
	OnDisconnected(client *Client, err error)
	OnReconnected(client *Client, attempt int)

	// 消息事件
	OnMessageReceived(client *Client, msg protocol.Message) error

	// 心跳事件
	OnHeartbeatSent(client *Client)
	OnHeartbeatReceived(client *Client)

	// 错误事件
	OnError(client *Client, err error)
}

// DefaultClientConfig 默认客户端配置
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ConnectTimeout:    10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		ReadBufferSize:    8192,
		WriteBufferSize:   8192,
		EnableReconnect:   true,
		ReconnectInterval: 5 * time.Second,
		MaxReconnectTries: 3,
		EnableHeartbeat:   true,
		HeartbeatInterval: 30 * time.Second,
		HeartbeatTimeout:  10 * time.Second,
	}
}

// NewClient 创建网络客户端
func NewClient(config *ClientConfig, handler ClientEventHandler) (*Client, error) {
	if config == nil {
		config = DefaultClientConfig()
	}

	if handler == nil {
		return nil, fmt.Errorf("event handler is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		eventHandler: handler,
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
		stats:        &ClientStats{},
	}

	return client, nil
}

// Connect 连接到服务器
func (c *Client) Connect(address string) error {
	if atomic.LoadInt32(&c.connected) == 1 {
		return fmt.Errorf("already connected")
	}

	// 创建TCP Socket
	sock, err := socket.NewTCPSocket()
	if err != nil {
		return fmt.Errorf("failed to create socket: %v", err)
	}

	// 连接到服务器
	if err := sock.Connect(address); err != nil {
		sock.Close()
		return fmt.Errorf("failed to connect to %s: %v", address, err)
	}

	// 设置Socket选项
	if err := sock.SetKeepAlive(true); err != nil {
		sock.Close()
		return fmt.Errorf("failed to set keepalive: %v", err)
	}

	// 创建连接对象
	localAddr := "unknown" // 简化处理，实际可以通过getsockname获取
	remoteAddr := address  // 使用连接地址
	conn := connection.NewConnection(sock, localAddr, remoteAddr)

	// 设置连接回调
	c.setupConnectionCallbacks(conn)

	c.conn = conn
	atomic.StoreInt32(&c.connected, 1)
	c.stats.ConnectTime = time.Now()
	c.stats.LastActivity = time.Now()

	// 启动读循环（客户端此前缺失读路径，OnMessageReceived 永远不触发）
	c.wg.Add(1)
	go c.readLoop(conn)

	// 启动连接处理
	c.wg.Add(1)
	go c.handleConnection()

	// 启动心跳
	if c.config.EnableHeartbeat {
		c.wg.Add(1)
		go c.heartbeatLoop()
	}

	// 通知连接成功
	c.eventHandler.OnConnected(c)

	return nil
}

// readLoop 客户端读循环
// 用独立epoll等待socket可读，可读后交给conn.Read()排空——
// 客户端socket是非阻塞的，不能直接轮询read（EAGAIN会导致100% CPU空转）
func (c *Client) readLoop(conn *connection.Connection) {
	defer c.wg.Done()

	ep, err := epoll.New()
	if err != nil {
		c.eventHandler.OnError(c, fmt.Errorf("failed to create epoll for client read: %v", err))
		return
	}
	defer ep.Close()

	if err := ep.Add(conn.FD(), epoll.EPOLLIN|epoll.EPOLLET); err != nil {
		c.eventHandler.OnError(c, fmt.Errorf("failed to register client fd to epoll: %v", err))
		return
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if atomic.LoadInt32(&c.connected) == 0 {
			return // 已断开
		}

		// 等待可读事件（100ms超时，兼顾断开检测）
		n, err := ep.Wait(100)
		if err != nil {
			if atomic.LoadInt32(&c.connected) == 0 {
				return
			}
			continue
		}

		if n == 0 {
			continue // 超时无事件
		}

		// 读排空（内部循环到EAGAIN），消息经OnMessage回调派发
		if err := conn.Read(); err != nil {
			if conn.IsClosed() || atomic.LoadInt32(&c.connected) == 0 {
				return
			}
			// 读错误：标记断开，通知上层，由handleConnection处理重连
			atomic.StoreInt32(&c.connected, 0)
			c.eventHandler.OnError(c, err)
			return
		}
	}
}

// Disconnect 断开连接
func (c *Client) Disconnect() error {
	if !atomic.CompareAndSwapInt32(&c.connected, 1, 0) {
		return fmt.Errorf("not connected")
	}

	// 取消上下文
	c.cancel()

	// 关闭连接（Close会触发onClose回调→OnDisconnected通知）
	if c.conn != nil {
		c.conn.Close()
	}

	// 等待协程结束
	c.wg.Wait()

	return nil
}

// Send 发送数据
func (c *Client) Send(data []byte) error {
	if atomic.LoadInt32(&c.connected) == 0 {
		return fmt.Errorf("not connected")
	}

	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}

	_, err := c.conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to send data: %v", err)
	}

	atomic.AddInt64(&c.stats.MessagesSent, 1)
	atomic.AddInt64(&c.stats.BytesSent, int64(len(data)))
	c.stats.LastActivity = time.Now()

	return nil
}

// SendMessage 发送协议消息
func (c *Client) SendMessage(msg protocol.Message) error {
	if atomic.LoadInt32(&c.connected) == 0 {
		return fmt.Errorf("not connected")
	}

	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}

	if err := c.conn.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	atomic.AddInt64(&c.stats.MessagesSent, 1)
	c.stats.LastActivity = time.Now()

	return nil
}

// setupConnectionCallbacks 设置连接回调
func (c *Client) setupConnectionCallbacks(conn *connection.Connection) {
	// 设置消息回调
	conn.OnMessage(func(conn *connection.Connection, msg protocol.Message) {
		atomic.AddInt64(&c.stats.MessagesReceived, 1)
		c.stats.LastActivity = time.Now()

		if err := c.eventHandler.OnMessageReceived(c, msg); err != nil {
			c.eventHandler.OnError(c, err)
		}
	})

	// 设置关闭回调
	conn.OnClose(func(conn *connection.Connection) {
		atomic.StoreInt32(&c.connected, 0)
		c.eventHandler.OnDisconnected(c, nil)
	})

	// 设置错误回调
	conn.OnError(func(conn *connection.Connection, err error) {
		c.eventHandler.OnError(c, err)
	})
}

// handleConnection 处理连接
func (c *Client) handleConnection() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return

		default:
			// 连接已断开，尝试重连
			if atomic.LoadInt32(&c.connected) == 0 {
				if c.config.EnableReconnect {
					c.attemptReconnect()
				}
				return
			}

			// 短暂休眠避免忙等待
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// attemptReconnect 尝试重连
func (c *Client) attemptReconnect() {
	maxTries := c.config.MaxReconnectTries
	if maxTries <= 0 {
		maxTries = 3 // 默认最多重试3次
	}

	for attempt := 1; attempt <= maxTries; attempt++ {
		select {
		case <-c.ctx.Done():
			return

		default:
			time.Sleep(c.config.ReconnectInterval)

			// 获取原始地址（这里简化处理，实际应该记录原始地址）
			if c.conn != nil && c.conn.RemoteAddr() != "" {
				if err := c.Connect(c.conn.RemoteAddr()); err == nil {
					atomic.AddInt64(&c.stats.ReconnectCount, 1)
					c.stats.LastReconnect = time.Now()
					c.eventHandler.OnReconnected(c, attempt)
					return
				}
			}
		}
	}

	// 重连失败
	c.eventHandler.OnError(c, fmt.Errorf("reconnect failed after %d attempts", maxTries))
}

// heartbeatLoop 心跳循环
func (c *Client) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-ticker.C:
			if atomic.LoadInt32(&c.connected) == 1 {
				// 发送心跳（这里简化为发送空数据，实际应该根据协议发送心跳消息）
				if err := c.Send([]byte("heartbeat")); err != nil {
					c.eventHandler.OnError(c, fmt.Errorf("heartbeat failed: %v", err))
				} else {
					c.eventHandler.OnHeartbeatSent(c)
				}
			}
		}
	}
}

// IsConnected 检查是否已连接
func (c *Client) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// GetStats 获取统计信息
func (c *Client) GetStats() *ClientStats {
	stats := *c.stats
	stats.MessagesSent = atomic.LoadInt64(&c.stats.MessagesSent)
	stats.MessagesReceived = atomic.LoadInt64(&c.stats.MessagesReceived)
	stats.BytesSent = atomic.LoadInt64(&c.stats.BytesSent)
	stats.BytesReceived = atomic.LoadInt64(&c.stats.BytesReceived)
	stats.ReconnectCount = atomic.LoadInt64(&c.stats.ReconnectCount)
	return &stats
}

// GetConnection 获取底层连接
func (c *Client) GetConnection() *connection.Connection {
	return c.conn
}

// GetUptime 获取连接时间
func (c *Client) GetUptime() time.Duration {
	if c.stats.ConnectTime.IsZero() {
		return 0
	}
	return time.Since(c.stats.ConnectTime)
}
