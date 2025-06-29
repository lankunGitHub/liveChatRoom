package connection

import (
	"liveChatroom/util/net/base/buffer"
	"liveChatroom/util/net/base/io"
	"liveChatroom/util/net/base/pool"
	"liveChatroom/util/net/base/socket"
	"liveChatroom/util/net/base/timer"
	"liveChatroom/util/net/protocol"
	"sync"
	"time"
)

// Pool 连接池 - 复用连接对象以减少内存分配
type Pool struct {
	mu sync.RWMutex

	// 对象池
	connectionPool *pool.SyncPool
	managerPool    *pool.SyncPool

	// 配置
	maxPoolSize int

	// 统计
	created int64
	reused  int64
}

// NewPool 创建连接池
func NewPool() *Pool {
	p := &Pool{
		maxPoolSize: 1000,
	}

	// 创建连接对象池
	p.connectionPool = pool.NewSyncPool(func() interface{} {
		// 创建空的连接对象，稍后通过Initialize方法初始化
		return &Connection{}
	})

	// 创建管理器对象池
	p.managerPool = pool.NewSyncPool(func() interface{} {
		return &Manager{
			connections: make(map[string]*Connection),
			connsByFD:   make(map[int]*Connection),
			stopChan:    make(chan struct{}),
		}
	})

	return p
}

// ==================== 连接池操作 ====================

// GetConnection 从池中获取连接对象
func (p *Pool) GetConnection(sock *socket.Socket, localAddr, remoteAddr string) *Connection {
	// 从池中获取连接对象
	conn := p.connectionPool.Get().(*Connection)

	// 初始化连接
	p.initConnection(conn, sock, localAddr, remoteAddr)

	p.reused++
	return conn
}

// PutConnection 将连接对象归还到池中
func (p *Pool) PutConnection(conn *Connection) {
	if conn == nil {
		return
	}

	// 清理连接状态
	p.cleanConnection(conn)

	// 归还到池中
	p.connectionPool.Put(conn)
}

// GetManager 从池中获取管理器对象
func (p *Pool) GetManager(maxConnections int) *Manager {
	mgr := p.managerPool.Get().(*Manager)

	// 初始化管理器
	p.initManager(mgr, maxConnections)

	return mgr
}

// PutManager 将管理器对象归还到池中
func (p *Pool) PutManager(mgr *Manager) {
	if mgr == nil {
		return
	}

	// 清理管理器状态
	p.cleanManager(mgr)

	// 归还到池中
	p.managerPool.Put(mgr)
}

// ==================== 内部方法 ====================

// initConnection 初始化连接对象
func (p *Pool) initConnection(conn *Connection, sock *socket.Socket, localAddr, remoteAddr string) {
	// 重置所有字段
	*conn = Connection{
		id:         generateConnectionID(),
		socket:     sock,
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
		state:      StateConnected,
		createdAt:  time.Now(),
		lastActive: time.Now(),

		// 创建新的组件（无法复用的部分）
		reader:      io.NewReader(sock.FD()),
		writer:      io.NewWriter(sock.FD()),
		readStream:  io.NewReadStream(sock.FD(), 8192),
		writeStream: io.NewWriteStream(sock.FD(), 8192),

		// 从全局池获取缓冲区
		readBuffer:  buffer.Get(8192),
		writeBuffer: buffer.Get(8192),

		// 默认配置
		readTimeout:  30 * time.Second,
		writeTimeout: 30 * time.Second,
		idleTimeout:  5 * time.Minute,
		keepAlive:    true,
		tcpNoDelay:   true,
	}

	// 配置socket选项
	conn.configureSocket()

	// 启动超时检测
	conn.startIdleTimer()
}

// cleanConnection 清理连接对象
func (p *Pool) cleanConnection(conn *Connection) {
	// 停止定时器
	if conn.timeoutTimer != 0 {
		timer.Cancel(conn.timeoutTimer)
		conn.timeoutTimer = 0
	}

	// 关闭流（它们会自动归还缓冲区）
	if conn.readStream != nil {
		conn.readStream.Close()
		conn.readStream = nil
	}
	if conn.writeStream != nil {
		conn.writeStream.Close()
		conn.writeStream = nil
	}

	// 归还缓冲区
	if conn.readBuffer != nil {
		buffer.Put(conn.readBuffer)
		conn.readBuffer = nil
	}
	if conn.writeBuffer != nil {
		buffer.Put(conn.writeBuffer)
		conn.writeBuffer = nil
	}

	// 关闭协议处理器
	if conn.protocolHandler != nil {
		conn.protocolHandler.Close()
		conn.protocolHandler = nil
	}

	// 清理其他字段
	conn.socket = nil
	conn.reader = nil
	conn.writer = nil
	conn.onRead = nil
	conn.onWrite = nil
	conn.onClose = nil
	conn.onError = nil
	conn.onMessage = nil
	conn.onUpgrade = nil

	// 重置状态
	conn.closed = 0
	conn.state = StateConnecting
	conn.protocolType = protocol.ProtocolUnknown
	conn.upgrading = false
}

// initManager 初始化管理器对象
func (p *Pool) initManager(mgr *Manager, maxConnections int) {
	// 清理现有状态
	p.cleanManager(mgr)

	// 重新初始化
	mgr.connections = make(map[string]*Connection)
	mgr.connsByFD = make(map[int]*Connection)
	mgr.maxConnections = maxConnections
	mgr.idleTimeout = 5 * time.Minute
	mgr.cleanupInterval = 30 * time.Second
	mgr.stopChan = make(chan struct{})
	mgr.running = false
}

// cleanManager 清理管理器对象
func (p *Pool) cleanManager(mgr *Manager) {
	// 停止管理器
	if mgr.running {
		mgr.Stop()
	}

	// 清理连接映射
	if mgr.connections != nil {
		for _, conn := range mgr.connections {
			if conn != nil {
				conn.Close()
			}
		}
		mgr.connections = nil
	}

	if mgr.connsByFD != nil {
		mgr.connsByFD = nil
	}

	// 清理事件处理器
	mgr.onConnect = nil
	mgr.onDisconnect = nil
	mgr.onError = nil
}

// ==================== 统计信息 ====================

// GetStats 获取池统计信息
func (p *Pool) GetStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return map[string]interface{}{
		"max_pool_size": p.maxPoolSize,
		"created":       p.created,
		"reused":        p.reused,
	}
}

// Clear 清空池
func (p *Pool) Clear() {
	p.connectionPool.Clear()
	p.managerPool.Clear()

	p.mu.Lock()
	p.created = 0
	p.reused = 0
	p.mu.Unlock()
}

// ==================== 全局连接池 ====================

var (
	globalPool     *Pool
	globalPoolOnce sync.Once
)

// GetGlobalPool 获取全局连接池
func GetGlobalPool() *Pool {
	globalPoolOnce.Do(func() {
		globalPool = NewPool()
	})
	return globalPool
}

// ==================== 便利函数 ====================

// NewConnectionFromPool 从全局池创建连接
func NewConnectionFromPool(sock *socket.Socket, localAddr, remoteAddr string) *Connection {
	return GetGlobalPool().GetConnection(sock, localAddr, remoteAddr)
}

// ReturnConnectionToPool 将连接归还到全局池
func ReturnConnectionToPool(conn *Connection) {
	GetGlobalPool().PutConnection(conn)
}

// NewManagerFromPool 从全局池创建管理器
func NewManagerFromPool(maxConnections int) *Manager {
	return GetGlobalPool().GetManager(maxConnections)
}

// ReturnManagerToPool 将管理器归还到全局池
func ReturnManagerToPool(mgr *Manager) {
	GetGlobalPool().PutManager(mgr)
}
