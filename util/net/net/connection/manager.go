package connection

import (
	"fmt"
	"liveChatroom/util/net/base/epoll"
	"liveChatroom/util/net/base/socket"
	"liveChatroom/util/net/protocol"
	"sync"
	"time"
)

// Manager 连接管理器 - 管理所有活跃连接
type Manager struct {
	mu sync.RWMutex

	// 连接存储
	connections map[string]*Connection
	connsByFD   map[int]*Connection

	// 配置
	maxConnections int
	idleTimeout    time.Duration

	// 事件处理
	onConnect    func(*Connection)
	onDisconnect func(*Connection)
	onError      func(*Connection, error)

	// 清理定时器
	cleanupInterval time.Duration
	stopChan        chan struct{}
	wg              sync.WaitGroup
	running         bool
}

// NewManager 创建连接管理器
func NewManager(maxConnections int) *Manager {
	if maxConnections <= 0 {
		maxConnections = 10000
	}

	mgr := &Manager{
		connections:     make(map[string]*Connection),
		connsByFD:       make(map[int]*Connection),
		maxConnections:  maxConnections,
		idleTimeout:     5 * time.Minute,
		cleanupInterval: 30 * time.Second,
		stopChan:        make(chan struct{}),
	}

	return mgr
}

// Start 启动管理器
func (m *Manager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return
	}

	m.running = true

	// 启动清理协程
	m.wg.Add(1)
	go m.cleanupLoop()
}

// Stop 停止管理器
func (m *Manager) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}

	m.running = false
	close(m.stopChan)
	m.mu.Unlock()

	// 等待清理协程结束（cleanup需要m.mu，必须先释放）
	m.wg.Wait()

	// 关闭所有连接（内部自行加锁）
	m.closeAllConnections()
}

// ==================== 连接管理 ====================

// AddConnection 添加连接
func (m *Manager) AddConnection(conn *Connection) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查连接数限制
	if len(m.connections) >= m.maxConnections {
		return fmt.Errorf("max connections reached: %d", m.maxConnections)
	}

	id := conn.ID()
	fd := conn.FD()

	// 检查是否已存在
	if _, exists := m.connections[id]; exists {
		return fmt.Errorf("connection already exists: %s", id)
	}

	if _, exists := m.connsByFD[fd]; exists {
		return fmt.Errorf("connection with fd %d already exists", fd)
	}

	// 添加连接
	m.connections[id] = conn
	m.connsByFD[fd] = conn

	// 设置连接事件处理器
	m.setupConnectionHandlers(conn)

	// 触发连接事件
	if m.onConnect != nil {
		m.onConnect(conn)
	}

	return nil
}

// RemoveConnection 移除连接
func (m *Manager) RemoveConnection(conn *Connection) error {
	m.mu.Lock()
	id := conn.ID()
	fd := conn.FD()

	// 检查连接是否存在
	if _, exists := m.connections[id]; !exists {
		m.mu.Unlock()
		return fmt.Errorf("connection not found: %s", id)
	}

	// 移除连接
	delete(m.connections, id)
	delete(m.connsByFD, fd)
	m.mu.Unlock()

	// 触发断开连接事件（锁外回调，避免回调内再次进入管理器时死锁）
	if m.onDisconnect != nil {
		m.onDisconnect(conn)
	}

	return nil
}

// GetConnection 根据ID获取连接
func (m *Manager) GetConnection(id string) *Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connections[id]
}

// GetConnectionByFD 根据FD获取连接
func (m *Manager) GetConnectionByFD(fd int) *Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connsByFD[fd]
}

// GetAllConnections 获取所有连接
func (m *Manager) GetAllConnections() []*Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()

	connections := make([]*Connection, 0, len(m.connections))
	for _, conn := range m.connections {
		connections = append(connections, conn)
	}

	return connections
}

// GetConnectionCount 获取连接数量
func (m *Manager) GetConnectionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.connections)
}

// ==================== 批量操作 ====================

// Broadcast 广播消息到所有连接
func (m *Manager) Broadcast(data []byte, filter func(*Connection) bool) {
	m.mu.RLock()
	connections := make([]*Connection, 0, len(m.connections))
	for _, conn := range m.connections {
		if filter == nil || filter(conn) {
			connections = append(connections, conn)
		}
	}
	m.mu.RUnlock()

	// 异步发送避免阻塞
	for _, conn := range connections {
		go func(c *Connection) {
			c.Write(data)
		}(conn)
	}
}

// BroadcastByProtocol 根据协议类型广播消息
func (m *Manager) BroadcastByProtocol(protocolType protocol.ProtocolType, msg protocol.Message, filter func(*Connection) bool) {
	m.mu.RLock()
	connections := make([]*Connection, 0, len(m.connections))
	for _, conn := range m.connections {
		if conn.GetProtocolType() == protocolType && (filter == nil || filter(conn)) {
			connections = append(connections, conn)
		}
	}
	m.mu.RUnlock()

	// 异步发送
	for _, conn := range connections {
		go func(c *Connection) {
			c.WriteMessage(msg)
		}(conn)
	}
}

// CloseAllConnections 关闭所有连接
func (m *Manager) CloseAllConnections() {
	m.closeAllConnections()
}

// closeAllConnections 关闭所有连接
// 先持锁取出快照并清空映射，再在锁外逐个关闭：
// conn.Close 会同步回调 onClose → RemoveConnection 再次加锁，
// 若持有 m.mu 时关闭会自死锁
func (m *Manager) closeAllConnections() {
	m.mu.Lock()
	conns := make([]*Connection, 0, len(m.connections))
	for _, conn := range m.connections {
		conns = append(conns, conn)
	}
	m.connections = make(map[string]*Connection)
	m.connsByFD = make(map[int]*Connection)
	m.mu.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}

// ==================== 连接查找 ====================

// FindConnections 根据条件查找连接
func (m *Manager) FindConnections(filter func(*Connection) bool) []*Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Connection
	for _, conn := range m.connections {
		if filter(conn) {
			result = append(result, conn)
		}
	}

	return result
}

// FindConnectionsByProtocol 根据协议类型查找连接
func (m *Manager) FindConnectionsByProtocol(protocolType protocol.ProtocolType) []*Connection {
	return m.FindConnections(func(conn *Connection) bool {
		return conn.GetProtocolType() == protocolType
	})
}

// FindWebSocketConnections 查找所有WebSocket连接
func (m *Manager) FindWebSocketConnections() []*Connection {
	return m.FindConnectionsByProtocol(protocol.ProtocolWebSocket)
}

// FindHTTPConnections 查找所有HTTP连接
func (m *Manager) FindHTTPConnections() []*Connection {
	return m.FindConnectionsByProtocol(protocol.ProtocolHTTP)
}

// ==================== 事件处理 ====================

// OnConnect 设置连接事件处理器
func (m *Manager) OnConnect(handler func(*Connection)) {
	m.onConnect = handler
}

// OnDisconnect 设置断开连接事件处理器
func (m *Manager) OnDisconnect(handler func(*Connection)) {
	m.onDisconnect = handler
}

// OnError 设置错误事件处理器
func (m *Manager) OnError(handler func(*Connection, error)) {
	m.onError = handler
}

// setupConnectionHandlers 设置连接事件处理器
func (m *Manager) setupConnectionHandlers(conn *Connection) {
	// 设置关闭事件处理器 - 自动从管理器中移除
	// 注意：Connection.OnClose 是单值字段，直接覆盖会丢失之前
	// 注册的回调（如引擎层的关闭通知），因此这里做链式包装
	prev := conn.GetOnClose()
	conn.OnClose(func(c *Connection) {
		if prev != nil {
			prev(c)
		}
		m.RemoveConnection(c)
	})

	// 设置错误事件处理器
	if m.onError != nil {
		conn.OnError(m.onError)
	}
}

// ==================== 清理和维护 ====================

// cleanupLoop 清理循环
func (m *Manager) cleanupLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanup()
		case <-m.stopChan:
			return
		}
	}
}

// cleanup 清理无效连接
// 持锁只做检查与摘除，Close 放到锁外执行：
// conn.Close 会同步回调 onClose → RemoveConnection 再次加锁，
// 若持有 m.mu 时关闭会自死锁
func (m *Manager) cleanup() {
	now := time.Now()
	var toClose []*Connection

	m.mu.Lock()
	for id, conn := range m.connections {
		// 检查连接是否已关闭
		if conn.IsClosed() {
			delete(m.connections, id)
			delete(m.connsByFD, conn.FD())
			continue
		}

		// 检查空闲超时
		if m.idleTimeout > 0 {
			conn.mu.RLock()
			lastActive := conn.lastActive
			conn.mu.RUnlock()

			if now.Sub(lastActive) > m.idleTimeout {
				delete(m.connections, id)
				delete(m.connsByFD, conn.FD())
				toClose = append(toClose, conn)
			}
		}
	}
	m.mu.Unlock()

	// 锁外关闭（触发 onClose 回调链，其中 RemoveConnection 会再进锁）
	for _, conn := range toClose {
		conn.Close()
	}
}

// ==================== 配置选项 ====================

// SetMaxConnections 设置最大连接数
func (m *Manager) SetMaxConnections(max int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxConnections = max
}

// SetIdleTimeout 设置空闲超时
func (m *Manager) SetIdleTimeout(timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idleTimeout = timeout
}

// SetCleanupInterval 设置清理间隔
func (m *Manager) SetCleanupInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupInterval = interval
}

// ==================== 统计信息 ====================

// GetStats 获取管理器统计信息
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	totalConnections := len(m.connections)
	protocolStats := make(map[string]int)

	for _, conn := range m.connections {
		protocolType := conn.GetProtocolType().String()
		protocolStats[protocolType]++
	}

	return map[string]interface{}{
		"total_connections": totalConnections,
		"protocol_stats":    protocolStats,
		"max_connections":   m.maxConnections,
		"idle_timeout":      m.idleTimeout,
		"cleanup_interval":  m.cleanupInterval,
		"running":           m.running,
	}
}

// ==================== 高级功能 ====================

// CreateConnection 从socket创建连接并添加到管理器
func (m *Manager) CreateConnection(sock *socket.Socket, localAddr, remoteAddr string) (*Connection, error) {
	conn := NewConnection(sock, localAddr, remoteAddr)

	err := m.AddConnection(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

// AcceptConnection 从监听socket接受连接
func (m *Manager) AcceptConnection(listener *socket.Socket) (*Connection, error) {
	// 接受新连接
	clientSock, remoteAddr, err := listener.Accept()
	if err != nil {
		return nil, err
	}

	localAddr := "unknown"
	return m.CreateConnection(clientSock, localAddr, remoteAddr)
}

// ==================== Epoll集成 ====================

// RegisterToEpoll 将连接注册到epoll
func (m *Manager) RegisterToEpoll(ep *epoll.Epoll, conn *Connection) error {
	return ep.Add(conn.FD(), epoll.EPOLLIN|epoll.EPOLLET)
}

// UnregisterFromEpoll 从epoll注销连接
func (m *Manager) UnregisterFromEpoll(ep *epoll.Epoll, conn *Connection) error {
	return ep.Del(conn.FD())
}

// HandleEpollEvents 处理epoll事件
func (m *Manager) HandleEpollEvents(ep *epoll.Epoll) error {
	n, err := ep.Wait(10) // 10ms超时
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		event := ep.GetEvent(i)
		fd := event.GetFD()

		conn := m.GetConnectionByFD(fd)
		if conn == nil {
			continue
		}

		if event.IsError() || event.IsHup() {
			// 连接错误或挂起
			conn.Close()
		} else if event.IsReadable() {
			// 可读事件
			conn.Read()
		}
	}

	return nil
}
