package connection

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionManager 连接管理器
type ConnectionManager struct {
	// 连接映射
	connections map[string]*ClientConnection
	userConns   map[uint32]map[string]*ClientConnection // userID -> connID -> connection
	mutex       sync.RWMutex

	// 配置
	maxConnections    int
	connectionTimeout time.Duration

	// 连接序列号生成器（心跳等系统消息用）
	connSeqGenerator *message.ConnSeqGenerator

	// 统计信息
	totalConnections    int64
	activeConnections   int64
	rejectedConnections int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewConnectionManager 创建连接管理器
func NewConnectionManager(maxConnections int) *ConnectionManager {
	return &ConnectionManager{
		connections:       make(map[string]*ClientConnection),
		userConns:         make(map[uint32]map[string]*ClientConnection),
		maxConnections:    maxConnections,
		connectionTimeout: 5 * time.Minute, // 5分钟超时
		connSeqGenerator:  message.NewConnSeqGenerator(),
	}
}

// Start 启动连接管理器
func (cm *ConnectionManager) Start(ctx context.Context) {
	cm.ctx, cm.cancel = context.WithCancel(ctx)

	// 启动清理协程
	cm.wg.Add(1)
	go cm.cleanupLoop()

	log.Printf("ConnectionManager started")
}

// Stop 停止连接管理器
func (cm *ConnectionManager) Stop() {
	if cm.cancel != nil {
		cm.cancel()
	}
	cm.wg.Wait()

	// 关闭所有连接
	cm.CloseAllConnections()

	log.Printf("ConnectionManager stopped")
}

// AddConnection 添加连接
func (cm *ConnectionManager) AddConnection(conn *ClientConnection) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// 检查连接数限制
	if len(cm.connections) >= cm.maxConnections {
		atomic.AddInt64(&cm.rejectedConnections, 1)
		return fmt.Errorf("max connections reached: %d", cm.maxConnections)
	}

	// 检查连接是否已存在
	if _, exists := cm.connections[conn.GetID()]; exists {
		return fmt.Errorf("connection %s already exists", conn.GetID())
	}

	// 添加连接
	cm.connections[conn.GetID()] = conn
	atomic.AddInt64(&cm.totalConnections, 1)
	atomic.AddInt64(&cm.activeConnections, 1)

	log.Printf("Connection added: %s (total: %d)", conn.GetID(), len(cm.connections))
	return nil
}

// RemoveConnection 移除连接
func (cm *ConnectionManager) RemoveConnection(connID string) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	conn, exists := cm.connections[connID]
	if !exists {
		return fmt.Errorf("connection %s not found", connID)
	}

	// 从用户连接映射中移除
	userID := conn.GetUserID()
	if userID != 0 {
		if userConnMap, exists := cm.userConns[userID]; exists {
			delete(userConnMap, connID)
			if len(userConnMap) == 0 {
				delete(cm.userConns, userID)
			}
		}
	}

	// 移除连接
	delete(cm.connections, connID)
	atomic.AddInt64(&cm.activeConnections, -1)

	// 关闭连接
	conn.Close()

	log.Printf("Connection removed: %s (total: %d)", connID, len(cm.connections))
	return nil
}

// GetConnection 获取连接
func (cm *ConnectionManager) GetConnection(connID string) *ClientConnection {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	return cm.connections[connID]
}

// GetUserConnections 获取用户的所有连接
func (cm *ConnectionManager) GetUserConnections(userID uint32) []*ClientConnection {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	userConnMap, exists := cm.userConns[userID]
	if !exists {
		return nil
	}

	connections := make([]*ClientConnection, 0, len(userConnMap))
	for _, conn := range userConnMap {
		if conn.IsConnected() {
			connections = append(connections, conn)
		}
	}

	return connections
}

// UpdateUserConnection 更新用户连接映射
func (cm *ConnectionManager) UpdateUserConnection(conn *ClientConnection) {
	if conn == nil {
		return
	}

	userID := conn.GetUserID()
	if userID == 0 {
		return // 未认证的连接
	}

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// 确保用户连接映射存在
	if _, exists := cm.userConns[userID]; !exists {
		cm.userConns[userID] = make(map[string]*ClientConnection)
	}

	cm.userConns[userID][conn.GetID()] = conn
}

// BroadcastToUser 向指定用户的所有连接广播消息
func (cm *ConnectionManager) BroadcastToUser(userID uint32, data []byte) error {
	connections := cm.GetUserConnections(userID)
	if len(connections) == 0 {
		return fmt.Errorf("no active connections for user %d", userID)
	}

	sent := 0
	for _, conn := range connections {
		if err := conn.Send(data); err != nil {
			log.Printf("Failed to send message to connection %s: %v", conn.GetID(), err)
		} else {
			sent++
		}
	}

	if sent == 0 {
		return fmt.Errorf("failed to send message to any connection for user %d", userID)
	}

	return nil
}

// BroadcastToAll 向所有连接广播消息
func (cm *ConnectionManager) BroadcastToAll(data []byte, filter func(*ClientConnection) bool) {
	cm.mutex.RLock()
	connections := make([]*ClientConnection, 0, len(cm.connections))
	for _, conn := range cm.connections {
		if filter == nil || filter(conn) {
			connections = append(connections, conn)
		}
	}
	cm.mutex.RUnlock()

	for _, conn := range connections {
		if err := conn.Send(data); err != nil {
			log.Printf("Failed to broadcast to connection %s: %v", conn.GetID(), err)
		}
	}
}

// BroadcastHeartbeat 广播心跳消息
func (cm *ConnectionManager) BroadcastHeartbeat() {
	// 创建心跳消息
	codec := message.NewMessageCodec()
	heartbeat := &message.Heartbeat{
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	// 为每个连接创建心跳消息（因为需要不同的用户ID）
	cm.mutex.RLock()
	connections := make([]*ClientConnection, 0, len(cm.connections))
	for _, conn := range cm.connections {
		if conn.IsConnected() && conn.GetUserID() != 0 {
			connections = append(connections, conn)
		}
	}
	cm.mutex.RUnlock()

	for _, conn := range connections {
		envelope, err := codec.CreateEnvelope(
			conn.GetUserID(),
			uint32(conn.GetCurrentRoom()),
			conn.GetLoginID(),
			// ValidateEnvelope要求connection_seq_id非0，用管理器自己的序列号
			cm.connSeqGenerator.Next(),
			heartbeat,
		)
		if err != nil {
			continue
		}

		data, err := codec.Serialize(envelope)
		if err != nil {
			continue
		}

		if err := conn.Send(data); err != nil {
			log.Printf("Failed to send heartbeat to %s: %v", conn.GetID(), err)
		}
	}
}

// CleanupExpiredConnections 清理过期连接
func (cm *ConnectionManager) CleanupExpiredConnections() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	expiredConns := make([]string, 0)
	for connID, conn := range cm.connections {
		if !conn.IsConnected() || conn.IsExpired(cm.connectionTimeout) {
			expiredConns = append(expiredConns, connID)
		}
	}

	for _, connID := range expiredConns {
		if conn := cm.connections[connID]; conn != nil {
			// 从用户连接映射中移除
			userID := conn.GetUserID()
			if userID != 0 {
				if userConnMap, exists := cm.userConns[userID]; exists {
					delete(userConnMap, connID)
					if len(userConnMap) == 0 {
						delete(cm.userConns, userID)
					}
				}
			}

			delete(cm.connections, connID)
			atomic.AddInt64(&cm.activeConnections, -1)
			conn.Close()
		}
	}

	if len(expiredConns) > 0 {
		log.Printf("Cleaned up %d expired connections", len(expiredConns))
	}
}

// CloseAllConnections 关闭所有连接
func (cm *ConnectionManager) CloseAllConnections() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	for _, conn := range cm.connections {
		conn.Close()
	}

	cm.connections = make(map[string]*ClientConnection)
	cm.userConns = make(map[uint32]map[string]*ClientConnection)
	atomic.StoreInt64(&cm.activeConnections, 0)
}

// cleanupLoop 清理循环
func (cm *ConnectionManager) cleanupLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(1 * time.Minute) // 每分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			cm.CleanupExpiredConnections()
		}
	}
}

// GetStats 获取统计信息
func (cm *ConnectionManager) GetStats() map[string]interface{} {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	return map[string]interface{}{
		"total_connections":    atomic.LoadInt64(&cm.totalConnections),
		"active_connections":   atomic.LoadInt64(&cm.activeConnections),
		"rejected_connections": atomic.LoadInt64(&cm.rejectedConnections),
		"connection_count":     len(cm.connections),
		"user_count":           len(cm.userConns),
		"max_connections":      cm.maxConnections,
	}
}

// GetActiveConnectionCount 获取活跃连接数
func (cm *ConnectionManager) GetActiveConnectionCount() int {
	return int(atomic.LoadInt64(&cm.activeConnections))
}

// GetConnectionByUserAndLogin 根据用户ID和登录ID获取连接
func (cm *ConnectionManager) GetConnectionByUserAndLogin(userID uint32, loginID uint8) *ClientConnection {
	connections := cm.GetUserConnections(userID)
	for _, conn := range connections {
		if conn.GetLoginID() == loginID {
			return conn
		}
	}
	return nil
}
