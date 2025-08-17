package msgcenter

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"sync"
	"sync/atomic"
	"time"
)

// RouterConnection 路由连接封装 - 连接到路由节点的连接
type RouterConnection struct {
	// 基础信息
	id         string
	connection *connection.Connection

	// 节点信息
	host    string
	port    int
	version string

	// 状态管理
	connected     int32 // 原子操作
	lastHeartbeat time.Time
	lastActivity  time.Time
	createdAt     time.Time

	// 发送缓冲区
	sendBuffer chan []byte

	// 互斥锁
	mutex sync.RWMutex

	// 统计信息
	messagesSent     int64
	messagesReceived int64
	queriesProcessed int64
	bytesTransferred int64
	errorCount       int64
}

// NewRouterConnection 创建路由连接
func NewRouterConnection(id string, conn *connection.Connection) *RouterConnection {
	now := time.Now()

	routerConn := &RouterConnection{
		id:            id,
		connection:    conn,
		lastHeartbeat: now,
		lastActivity:  now,
		createdAt:     now,
		sendBuffer:    make(chan []byte, 100), // 缓冲100条消息
	}

	atomic.StoreInt32(&routerConn.connected, 1)

	// 启动发送协程
	go routerConn.sendLoop()

	return routerConn
}

// GetID 获取连接ID
func (rc *RouterConnection) GetID() string {
	return rc.id
}

// GetConnection 获取底层连接
func (rc *RouterConnection) GetConnection() *connection.Connection {
	return rc.connection
}

// GetHost 获取主机地址
func (rc *RouterConnection) GetHost() string {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()
	return rc.host
}

// SetHost 设置主机地址
func (rc *RouterConnection) SetHost(host string) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.host = host
}

// GetPort 获取端口
func (rc *RouterConnection) GetPort() int {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()
	return rc.port
}

// SetPort 设置端口
func (rc *RouterConnection) SetPort(port int) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.port = port
}

// GetVersion 获取版本信息
func (rc *RouterConnection) GetVersion() string {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()
	return rc.version
}

// SetVersion 设置版本信息
func (rc *RouterConnection) SetVersion(version string) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.version = version
}

// IsConnected 检查是否已连接
func (rc *RouterConnection) IsConnected() bool {
	return atomic.LoadInt32(&rc.connected) == 1
}

// UpdateHeartbeat 更新心跳时间
func (rc *RouterConnection) UpdateHeartbeat() {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.lastHeartbeat = time.Now()
}

// UpdateActivity 更新活动时间
func (rc *RouterConnection) UpdateActivity() {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	rc.lastActivity = time.Now()
	atomic.AddInt64(&rc.messagesReceived, 1)
}

// GetLastHeartbeat 获取最后心跳时间
func (rc *RouterConnection) GetLastHeartbeat() time.Time {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()
	return rc.lastHeartbeat
}

// GetLastActivity 获取最后活动时间
func (rc *RouterConnection) GetLastActivity() time.Time {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()
	return rc.lastActivity
}

// GetCreatedAt 获取创建时间
func (rc *RouterConnection) GetCreatedAt() time.Time {
	return rc.createdAt
}

// IsHealthy 检查是否健康
func (rc *RouterConnection) IsHealthy() bool {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()

	return rc.IsConnected() &&
		time.Since(rc.lastHeartbeat) < 2*time.Minute // 2分钟内有心跳
}

// Send 发送数据（异步）
func (rc *RouterConnection) Send(data []byte) error {
	if !rc.IsConnected() {
		return fmt.Errorf("router connection %s is not connected", rc.id)
	}

	select {
	case rc.sendBuffer <- data:
		return nil
	default:
		atomic.AddInt64(&rc.errorCount, 1)
		return fmt.Errorf("send buffer full for router connection %s", rc.id)
	}
}

// SendSync 同步发送数据
func (rc *RouterConnection) SendSync(data []byte) error {
	if !rc.IsConnected() {
		return fmt.Errorf("router connection %s is not connected", rc.id)
	}

	if rc.connection == nil {
		return fmt.Errorf("connection is nil for router %s", rc.id)
	}

	_, err := rc.connection.Write(data)
	if err != nil {
		atomic.AddInt64(&rc.errorCount, 1)
		return err
	}

	atomic.AddInt64(&rc.messagesSent, 1)
	atomic.AddInt64(&rc.bytesTransferred, int64(len(data)))

	return nil
}

// sendLoop 发送循环
func (rc *RouterConnection) sendLoop() {
	for data := range rc.sendBuffer {
		if !rc.IsConnected() {
			break
		}

		if err := rc.SendSync(data); err != nil {
			// 发送失败，记录错误
			atomic.AddInt64(&rc.errorCount, 1)
			break
		}
	}
}

// Close 关闭连接
func (rc *RouterConnection) Close() error {
	if !atomic.CompareAndSwapInt32(&rc.connected, 1, 0) {
		return nil // 已经关闭
	}

	// 关闭发送缓冲区
	close(rc.sendBuffer)

	// 关闭底层连接
	if rc.connection != nil {
		return rc.connection.Close()
	}

	return nil
}

// IncrementQueriesProcessed 增加查询处理计数
func (rc *RouterConnection) IncrementQueriesProcessed() {
	atomic.AddInt64(&rc.queriesProcessed, 1)
}

// GetStats 获取连接统计信息
func (rc *RouterConnection) GetStats() map[string]interface{} {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()

	return map[string]interface{}{
		"id":                rc.id,
		"host":              rc.host,
		"port":              rc.port,
		"version":           rc.version,
		"connected":         rc.IsConnected(),
		"healthy":           rc.IsHealthy(),
		"created_at":        rc.createdAt.Unix(),
		"last_heartbeat":    rc.lastHeartbeat.Unix(),
		"last_activity":     rc.lastActivity.Unix(),
		"messages_sent":     atomic.LoadInt64(&rc.messagesSent),
		"messages_received": atomic.LoadInt64(&rc.messagesReceived),
		"queries_processed": atomic.LoadInt64(&rc.queriesProcessed),
		"bytes_transferred": atomic.LoadInt64(&rc.bytesTransferred),
		"error_count":       atomic.LoadInt64(&rc.errorCount),
		"send_buffer_len":   len(rc.sendBuffer),
	}
}

// IsExpired 检查连接是否过期
func (rc *RouterConnection) IsExpired(timeout time.Duration) bool {
	return time.Since(rc.GetLastActivity()) > timeout
}

// GetRemoteAddr 获取远程地址
func (rc *RouterConnection) GetRemoteAddr() string {
	if rc.connection != nil {
		return rc.connection.RemoteAddr()
	}
	return ""
}

// GetLocalAddr 获取本地地址
func (rc *RouterConnection) GetLocalAddr() string {
	if rc.connection != nil {
		return rc.connection.LocalAddr()
	}
	return ""
}
