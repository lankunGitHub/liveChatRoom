package connection

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"sync"
	"sync/atomic"
	"time"
)

// ClientConnection 客户端连接封装
type ClientConnection struct {
	// 基础信息
	id          string
	connection  *connection.Connection
	userID      uint32
	loginID     uint8
	currentRoom uint64

	// 状态管理
	connected    int32 // 原子操作
	lastActivity time.Time
	createdAt    time.Time

	// 消息序列管理
	lastMessageSeq  uint64
	lastProcessedID []byte

	// 缓冲区
	sendBuffer chan []byte

	// 互斥锁
	mutex sync.RWMutex

	// 统计信息
	messagesSent     int64
	messagesReceived int64
	bytesTransferred int64
}

// NewClientConnection 创建客户端连接
func NewClientConnection(id string, conn *connection.Connection) *ClientConnection {
	now := time.Now()

	client := &ClientConnection{
		id:           id,
		connection:   conn,
		lastActivity: now,
		createdAt:    now,
		sendBuffer:   make(chan []byte, 100), // 缓冲100条消息
	}

	atomic.StoreInt32(&client.connected, 1)

	// 启动发送协程
	go client.sendLoop()

	return client
}

// GetID 获取连接ID
func (cc *ClientConnection) GetID() string {
	return cc.id
}

// GetConnection 获取底层连接
func (cc *ClientConnection) GetConnection() *connection.Connection {
	return cc.connection
}

// GetUserID 获取用户ID
func (cc *ClientConnection) GetUserID() uint32 {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	return cc.userID
}

// GetLoginID 获取登录ID
func (cc *ClientConnection) GetLoginID() uint8 {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	return cc.loginID
}

// GetCurrentRoom 获取当前房间ID
func (cc *ClientConnection) GetCurrentRoom() uint64 {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	return cc.currentRoom
}

// SetCurrentRoom 设置当前房间ID
func (cc *ClientConnection) SetCurrentRoom(roomID uint64) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.currentRoom = roomID
}

// UpdateLastActivity 更新最后活动时间和用户信息
func (cc *ClientConnection) UpdateLastActivity(userID uint32, roomID uint64, loginID uint8, timestamp int64) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	cc.lastActivity = time.Now()
	cc.userID = userID
	cc.loginID = loginID

	if roomID != 0 {
		cc.currentRoom = roomID
	}

	atomic.AddInt64(&cc.messagesReceived, 1)
}

// GetLastActivity 获取最后活动时间
func (cc *ClientConnection) GetLastActivity() time.Time {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	return cc.lastActivity
}

// GetCreatedAt 获取创建时间
func (cc *ClientConnection) GetCreatedAt() time.Time {
	return cc.createdAt
}

// IsConnected 检查是否已连接
func (cc *ClientConnection) IsConnected() bool {
	return atomic.LoadInt32(&cc.connected) == 1
}

// Send 发送数据（异步）
func (cc *ClientConnection) Send(data []byte) error {
	if !cc.IsConnected() {
		return fmt.Errorf("connection closed")
	}

	select {
	case cc.sendBuffer <- data:
		return nil
	default:
		return fmt.Errorf("send buffer full")
	}
}

// SendSync 同步发送数据
func (cc *ClientConnection) SendSync(data []byte) error {
	if !cc.IsConnected() {
		return fmt.Errorf("connection closed")
	}

	if cc.connection == nil {
		return fmt.Errorf("connection is nil")
	}

	_, err := cc.connection.Write(data)
	if err != nil {
		return err
	}

	atomic.AddInt64(&cc.messagesSent, 1)
	atomic.AddInt64(&cc.bytesTransferred, int64(len(data)))

	return nil
}

// sendLoop 发送循环
func (cc *ClientConnection) sendLoop() {
	for data := range cc.sendBuffer {
		if !cc.IsConnected() {
			break
		}

		if err := cc.SendSync(data); err != nil {
			// 发送失败，可能需要关闭连接
			break
		}
	}
}

// Close 关闭连接
func (cc *ClientConnection) Close() error {
	if !atomic.CompareAndSwapInt32(&cc.connected, 1, 0) {
		return nil // 已经关闭
	}

	// 关闭发送缓冲区
	close(cc.sendBuffer)

	// 关闭底层连接
	if cc.connection != nil {
		return cc.connection.Close()
	}

	return nil
}

// GetStats 获取连接统计信息
func (cc *ClientConnection) GetStats() map[string]interface{} {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()

	return map[string]interface{}{
		"id":                cc.id,
		"user_id":           cc.userID,
		"login_id":          cc.loginID,
		"current_room":      cc.currentRoom,
		"connected":         cc.IsConnected(),
		"created_at":        cc.createdAt.Unix(),
		"last_activity":     cc.lastActivity.Unix(),
		"messages_sent":     atomic.LoadInt64(&cc.messagesSent),
		"messages_received": atomic.LoadInt64(&cc.messagesReceived),
		"bytes_transferred": atomic.LoadInt64(&cc.bytesTransferred),
		"send_buffer_len":   len(cc.sendBuffer),
	}
}

// GetLastMessageSeq 获取最后消息序列号
func (cc *ClientConnection) GetLastMessageSeq() uint64 {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	return cc.lastMessageSeq
}

// SetLastMessageSeq 设置最后消息序列号
func (cc *ClientConnection) SetLastMessageSeq(seq uint64) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.lastMessageSeq = seq
}

// GetLastProcessedID 获取最后处理的消息ID
func (cc *ClientConnection) GetLastProcessedID() []byte {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	if cc.lastProcessedID == nil {
		return nil
	}
	result := make([]byte, len(cc.lastProcessedID))
	copy(result, cc.lastProcessedID)
	return result
}

// SetLastProcessedID 设置最后处理的消息ID
func (cc *ClientConnection) SetLastProcessedID(id []byte) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	if id == nil {
		cc.lastProcessedID = nil
	} else {
		cc.lastProcessedID = make([]byte, len(id))
		copy(cc.lastProcessedID, id)
	}
}

// IsExpired 检查连接是否过期
func (cc *ClientConnection) IsExpired(timeout time.Duration) bool {
	return time.Since(cc.GetLastActivity()) > timeout
}

// GetRemoteAddr 获取远程地址
func (cc *ClientConnection) GetRemoteAddr() string {
	if cc.connection != nil {
		return cc.connection.RemoteAddr()
	}
	return ""
}

// GetLocalAddr 获取本地地址
func (cc *ClientConnection) GetLocalAddr() string {
	if cc.connection != nil {
		return cc.connection.LocalAddr()
	}
	return ""
}
