package router

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"sync"
	"sync/atomic"
	"time"
)

// NodeConnection 节点连接封装 - 连接节点和消息中心的统一抽象
type NodeConnection struct {
	// 基础信息
	id         string
	nodeType   NodeType // "connection" 或 "message_center"
	connection *connection.Connection

	// 节点信息
	host    string
	port    int
	version string
	status  NodeStatus

	// 状态管理
	connected     int32 // 原子操作
	lastHeartbeat time.Time
	lastActivity  time.Time
	createdAt     time.Time

	// 负载信息
	activeConnections int32
	messageProcessed  int64
	cpuUsage          float64
	memoryUsage       float64

	// 发送缓冲区
	sendBuffer chan []byte

	// 互斥锁
	mutex sync.RWMutex

	// 统计信息
	messagesSent     int64
	messagesReceived int64
	bytesTransferred int64
	errorCount       int64
}

// MessageCenterConnection 消息中心连接（NodeConnection的特化）
type MessageCenterConnection struct {
	*NodeConnection

	// 消息中心特有属性
	kafkaTopics   []string
	storageStatus string
	queueDepth    int64
}

// NodeType 节点类型
type NodeType string

const (
	NodeTypeConnection    NodeType = "connection"
	NodeTypeMessageCenter NodeType = "message_center"
)

// NodeStatus 节点状态
type NodeStatus string

const (
	NodeStatusHealthy      NodeStatus = "healthy"
	NodeStatusUnhealthy    NodeStatus = "unhealthy"
	NodeStatusConnecting   NodeStatus = "connecting"
	NodeStatusDisconnected NodeStatus = "disconnected"
)

// NewNodeConnection 创建节点连接
func NewNodeConnection(id string, nodeType NodeType, conn *connection.Connection) *NodeConnection {
	now := time.Now()

	node := &NodeConnection{
		id:            id,
		nodeType:      nodeType,
		connection:    conn,
		status:        NodeStatusConnecting,
		lastHeartbeat: now,
		lastActivity:  now,
		createdAt:     now,
		sendBuffer:    make(chan []byte, 200), // 缓冲200条消息
	}

	atomic.StoreInt32(&node.connected, 1)

	// 启动发送协程
	go node.sendLoop()

	return node
}

// NewMessageCenterConnection 创建消息中心连接
func NewMessageCenterConnection(id string, conn *connection.Connection) *MessageCenterConnection {
	nodeConn := NewNodeConnection(id, NodeTypeMessageCenter, conn)

	return &MessageCenterConnection{
		NodeConnection: nodeConn,
		kafkaTopics:    make([]string, 0),
		storageStatus:  "normal",
		queueDepth:     0,
	}
}

// GetID 获取节点ID
func (nc *NodeConnection) GetID() string {
	return nc.id
}

// GetNodeType 获取节点类型
func (nc *NodeConnection) GetNodeType() NodeType {
	return nc.nodeType
}

// GetConnection 获取底层连接
func (nc *NodeConnection) GetConnection() *connection.Connection {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()
	return nc.connection
}

// SetConnection 设置底层连接（由客户端事件处理器在连接建立/断开时更新）
func (nc *NodeConnection) SetConnection(conn *connection.Connection) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	nc.connection = conn
}

// GetHost 获取主机地址
func (nc *NodeConnection) GetHost() string {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()
	return nc.host
}

// SetHost 设置主机地址
func (nc *NodeConnection) SetHost(host string) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	nc.host = host
}

// GetPort 获取端口
func (nc *NodeConnection) GetPort() int {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()
	return nc.port
}

// SetPort 设置端口
func (nc *NodeConnection) SetPort(port int) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	nc.port = port
}

// GetVersion 获取版本信息
func (nc *NodeConnection) GetVersion() string {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()
	return nc.version
}

// SetVersion 设置版本信息
func (nc *NodeConnection) SetVersion(version string) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	nc.version = version
}

// GetStatus 获取节点状态
func (nc *NodeConnection) GetStatus() NodeStatus {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()
	return nc.status
}

// SetStatus 设置节点状态
func (nc *NodeConnection) SetStatus(status NodeStatus) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	nc.status = status
}

// IsConnected 检查是否已连接
func (nc *NodeConnection) IsConnected() bool {
	return atomic.LoadInt32(&nc.connected) == 1
}

// IsHealthy 检查是否健康
func (nc *NodeConnection) IsHealthy() bool {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()

	return nc.status == NodeStatusHealthy &&
		time.Since(nc.lastHeartbeat) < 2*time.Minute // 2分钟内有心跳
}

// UpdateHeartbeat 更新心跳时间
func (nc *NodeConnection) UpdateHeartbeat() {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	nc.lastHeartbeat = time.Now()
	if nc.status == NodeStatusConnecting || nc.status == NodeStatusUnhealthy {
		nc.status = NodeStatusHealthy
	}
}

// UpdateActivity 更新活动时间
func (nc *NodeConnection) UpdateActivity() {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	nc.lastActivity = time.Now()
	atomic.AddInt64(&nc.messagesReceived, 1)
}

// GetLastHeartbeat 获取最后心跳时间
func (nc *NodeConnection) GetLastHeartbeat() time.Time {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()
	return nc.lastHeartbeat
}

// GetLastActivity 获取最后活动时间
func (nc *NodeConnection) GetLastActivity() time.Time {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()
	return nc.lastActivity
}

// GetCreatedAt 获取创建时间
func (nc *NodeConnection) GetCreatedAt() time.Time {
	return nc.createdAt
}

// UpdateLoadInfo 更新负载信息
func (nc *NodeConnection) UpdateLoadInfo(activeConns int32, cpuUsage, memoryUsage float64) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	atomic.StoreInt32(&nc.activeConnections, activeConns)
	nc.cpuUsage = cpuUsage
	nc.memoryUsage = memoryUsage
}

// GetLoadInfo 获取负载信息
func (nc *NodeConnection) GetLoadInfo() (int32, float64, float64) {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()

	return atomic.LoadInt32(&nc.activeConnections), nc.cpuUsage, nc.memoryUsage
}

// GetActiveConnections 获取活跃连接数
func (nc *NodeConnection) GetActiveConnections() int32 {
	return atomic.LoadInt32(&nc.activeConnections)
}

// Send 发送数据（异步）
func (nc *NodeConnection) Send(data []byte) error {
	if !nc.IsConnected() {
		return fmt.Errorf("node connection %s is not connected", nc.id)
	}

	select {
	case nc.sendBuffer <- data:
		return nil
	default:
		atomic.AddInt64(&nc.errorCount, 1)
		return fmt.Errorf("send buffer full for node %s", nc.id)
	}
}

// SendSync 同步发送数据
func (nc *NodeConnection) SendSync(data []byte) error {
	if !nc.IsConnected() {
		return fmt.Errorf("node connection %s is not connected", nc.id)
	}

	if nc.connection == nil {
		return fmt.Errorf("connection is nil for node %s", nc.id)
	}

	_, err := nc.connection.Write(data)
	if err != nil {
		atomic.AddInt64(&nc.errorCount, 1)
		return err
	}

	atomic.AddInt64(&nc.messagesSent, 1)
	atomic.AddInt64(&nc.bytesTransferred, int64(len(data)))

	return nil
}

// sendLoop 发送循环
func (nc *NodeConnection) sendLoop() {
	for data := range nc.sendBuffer {
		if !nc.IsConnected() {
			break
		}

		if err := nc.SendSync(data); err != nil {
			// 发送失败，可能需要标记节点为不健康
			nc.SetStatus(NodeStatusUnhealthy)
			break
		}
	}
}

// Close 关闭连接
func (nc *NodeConnection) Close() error {
	if !atomic.CompareAndSwapInt32(&nc.connected, 1, 0) {
		return nil // 已经关闭
	}

	// 更新状态
	nc.SetStatus(NodeStatusDisconnected)

	// 关闭发送缓冲区
	close(nc.sendBuffer)

	// 关闭底层连接
	if nc.connection != nil {
		return nc.connection.Close()
	}

	return nil
}

// GetStats 获取节点统计信息
func (nc *NodeConnection) GetStats() map[string]interface{} {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()

	return map[string]interface{}{
		"id":                 nc.id,
		"node_type":          string(nc.nodeType),
		"host":               nc.host,
		"port":               nc.port,
		"version":            nc.version,
		"status":             string(nc.status),
		"connected":          nc.IsConnected(),
		"healthy":            nc.IsHealthy(),
		"created_at":         nc.createdAt.Unix(),
		"last_heartbeat":     nc.lastHeartbeat.Unix(),
		"last_activity":      nc.lastActivity.Unix(),
		"active_connections": atomic.LoadInt32(&nc.activeConnections),
		"messages_sent":      atomic.LoadInt64(&nc.messagesSent),
		"messages_received":  atomic.LoadInt64(&nc.messagesReceived),
		"bytes_transferred":  atomic.LoadInt64(&nc.bytesTransferred),
		"error_count":        atomic.LoadInt64(&nc.errorCount),
		"cpu_usage":          nc.cpuUsage,
		"memory_usage":       nc.memoryUsage,
		"send_buffer_len":    len(nc.sendBuffer),
	}
}

// GetLoadScore 获取负载分数（用于负载均衡）
func (nc *NodeConnection) GetLoadScore() float64 {
	if !nc.IsHealthy() {
		return 1000.0 // 不健康的节点给予最高分数（最不优先）
	}

	nc.mutex.RLock()
	defer nc.mutex.RUnlock()

	// 综合考虑连接数、CPU和内存使用率
	connectionScore := float64(atomic.LoadInt32(&nc.activeConnections)) / 1000.0
	cpuScore := nc.cpuUsage
	memoryScore := nc.memoryUsage

	// 加权计算总分数
	return connectionScore*0.4 + cpuScore*0.3 + memoryScore*0.3
}

// IsExpired 检查连接是否过期
func (nc *NodeConnection) IsExpired(timeout time.Duration) bool {
	return time.Since(nc.GetLastActivity()) > timeout
}

// GetRemoteAddr 获取远程地址
func (nc *NodeConnection) GetRemoteAddr() string {
	if nc.connection != nil {
		return nc.connection.RemoteAddr()
	}
	return ""
}

// GetLocalAddr 获取本地地址
func (nc *NodeConnection) GetLocalAddr() string {
	if nc.connection != nil {
		return nc.connection.LocalAddr()
	}
	return ""
}

// MessageCenterConnection 的特有方法

// AddKafkaTopic 添加Kafka主题
func (mcc *MessageCenterConnection) AddKafkaTopic(topic string) {
	mcc.mutex.Lock()
	defer mcc.mutex.Unlock()

	for _, t := range mcc.kafkaTopics {
		if t == topic {
			return // 已存在
		}
	}

	mcc.kafkaTopics = append(mcc.kafkaTopics, topic)
}

// GetKafkaTopics 获取Kafka主题列表
func (mcc *MessageCenterConnection) GetKafkaTopics() []string {
	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	topics := make([]string, len(mcc.kafkaTopics))
	copy(topics, mcc.kafkaTopics)
	return topics
}

// SetStorageStatus 设置存储状态
func (mcc *MessageCenterConnection) SetStorageStatus(status string) {
	mcc.mutex.Lock()
	defer mcc.mutex.Unlock()
	mcc.storageStatus = status
}

// GetStorageStatus 获取存储状态
func (mcc *MessageCenterConnection) GetStorageStatus() string {
	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()
	return mcc.storageStatus
}

// UpdateQueueDepth 更新队列深度
func (mcc *MessageCenterConnection) UpdateQueueDepth(depth int64) {
	atomic.StoreInt64(&mcc.queueDepth, depth)
}

// GetQueueDepth 获取队列深度
func (mcc *MessageCenterConnection) GetQueueDepth() int64 {
	return atomic.LoadInt64(&mcc.queueDepth)
}

// GetMessageCenterStats 获取消息中心特有统计信息
func (mcc *MessageCenterConnection) GetMessageCenterStats() map[string]interface{} {
	stats := mcc.GetStats()

	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	stats["kafka_topics"] = mcc.kafkaTopics
	stats["storage_status"] = mcc.storageStatus
	stats["queue_depth"] = atomic.LoadInt64(&mcc.queueDepth)

	return stats
}
