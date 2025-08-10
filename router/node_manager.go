package router

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"liveChatroom/util/net/api"
	"liveChatroom/util/net/net/connection"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// NodeManager 节点管理器 - 管理连接节点和消息中心节点
type NodeManager struct {
	router *RouterServer
	config *RouterConfig

	// 连接节点管理
	connectionNodes map[string]*NodeConnection
	connMutex       sync.RWMutex

	// 消息中心管理
	messageCenters map[string]*MessageCenterConnection
	centerMutex    sync.RWMutex

	// 客户端管理（用于连接到消息中心）
	centerClients map[string]*api.Client
	clientMutex   sync.RWMutex

	// 消息编解码
	codec            *message.MessageCodec
	connSeqGenerator *message.ConnSeqGenerator

	// 统计信息
	totalConnectionNodes  int64
	totalMessageCenters   int64
	activeConnectionNodes int64
	activeMessageCenters  int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewNodeManager 创建节点管理器
func NewNodeManager(router *RouterServer, config *RouterConfig) *NodeManager {
	return &NodeManager{
		router:           router,
		config:           config,
		connectionNodes:  make(map[string]*NodeConnection),
		messageCenters:   make(map[string]*MessageCenterConnection),
		centerClients:    make(map[string]*api.Client),
		codec:            message.NewMessageCodec(),
		connSeqGenerator: message.NewConnSeqGenerator(),
	}
}

// Start 启动节点管理器
func (nm *NodeManager) Start(ctx context.Context) {
	nm.ctx, nm.cancel = context.WithCancel(ctx)

	// 连接到配置的消息中心
	nm.connectToMessageCenters()

	// 启动健康检查协程
	nm.wg.Add(1)
	go nm.healthCheckLoop()

	// 启动重连协程
	nm.wg.Add(1)
	go nm.reconnectLoop()

	log.Printf("NodeManager started")
}

// Stop 停止节点管理器
func (nm *NodeManager) Stop() {
	if nm.cancel != nil {
		nm.cancel()
	}
	nm.wg.Wait()

	// 关闭所有连接节点
	nm.CloseAllConnectionNodes()

	// 关闭所有消息中心连接
	nm.CloseAllMessageCenters()

	log.Printf("NodeManager stopped")
}

// 连接节点管理

// RegisterConnectionNode 注册连接节点
func (nm *NodeManager) RegisterConnectionNode(nodeID string, conn *connection.Connection, host string, port int) *NodeConnection {
	nm.connMutex.Lock()
	defer nm.connMutex.Unlock()

	// 检查是否已存在
	if existingNode, exists := nm.connectionNodes[nodeID]; exists {
		log.Printf("Connection node %s already registered, closing old connection", nodeID)
		existingNode.Close()
	}

	// 创建新的节点连接
	nodeConn := NewNodeConnection(nodeID, NodeTypeConnection, conn)
	nodeConn.SetHost(host)
	nodeConn.SetPort(port)
	nodeConn.SetStatus(NodeStatusHealthy)

	nm.connectionNodes[nodeID] = nodeConn
	atomic.AddInt64(&nm.totalConnectionNodes, 1)
	atomic.AddInt64(&nm.activeConnectionNodes, 1)

	log.Printf("Connection node registered: %s (%s:%d)", nodeID, host, port)
	return nodeConn
}

// UnregisterConnectionNode 注销连接节点
func (nm *NodeManager) UnregisterConnectionNode(nodeID string) error {
	nm.connMutex.Lock()
	defer nm.connMutex.Unlock()

	node, exists := nm.connectionNodes[nodeID]
	if !exists {
		return fmt.Errorf("connection node %s not found", nodeID)
	}

	// 关闭连接
	node.Close()

	// 从映射中删除
	delete(nm.connectionNodes, nodeID)
	atomic.AddInt64(&nm.activeConnectionNodes, -1)

	log.Printf("Connection node unregistered: %s", nodeID)
	return nil
}

// GetConnectionNode 获取连接节点
func (nm *NodeManager) GetConnectionNode(nodeID string) *NodeConnection {
	nm.connMutex.RLock()
	defer nm.connMutex.RUnlock()
	return nm.connectionNodes[nodeID]
}

// GetAllConnectionNodes 获取所有连接节点
func (nm *NodeManager) GetAllConnectionNodes() []*NodeConnection {
	nm.connMutex.RLock()
	defer nm.connMutex.RUnlock()

	nodes := make([]*NodeConnection, 0, len(nm.connectionNodes))
	for _, node := range nm.connectionNodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetHealthyConnectionNodes 获取健康的连接节点
func (nm *NodeManager) GetHealthyConnectionNodes() []*NodeConnection {
	nm.connMutex.RLock()
	defer nm.connMutex.RUnlock()

	nodes := make([]*NodeConnection, 0)
	for _, node := range nm.connectionNodes {
		if node.IsHealthy() {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetConnectionNodeCount 获取连接节点数量
func (nm *NodeManager) GetConnectionNodeCount() int {
	nm.connMutex.RLock()
	defer nm.connMutex.RUnlock()
	return len(nm.connectionNodes)
}

// CloseAllConnectionNodes 关闭所有连接节点
func (nm *NodeManager) CloseAllConnectionNodes() {
	nm.connMutex.Lock()
	defer nm.connMutex.Unlock()

	for _, node := range nm.connectionNodes {
		node.Close()
	}

	nm.connectionNodes = make(map[string]*NodeConnection)
	atomic.StoreInt64(&nm.activeConnectionNodes, 0)
}

// 消息中心管理

// connectToMessageCenters 连接到配置的消息中心
func (nm *NodeManager) connectToMessageCenters() {
	for _, addr := range nm.config.MessageCenterAddrs {
		nm.connectToMessageCenter(addr)
	}
}

// connectToMessageCenter 连接到指定消息中心
func (nm *NodeManager) connectToMessageCenter(addr string) error {
	nm.centerMutex.Lock()
	defer nm.centerMutex.Unlock()

	// 检查是否已连接
	if _, exists := nm.messageCenters[addr]; exists {
		return nil
	}

	// 创建客户端配置
	clientConfig := &api.ClientConfig{
		ConnectTimeout:    nm.config.MessageCenterTimeout,
		ReadTimeout:       nm.config.MessageCenterTimeout,
		WriteTimeout:      nm.config.MessageCenterTimeout,
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
		EnableReconnect:   true,
		ReconnectInterval: 5 * time.Second,
		MaxReconnectTries: 3,
		EnableHeartbeat:   true,
		HeartbeatInterval: 30 * time.Second,
		HeartbeatTimeout:  10 * time.Second,
	}

	// 创建客户端
	centerClient, err := api.NewClient(clientConfig, nil) // 简化实现，不设置事件处理器
	if err != nil {
		return fmt.Errorf("failed to create message center client: %v", err)
	}

	// 建立连接
	err = centerClient.Connect(addr)
	if err != nil {
		centerClient.Disconnect()
		return fmt.Errorf("failed to connect to message center %s: %v", addr, err)
	}

	// 创建消息中心连接
	centerConn := NewMessageCenterConnection(addr, centerClient.GetConnection())
	centerConn.SetStatus(NodeStatusHealthy)

	// 保存连接和客户端
	nm.messageCenters[addr] = centerConn
	nm.centerClients[addr] = centerClient

	atomic.AddInt64(&nm.totalMessageCenters, 1)
	atomic.AddInt64(&nm.activeMessageCenters, 1)

	log.Printf("Connected to message center: %s", addr)
	return nil
}

// GetMessageCenter 获取消息中心连接
func (nm *NodeManager) GetMessageCenter(addr string) *MessageCenterConnection {
	nm.centerMutex.RLock()
	defer nm.centerMutex.RUnlock()
	return nm.messageCenters[addr]
}

// GetAllMessageCenters 获取所有消息中心连接
func (nm *NodeManager) GetAllMessageCenters() []*MessageCenterConnection {
	nm.centerMutex.RLock()
	defer nm.centerMutex.RUnlock()

	centers := make([]*MessageCenterConnection, 0, len(nm.messageCenters))
	for _, center := range nm.messageCenters {
		centers = append(centers, center)
	}
	return centers
}

// GetHealthyMessageCenters 获取健康的消息中心连接
func (nm *NodeManager) GetHealthyMessageCenters() []*MessageCenterConnection {
	nm.centerMutex.RLock()
	defer nm.centerMutex.RUnlock()

	centers := make([]*MessageCenterConnection, 0)
	for _, center := range nm.messageCenters {
		if center.IsHealthy() {
			centers = append(centers, center)
		}
	}
	return centers
}

// GetMessageCenterCount 获取消息中心数量
func (nm *NodeManager) GetMessageCenterCount() int {
	nm.centerMutex.RLock()
	defer nm.centerMutex.RUnlock()
	return len(nm.messageCenters)
}

// CloseAllMessageCenters 关闭所有消息中心连接
func (nm *NodeManager) CloseAllMessageCenters() {
	nm.centerMutex.Lock()
	defer nm.centerMutex.Unlock()

	// 关闭连接
	for _, center := range nm.messageCenters {
		center.Close()
	}

	// 关闭客户端
	nm.clientMutex.Lock()
	for _, client := range nm.centerClients {
		client.Disconnect()
	}
	nm.centerClients = make(map[string]*api.Client)
	nm.clientMutex.Unlock()

	nm.messageCenters = make(map[string]*MessageCenterConnection)
	atomic.StoreInt64(&nm.activeMessageCenters, 0)
}

// 心跳和健康检查

// SendHeartbeatToConnectionNodes 向所有连接节点发送心跳
func (nm *NodeManager) SendHeartbeatToConnectionNodes() {
	nodes := nm.GetAllConnectionNodes()
	if len(nodes) == 0 {
		return
	}

	// 创建心跳消息
	heartbeat := &message.Heartbeat{
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	for _, node := range nodes {
		if !node.IsConnected() {
			continue
		}

		envelope, err := nm.codec.CreateEnvelope(
			0, // 系统消息
			0, // 无房间
			0, // 无登录ID
			nm.connSeqGenerator.Next(),
			heartbeat,
		)
		if err != nil {
			continue
		}

		data, err := nm.codec.Serialize(envelope)
		if err != nil {
			continue
		}

		if err := node.Send(data); err != nil {
			log.Printf("Failed to send heartbeat to connection node %s: %v", node.GetID(), err)
			node.SetStatus(NodeStatusUnhealthy)
		}
	}
}

// SendHeartbeatToMessageCenters 向所有消息中心发送心跳
func (nm *NodeManager) SendHeartbeatToMessageCenters() {
	centers := nm.GetAllMessageCenters()
	if len(centers) == 0 {
		return
	}

	// 创建心跳消息
	heartbeat := &message.Heartbeat{
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	for _, center := range centers {
		if !center.IsConnected() {
			continue
		}

		envelope, err := nm.codec.CreateEnvelope(
			0, // 系统消息
			0, // 无房间
			0, // 无登录ID
			nm.connSeqGenerator.Next(),
			heartbeat,
		)
		if err != nil {
			continue
		}

		data, err := nm.codec.Serialize(envelope)
		if err != nil {
			continue
		}

		if err := center.Send(data); err != nil {
			log.Printf("Failed to send heartbeat to message center %s: %v", center.GetID(), err)
			center.SetStatus(NodeStatusUnhealthy)
		}
	}
}

// CheckConnectionNodeHealth 检查连接节点健康状态
func (nm *NodeManager) CheckConnectionNodeHealth() {
	nm.connMutex.RLock()
	unhealthyNodes := make([]*NodeConnection, 0)
	for _, node := range nm.connectionNodes {
		if !node.IsHealthy() || node.IsExpired(nm.config.NodeTimeout) {
			unhealthyNodes = append(unhealthyNodes, node)
		}
	}
	nm.connMutex.RUnlock()

	for _, node := range unhealthyNodes {
		log.Printf("Connection node %s is unhealthy, marking for cleanup", node.GetID())
		node.SetStatus(NodeStatusUnhealthy)
	}
}

// CheckMessageCenterHealth 检查消息中心健康状态
func (nm *NodeManager) CheckMessageCenterHealth() {
	nm.centerMutex.RLock()
	unhealthyCenters := make([]*MessageCenterConnection, 0)
	for _, center := range nm.messageCenters {
		if !center.IsHealthy() || center.IsExpired(nm.config.NodeTimeout) {
			unhealthyCenters = append(unhealthyCenters, center)
		}
	}
	nm.centerMutex.RUnlock()

	for _, center := range unhealthyCenters {
		log.Printf("Message center %s is unhealthy, marking for cleanup", center.GetID())
		center.SetStatus(NodeStatusUnhealthy)
	}
}

// CleanupUnhealthyNodes 清理不健康的节点
func (nm *NodeManager) CleanupUnhealthyNodes() {
	// 清理不健康的连接节点
	nm.connMutex.Lock()
	toRemove := make([]string, 0)
	for nodeID, node := range nm.connectionNodes {
		if node.GetStatus() == NodeStatusUnhealthy &&
			time.Since(node.GetLastActivity()) > nm.config.NodeTimeout {
			toRemove = append(toRemove, nodeID)
		}
	}

	for _, nodeID := range toRemove {
		if node := nm.connectionNodes[nodeID]; node != nil {
			node.Close()
			delete(nm.connectionNodes, nodeID)
			atomic.AddInt64(&nm.activeConnectionNodes, -1)
			log.Printf("Cleaned up unhealthy connection node: %s", nodeID)
		}
	}
	nm.connMutex.Unlock()

	// 清理不健康的消息中心连接
	nm.centerMutex.Lock()
	toRemoveCenters := make([]string, 0)
	for addr, center := range nm.messageCenters {
		if center.GetStatus() == NodeStatusUnhealthy &&
			time.Since(center.GetLastActivity()) > nm.config.NodeTimeout {
			toRemoveCenters = append(toRemoveCenters, addr)
		}
	}

	for _, addr := range toRemoveCenters {
		if center := nm.messageCenters[addr]; center != nil {
			center.Close()
			delete(nm.messageCenters, addr)
			atomic.AddInt64(&nm.activeMessageCenters, -1)
		}

		// 关闭对应的客户端
		nm.clientMutex.Lock()
		if client := nm.centerClients[addr]; client != nil {
			client.Disconnect()
			delete(nm.centerClients, addr)
		}
		nm.clientMutex.Unlock()

		log.Printf("Cleaned up unhealthy message center: %s", addr)
	}
	nm.centerMutex.Unlock()
}

// 后台任务

// healthCheckLoop 健康检查循环
func (nm *NodeManager) healthCheckLoop() {
	defer nm.wg.Done()

	ticker := time.NewTicker(nm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-nm.ctx.Done():
			return
		case <-ticker.C:
			nm.CheckConnectionNodeHealth()
			nm.CheckMessageCenterHealth()
		}
	}
}

// reconnectLoop 重连循环
func (nm *NodeManager) reconnectLoop() {
	defer nm.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次重连
	defer ticker.Stop()

	for {
		select {
		case <-nm.ctx.Done():
			return
		case <-ticker.C:
			nm.attemptReconnectMessageCenters()
		}
	}
}

// attemptReconnectMessageCenters 尝试重连消息中心
func (nm *NodeManager) attemptReconnectMessageCenters() {
	for _, addr := range nm.config.MessageCenterAddrs {
		nm.centerMutex.RLock()
		center, exists := nm.messageCenters[addr]
		nm.centerMutex.RUnlock()

		if !exists || !center.IsConnected() || !center.IsHealthy() {
			log.Printf("Attempting to reconnect to message center: %s", addr)
			if err := nm.connectToMessageCenter(addr); err != nil {
				log.Printf("Failed to reconnect to message center %s: %v", addr, err)
			}
		}
	}
}

// GetStats 获取统计信息
func (nm *NodeManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_connection_nodes":   atomic.LoadInt64(&nm.totalConnectionNodes),
		"active_connection_nodes":  atomic.LoadInt64(&nm.activeConnectionNodes),
		"total_message_centers":    atomic.LoadInt64(&nm.totalMessageCenters),
		"active_message_centers":   atomic.LoadInt64(&nm.activeMessageCenters),
		"connection_nodes_count":   nm.GetConnectionNodeCount(),
		"message_centers_count":    nm.GetMessageCenterCount(),
		"healthy_connection_nodes": len(nm.GetHealthyConnectionNodes()),
		"healthy_message_centers":  len(nm.GetHealthyMessageCenters()),
	}
}
