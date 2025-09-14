package client

import (
	"context"
	"fmt"
	"liveChatroom/config"
	"liveChatroom/message"
	"liveChatroom/util/net/api"
	"liveChatroom/util/net/net/connection"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LiveChatClient 实时聊天客户端
type LiveChatClient struct {
	// 基础信息
	userID  uint32
	loginID uint8
	roomID  uint32

	// 网络连接
	connections     []*ConnectionNode // 多个连接节点
	activeConnIndex int32             // 当前活跃连接索引
	connMutex       sync.RWMutex

	// 消息处理
	codec            *message.MessageCodec
	connSeqGenerator *message.ConnSeqGenerator

	// 消息可靠性保证
	reliabilityManager *ReliabilityManager

	// 消息排序去重
	messageProcessor *MessageProcessor

	// 事件处理
	eventHandler EventHandler

	// 配置
	config *ClientConfig

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 状态
	connected   int32 // 原子操作
	started     int32 // 原子操作
	syncingRoom int32 // 原子操作，防止重复的房间同步

	// 统计信息
	stats *ClientStats
}

// handleConnectionLoss 处理连接丢失
func (c *LiveChatClient) handleConnectionLoss() {
	if atomic.LoadInt32(&c.started) == 0 {
		return // 客户端已停止
	}

	log.Printf("Handling connection loss, attempting to reconnect...")

	// 尝试连接到其他可用服务器
	if err := c.switchToNextAvailableConnection(); err != nil {
		log.Printf("Failed to switch connection: %v", err)

		// 所有连接都失败，开始重连循环
		go c.reconnectLoop()
	}
}

// reconnectLoop 重连循环
func (c *LiveChatClient) reconnectLoop() {
	ticker := time.NewTicker(c.config.ReconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if atomic.LoadInt32(&c.connected) == 0 && atomic.LoadInt32(&c.started) == 1 {
				log.Printf("Attempting to reconnect...")
				if err := c.connectToAvailableServer(); err == nil {
					log.Printf("Reconnected successfully")

					// 同步房间状态，补拉断线期间的消息
					c.syncRoomAfterReconnect()

					return // 重连成功，退出循环
				}
			} else {
				return // 已连接或已停止，退出循环
			}
		}
	}
}

// ConnectionNode 连接节点
type ConnectionNode struct {
	addr       string
	client     *api.Client
	connection *connection.Connection // 实际的连接对象

	connected int32 // 原子操作
	lastPing  time.Time

	// 连接状态
	mutex sync.RWMutex
}

// ClientConfig 客户端配置 - 使用统一的配置结构
type ClientConfig = config.ClientConfig

// ClientStats 客户端统计信息
type ClientStats struct {
	TotalMessagesSent     int64
	TotalMessagesReceived int64
	TotalRetries          int64
	TotalFetches          int64
	ConnectionSwitches    int64
	TotalBytes            int64
	LastMessageTime       time.Time
}

// EventHandler 事件处理器接口
type EventHandler interface {
	// 连接事件
	OnConnected(serverAddr string)
	OnDisconnected(serverAddr string, err error)
	OnConnectionSwitched(fromAddr, toAddr string)

	// 消息事件
	OnMessageReceived(envelope *message.MessageEnvelope)
	OnMessageSent(envelope *message.MessageEnvelope)
	OnMessageDelivered(envelope *message.MessageEnvelope) // 收到ACK确认
	OnMessageFailed(envelope *message.MessageEnvelope, err error)

	// 房间事件
	OnRoomJoined(roomInfo *message.RoomInfo)
	OnRoomLeft(roomID uint64)
	OnRoomMemberJoined(userID uint64)
	OnRoomMemberLeft(userID uint64)

	// 错误事件
	OnError(err error)
}

// NewLiveChatClient 创建实时聊天客户端
func NewLiveChatClient(userID uint32, loginID uint8, config *ClientConfig, handler EventHandler) *LiveChatClient {
	if config == nil {
		config = DefaultClientConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &LiveChatClient{
		userID:       userID,
		loginID:      loginID,
		config:       config,
		eventHandler: handler,
		ctx:          ctx,
		cancel:       cancel,
		stats:        &ClientStats{},

		codec:            message.NewMessageCodec(),
		connSeqGenerator: message.NewConnSeqGenerator(),
	}

	// 初始化连接节点
	client.initConnections()

	// 初始化消息处理器
	client.messageProcessor = NewMessageProcessor(config.BufferSize)

	// 初始化可靠性管理器
	client.reliabilityManager = NewReliabilityManager(client, config)

	return client
}

// DefaultClientConfig 默认客户端配置 - 从全局配置获取
func DefaultClientConfig() *ClientConfig {
	if config.GlobalConf != nil {
		clientConfig := config.GetClientConfig()
		return &clientConfig
	}

	// 如果全局配置未加载，返回默认值
	return &ClientConfig{
		ServerAddrs:       []string{"ws://localhost:8080"},
		ConnectTimeout:    10 * time.Second,
		ReconnectInterval: 5 * time.Second,
		MessageTimeout:    10 * time.Second,
		MaxRetries:        3,
		HeartbeatInterval: 30 * time.Second,
		BufferSize:        1000,
	}
}

// initConnections 初始化连接节点
func (c *LiveChatClient) initConnections() {
	c.connections = make([]*ConnectionNode, len(c.config.ServerAddrs))

	for i, addr := range c.config.ServerAddrs {
		node := &ConnectionNode{
			addr: addr,
		}

		// 创建客户端配置
		clientConfig := &api.ClientConfig{
			ConnectTimeout:    c.config.ConnectTimeout,
			ReadTimeout:       c.config.MessageTimeout,
			WriteTimeout:      c.config.MessageTimeout,
			ReadBufferSize:    4096,
			WriteBufferSize:   4096,
			EnableReconnect:   false, // 重连由LiveChatClient统一管理
			ReconnectInterval: c.config.ReconnectInterval,
			MaxReconnectTries: c.config.MaxRetries,
			EnableHeartbeat:   false, // 心跳走应用层协议
			HeartbeatInterval: c.config.HeartbeatInterval,
			HeartbeatTimeout:  c.config.HeartbeatInterval,
		}

		// 创建网络事件处理器
		handler := &ClientNetworkHandler{
			client: c,
			node:   node,
		}

		// 创建客户端
		var err error
		node.client, err = api.NewClient(clientConfig, handler)
		if err != nil {
			log.Printf("Failed to create network client for %s: %v", addr, err)
			continue
		}

		c.connections[i] = node
	}
}

// Start 启动客户端
func (c *LiveChatClient) Start() error {
	if !atomic.CompareAndSwapInt32(&c.started, 0, 1) {
		return fmt.Errorf("client already started")
	}

	// 启动消息处理器
	c.messageProcessor.Start(c.ctx)

	// 启动可靠性管理器
	c.reliabilityManager.Start(c.ctx)

	// 尝试连接到第一个可用的服务器
	if err := c.connectToAvailableServer(); err != nil {
		atomic.StoreInt32(&c.started, 0)
		return fmt.Errorf("failed to connect to any server: %v", err)
	}

	// 启动心跳
	c.wg.Add(1)
	go c.heartbeatLoop()

	log.Printf("LiveChatClient started for user %d", c.userID)
	return nil
}

// Stop 停止客户端
func (c *LiveChatClient) Stop() error {
	if !atomic.CompareAndSwapInt32(&c.started, 1, 0) {
		return fmt.Errorf("client not started")
	}

	// 取消上下文
	c.cancel()

	// 断开所有连接
	c.connMutex.Lock()
	for _, node := range c.connections {
		if node.client != nil && atomic.LoadInt32(&node.connected) == 1 {
			node.client.Disconnect()
		}
	}
	c.connMutex.Unlock()

	// 等待所有协程结束
	c.wg.Wait()

	atomic.StoreInt32(&c.connected, 0)
	log.Printf("LiveChatClient stopped for user %d", c.userID)
	return nil
}

// connectToAvailableServer 连接到可用的服务器
func (c *LiveChatClient) connectToAvailableServer() error {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	for i, node := range c.connections {
		if err := c.connectToNode(node); err == nil {
			atomic.StoreInt32(&c.activeConnIndex, int32(i))
			atomic.StoreInt32(&c.connected, 1)
			if c.eventHandler != nil {
				c.eventHandler.OnConnected(node.addr)
			}
			return nil
		}
	}

	return fmt.Errorf("no server available")
}

// connectToNode 连接到指定节点
func (c *LiveChatClient) connectToNode(node *ConnectionNode) error {
	if atomic.LoadInt32(&node.connected) == 1 {
		return nil // 已经连接
	}

	if node.client == nil {
		return fmt.Errorf("network client not initialized for %s", node.addr)
	}

	// 底层socket只认host:port，这里把协议前缀去掉
	if err := node.client.Connect(parseServerAddr(node.addr)); err != nil {
		return fmt.Errorf("failed to connect to %s: %v", node.addr, err)
	}

	// 连接对象由ClientNetworkHandler.OnConnected设置
	atomic.StoreInt32(&node.connected, 1)
	node.lastPing = time.Now()
	return nil
}

// parseServerAddr 解析服务器地址，去掉ws/wss协议前缀
func parseServerAddr(addr string) string {
	if strings.HasPrefix(addr, "ws://") {
		return strings.TrimPrefix(addr, "ws://")
	}
	if strings.HasPrefix(addr, "wss://") {
		return strings.TrimPrefix(addr, "wss://")
	}
	return addr
}

// switchToNextAvailableConnection 切换到下一个可用连接
func (c *LiveChatClient) switchToNextAvailableConnection() error {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	currentIndex := int(atomic.LoadInt32(&c.activeConnIndex))
	oldAddr := c.connections[currentIndex].addr

	// 尝试连接到其他节点
	for i := 0; i < len(c.connections); i++ {
		nextIndex := (currentIndex + i + 1) % len(c.connections)
		node := c.connections[nextIndex]

		if err := c.connectToNode(node); err == nil {
			// 切换连接
			atomic.StoreInt32(&c.activeConnIndex, int32(nextIndex))
			atomic.AddInt64(&c.stats.ConnectionSwitches, 1)

			if c.eventHandler != nil {
				c.eventHandler.OnConnectionSwitched(oldAddr, node.addr)
			}

			// 切换后同步房间状态，补拉断线期间的消息
			c.syncRoomAfterReconnect()

			log.Printf("Switched connection from %s to %s", oldAddr, node.addr)
			return nil
		}
	}

	// 如果所有连接都失败，标记为断开连接
	atomic.StoreInt32(&c.connected, 0)
	return fmt.Errorf("no available connection")
}

// getActiveConnection 获取当前活跃连接
func (c *LiveChatClient) getActiveConnection() *ConnectionNode {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil
	}

	index := atomic.LoadInt32(&c.activeConnIndex)
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	if index >= 0 && int(index) < len(c.connections) {
		node := c.connections[index]
		if atomic.LoadInt32(&node.connected) == 1 {
			return node
		}
	}

	return nil
}

// SendMessage 发送消息（通用方法）
func (c *LiveChatClient) SendMessage(messageContent interface{}) error {
	if atomic.LoadInt32(&c.connected) == 0 {
		return fmt.Errorf("client not connected")
	}

	// 创建消息信封
	connSeq := c.connSeqGenerator.Next()
	envelope, err := c.codec.CreateEnvelope(c.userID, c.roomID, c.loginID, connSeq, messageContent)
	if err != nil {
		return fmt.Errorf("failed to create envelope: %v", err)
	}

	// 序列化消息
	data, err := c.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 通过可靠性管理器发送
	return c.reliabilityManager.SendMessage(envelope, data)
}

// 具体消息发送方法

// AllocateRoomID 分配房间ID
func (c *LiveChatClient) AllocateRoomID() error {
	msg := &message.AllocateRoomId{}
	return c.SendMessage(msg)
}

// CreateRoom 创建房间
func (c *LiveChatClient) CreateRoom(roomID uint64, name string, maxMembers uint32, password string) error {
	msg := &message.CreateRoom{
		RoomId:     roomID,
		Name:       name,
		MaxMembers: maxMembers,
		Password:   password,
	}
	c.roomID = uint32(roomID) // 设置当前房间ID
	return c.SendMessage(msg)
}

// JoinRoom 加入房间
func (c *LiveChatClient) JoinRoom(roomID uint64, password string) error {
	msg := &message.JoinRoom{
		Password: password,
	}
	c.roomID = uint32(roomID) // 设置当前房间ID
	return c.SendMessage(msg)
}

// LeaveRoom 离开房间
func (c *LiveChatClient) LeaveRoom() error {
	msg := &message.LeaveRoom{}
	return c.SendMessage(msg)
}

// CloseRoom 关闭房间
func (c *LiveChatClient) CloseRoom() error {
	msg := &message.CloseRoom{}
	return c.SendMessage(msg)
}

// SendChatMessage 发送聊天消息
func (c *LiveChatClient) SendChatMessage(content string, messageType message.MessageType, replyTo uint64) error {
	msg := &message.ChatMessage{
		Content:     content,
		MessageType: messageType,
		ReplyTo:     replyTo,
	}
	return c.SendMessage(msg)
}

// FetchMessage 拉取特定消息
func (c *LiveChatClient) FetchMessage(messageID []byte) error {
	msg := &message.MessageFetchRequest{
		MessageId: messageID,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	return c.SendMessage(msg)
}

// FetchRoomMessages 拉取房间消息
func (c *LiveChatClient) FetchRoomMessages(beforeMessageID uint64, limit uint32, sinceTimestamp uint64) error {
	msg := &message.RoomMessageFetchRequest{
		RoomId:          uint64(c.roomID),
		BeforeMessageId: beforeMessageID,
		Limit:           limit,
		SinceTimestamp:  sinceTimestamp,
	}
	return c.SendMessage(msg)
}

// SendHeartbeat 发送心跳
func (c *LiveChatClient) SendHeartbeat() error {
	msg := &message.Heartbeat{
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	return c.SendMessage(msg)
}

// heartbeatLoop 心跳循环
func (c *LiveChatClient) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if atomic.LoadInt32(&c.connected) == 1 {
				if err := c.SendHeartbeat(); err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
				}
			}
		}
	}
}

// handleIncomingMessage 处理接收到的消息
func (c *LiveChatClient) handleIncomingMessage(data []byte) error {
	// 反序列化消息
	envelope, err := c.codec.Deserialize(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %v", err)
	}

	// 更新统计信息
	atomic.AddInt64(&c.stats.TotalMessagesReceived, 1)
	c.stats.LastMessageTime = time.Now()

	// 检查消息是否为ACK确认
	if c.reliabilityManager.HandleACK(envelope) {
		return nil // ACK消息已处理
	}

	// 通过消息处理器处理消息（排序去重）
	c.messageProcessor.ProcessMessage(envelope)

	return nil
}

// GetStats 获取统计信息
func (c *LiveChatClient) GetStats() *ClientStats {
	return c.stats
}

// IsConnected 检查是否已连接
func (c *LiveChatClient) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// GetCurrentServerAddr 获取当前服务器地址
func (c *LiveChatClient) GetCurrentServerAddr() string {
	node := c.getActiveConnection()
	if node != nil {
		return node.addr
	}
	return ""
}

// GetRoomID 获取当前房间ID
func (c *LiveChatClient) GetRoomID() uint32 {
	return c.roomID
}

// syncRoomAfterReconnect 重连/切换节点后同步房间状态
// 断线期间的消息通过重新加入房间+拉取历史消息补回来
func (c *LiveChatClient) syncRoomAfterReconnect() {
	roomID := c.GetRoomID()
	if roomID == 0 || atomic.LoadInt32(&c.connected) == 0 {
		return // 不在房间中或未连接，无需同步
	}

	if !atomic.CompareAndSwapInt32(&c.syncingRoom, 0, 1) {
		return // 已有同步在进行中
	}

	go func() {
		defer atomic.StoreInt32(&c.syncingRoom, 0)

		// 等连接稳定后再同步
		time.Sleep(200 * time.Millisecond)

		// 重新加入房间（新连接上服务器不保留之前的会话状态）
		if err := c.JoinRoom(uint64(roomID), ""); err != nil {
			log.Printf("Failed to rejoin room %d after reconnect: %v", roomID, err)
			return
		}

		// 补拉断线期间的消息
		batchSize := uint32(c.config.FetchBatchSize)
		if batchSize == 0 {
			batchSize = 50
		}
		if err := c.FetchRoomMessages(0, batchSize, 0); err != nil {
			log.Printf("Failed to fetch room messages after reconnect: %v", err)
		} else {
			log.Printf("Room %d synced after reconnect", roomID)
		}
	}()
}
