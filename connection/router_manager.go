package connection

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"liveChatroom/util/net/api"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// RouterManager 路由管理器 - 负责与路由节点的通信
type RouterManager struct {
	// 路由节点连接
	routers     map[string]*RouterConnection
	routerMutex sync.RWMutex

	// 配置
	routerAddrs       []string
	connectTimeout    time.Duration
	retryInterval     time.Duration
	heartbeatInterval time.Duration

	// 消息编解码
	codec            *message.MessageCodec
	connSeqGenerator *message.ConnSeqGenerator

	// 当前活跃路由索引
	activeRouterIndex int32

	// 可靠性保证器（用于与路由节点的通信）
	reliabilityManager *RouterReliabilityManager

	// 统计信息
	totalMessages  int64
	failedMessages int64
	routerSwitches int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// RouterConnection 路由节点连接
type RouterConnection struct {
	addr       string
	client     *api.Client
	connection *connection.Connection

	connected        int32 // 原子操作
	lastHeartbeat    time.Time
	messagesSent     int64
	messagesReceived int64

	mutex sync.RWMutex
}

// RouterReliabilityManager 路由通信可靠性管理器
type RouterReliabilityManager struct {
	routerManager   *RouterManager
	pendingMessages map[string]*PendingRouterMessage
	pendingMutex    sync.RWMutex
	messageTimeout  time.Duration
	maxRetries      int
}

// PendingRouterMessage 待确认的路由消息
type PendingRouterMessage struct {
	envelope   *message.MessageEnvelope
	data       []byte
	sentTime   time.Time
	retries    int
	timer      *time.Timer
	targetAddr string
	mutex      sync.Mutex
}

// NewRouterManager 创建路由管理器
func NewRouterManager(routerAddrs []string, config *ConnectionConfig) *RouterManager {
	rm := &RouterManager{
		routers:           make(map[string]*RouterConnection),
		routerAddrs:       routerAddrs,
		connectTimeout:    config.RouterConnectTimeout,
		retryInterval:     config.RouterRetryInterval,
		heartbeatInterval: config.HeartbeatInterval,
		codec:             message.NewMessageCodec(),
		connSeqGenerator:  message.NewConnSeqGenerator(),
	}

	// 初始化可靠性管理器
	rm.reliabilityManager = &RouterReliabilityManager{
		routerManager:   rm,
		pendingMessages: make(map[string]*PendingRouterMessage),
		messageTimeout:  config.MessageTimeout,
		maxRetries:      config.MaxRetries,
	}

	return rm
}

// Start 启动路由管理器
func (rm *RouterManager) Start(ctx context.Context) {
	rm.ctx, rm.cancel = context.WithCancel(ctx)

	// 初始化路由连接
	rm.initRouterConnections()

	// 启动连接管理协程
	rm.wg.Add(1)
	go rm.connectionLoop()

	// 启动可靠性管理协程
	rm.wg.Add(1)
	go rm.reliabilityManager.start(rm.ctx)

	log.Printf("RouterManager started with %d router addresses", len(rm.routerAddrs))
}

// Stop 停止路由管理器
func (rm *RouterManager) Stop() {
	if rm.cancel != nil {
		rm.cancel()
	}
	rm.wg.Wait()

	// 关闭所有路由连接
	rm.closeAllConnections()

	log.Printf("RouterManager stopped")
}

// initRouterConnections 初始化路由连接
func (rm *RouterManager) initRouterConnections() {
	for _, addr := range rm.routerAddrs {
		rm.createRouterConnection(addr)
	}

	// 尝试连接到第一个可用的路由节点
	rm.connectToAvailableRouter()
}

// createRouterConnection 创建路由连接
func (rm *RouterManager) createRouterConnection(addr string) {
	routerConn := &RouterConnection{
		addr: addr,
	}

	// 创建客户端配置
	clientConfig := &api.ClientConfig{
		ConnectTimeout:    rm.connectTimeout,
		ReadTimeout:       rm.connectTimeout,
		WriteTimeout:      rm.connectTimeout,
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
		EnableReconnect:   true,
		ReconnectInterval: rm.retryInterval,
		MaxReconnectTries: 3,
		EnableHeartbeat:   true,
		HeartbeatInterval: rm.heartbeatInterval,
		HeartbeatTimeout:  10 * time.Second,
	}

	// 创建路由客户端事件处理器
	handler := &RouterClientHandler{routerConn: routerConn}

	var err error
	routerConn.client, err = api.NewClient(clientConfig, handler)
	if err != nil {
		log.Printf("Failed to create router client for %s: %v", addr, err)
		return
	}

	rm.routerMutex.Lock()
	rm.routers[addr] = routerConn
	rm.routerMutex.Unlock()
}

// GetRouterAddrs 获取配置的路由节点地址列表
func (rm *RouterManager) GetRouterAddrs() []string {
	rm.routerMutex.RLock()
	defer rm.routerMutex.RUnlock()

	addrs := make([]string, 0, len(rm.routers))
	for addr := range rm.routers {
		addrs = append(addrs, addr)
	}
	return addrs
}

// connectToAvailableRouter 连接到可用路由节点
func (rm *RouterManager) connectToAvailableRouter() error {
	rm.routerMutex.RLock()
	addrs := make([]string, 0, len(rm.routers))
	for addr := range rm.routers {
		addrs = append(addrs, addr)
	}
	rm.routerMutex.RUnlock()

	for i, addr := range addrs {
		if err := rm.connectToRouter(addr); err == nil {
			atomic.StoreInt32(&rm.activeRouterIndex, int32(i))
			log.Printf("Connected to router: %s", addr)
			return nil
		}
	}

	return fmt.Errorf("failed to connect to any router")
}

// connectToRouter 连接到指定路由节点
func (rm *RouterManager) connectToRouter(addr string) error {
	rm.routerMutex.RLock()
	routerConn, exists := rm.routers[addr]
	rm.routerMutex.RUnlock()

	if !exists {
		return fmt.Errorf("router %s not found", addr)
	}

	if atomic.LoadInt32(&routerConn.connected) == 1 {
		return nil // 已连接
	}

	// 建立连接
	if err := routerConn.client.Connect(addr); err != nil {
		return fmt.Errorf("failed to connect to router %s: %v", addr, err)
	}

	// 连接状态和connection对象通过RouterClientHandler设置

	return nil
}

// SendMessageToRouter 发送消息到路由节点（带可靠性保证）
func (rm *RouterManager) SendMessageToRouter(envelope *message.MessageEnvelope) error {
	// 序列化消息
	data, err := rm.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 通过可靠性管理器发送
	return rm.reliabilityManager.sendMessage(envelope, data)
}

// SendMessageToRouterSync 同步发送消息到路由节点
func (rm *RouterManager) SendMessageToRouterSync(envelope *message.MessageEnvelope) error {
	router := rm.getActiveRouter()
	if router == nil {
		return fmt.Errorf("no active router connection")
	}

	data, err := rm.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	return rm.sendToRouter(router, data)
}

// sendToRouter 发送数据到指定路由节点
func (rm *RouterManager) sendToRouter(router *RouterConnection, data []byte) error {
	if atomic.LoadInt32(&router.connected) == 0 {
		return fmt.Errorf("router %s not connected", router.addr)
	}

	if router.connection == nil {
		return fmt.Errorf("router connection is nil")
	}

	_, err := router.connection.Write(data)
	if err != nil {
		atomic.AddInt64(&rm.failedMessages, 1)
		return fmt.Errorf("failed to write to router %s: %v", router.addr, err)
	}

	atomic.AddInt64(&rm.totalMessages, 1)
	atomic.AddInt64(&router.messagesSent, 1)
	return nil
}

// getActiveRouter 获取当前活跃路由连接
func (rm *RouterManager) getActiveRouter() *RouterConnection {
	index := atomic.LoadInt32(&rm.activeRouterIndex)

	rm.routerMutex.RLock()
	defer rm.routerMutex.RUnlock()

	i := 0
	for _, router := range rm.routers {
		if int32(i) == index && atomic.LoadInt32(&router.connected) == 1 {
			return router
		}
		i++
	}

	return nil
}

// switchToNextRouter 切换到下一个可用路由节点
func (rm *RouterManager) switchToNextRouter() error {
	rm.routerMutex.RLock()
	addrs := make([]string, 0, len(rm.routers))
	for addr := range rm.routers {
		addrs = append(addrs, addr)
	}
	rm.routerMutex.RUnlock()

	currentIndex := int(atomic.LoadInt32(&rm.activeRouterIndex))

	for i := 0; i < len(addrs); i++ {
		nextIndex := (currentIndex + i + 1) % len(addrs)
		addr := addrs[nextIndex]

		if err := rm.connectToRouter(addr); err == nil {
			atomic.StoreInt32(&rm.activeRouterIndex, int32(nextIndex))
			atomic.AddInt64(&rm.routerSwitches, 1)
			log.Printf("Switched to router: %s", addr)
			return nil
		}
	}

	return fmt.Errorf("no available router")
}

// SendHeartbeat 发送心跳到所有路由节点
func (rm *RouterManager) SendHeartbeat() {
	heartbeat := &message.Heartbeat{
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	// 为心跳创建一个特殊的消息信封
	envelope, err := rm.codec.CreateEnvelope(
		0, // 系统消息
		0, // 无房间
		0, // 无登录ID
		rm.connSeqGenerator.Next(),
		heartbeat,
	)
	if err != nil {
		log.Printf("Failed to create heartbeat envelope: %v", err)
		return
	}

	data, err := rm.codec.Serialize(envelope)
	if err != nil {
		log.Printf("Failed to serialize heartbeat: %v", err)
		return
	}

	rm.routerMutex.RLock()
	routers := make([]*RouterConnection, 0, len(rm.routers))
	for _, router := range rm.routers {
		if atomic.LoadInt32(&router.connected) == 1 {
			routers = append(routers, router)
		}
	}
	rm.routerMutex.RUnlock()

	for _, router := range routers {
		if err := rm.sendToRouter(router, data); err != nil {
			log.Printf("Failed to send heartbeat to router %s: %v", router.addr, err)
		} else {
			router.mutex.Lock()
			router.lastHeartbeat = time.Now()
			router.mutex.Unlock()
		}
	}
}

// connectionLoop 连接管理循环
func (rm *RouterManager) connectionLoop() {
	defer rm.wg.Done()

	retryTicker := time.NewTicker(rm.retryInterval)
	defer retryTicker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-retryTicker.C:
			rm.checkAndReconnect()
		}
	}
}

// checkAndReconnect 检查并重连断开的路由节点
func (rm *RouterManager) checkAndReconnect() {
	rm.routerMutex.RLock()
	disconnectedAddrs := make([]string, 0)
	for addr, router := range rm.routers {
		if atomic.LoadInt32(&router.connected) == 0 {
			disconnectedAddrs = append(disconnectedAddrs, addr)
		}
	}
	rm.routerMutex.RUnlock()

	for _, addr := range disconnectedAddrs {
		if err := rm.connectToRouter(addr); err != nil {
			log.Printf("Failed to reconnect to router %s: %v", addr, err)
		} else {
			log.Printf("Reconnected to router: %s", addr)
		}
	}
}

// closeAllConnections 关闭所有连接
func (rm *RouterManager) closeAllConnections() {
	rm.routerMutex.Lock()
	defer rm.routerMutex.Unlock()

	for _, router := range rm.routers {
		if router.connection != nil {
			router.connection.Close()
		}
		if router.client != nil {
			router.client.Disconnect()
		}
		atomic.StoreInt32(&router.connected, 0)
	}
}

// GetStats 获取统计信息
func (rm *RouterManager) GetStats() map[string]interface{} {
	rm.routerMutex.RLock()
	defer rm.routerMutex.RUnlock()

	connectedCount := 0
	for _, router := range rm.routers {
		if atomic.LoadInt32(&router.connected) == 1 {
			connectedCount++
		}
	}

	return map[string]interface{}{
		"total_routers":       len(rm.routers),
		"connected_routers":   connectedCount,
		"active_router_index": atomic.LoadInt32(&rm.activeRouterIndex),
		"total_messages":      atomic.LoadInt64(&rm.totalMessages),
		"failed_messages":     atomic.LoadInt64(&rm.failedMessages),
		"router_switches":     atomic.LoadInt64(&rm.routerSwitches),
	}
}

// RouterReliabilityManager 方法

// start 启动可靠性管理器
func (rrm *RouterReliabilityManager) start(ctx context.Context) {
	defer rrm.routerManager.wg.Done()

	cleanupTicker := time.NewTicker(1 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			rrm.cleanup()
		}
	}
}

// sendMessage 发送消息（带重传机制）
func (rrm *RouterReliabilityManager) sendMessage(envelope *message.MessageEnvelope, data []byte) error {
	router := rrm.routerManager.getActiveRouter()
	if router == nil {
		return fmt.Errorf("no active router")
	}

	messageKey := fmt.Sprintf("%x", envelope.MessageId)

	// 创建待确认消息
	pending := &PendingRouterMessage{
		envelope:   envelope,
		data:       data,
		sentTime:   time.Now(),
		retries:    0,
		targetAddr: router.addr,
	}

	// 添加到待确认列表
	rrm.pendingMutex.Lock()
	rrm.pendingMessages[messageKey] = pending
	rrm.pendingMutex.Unlock()

	// 发送消息
	if err := rrm.routerManager.sendToRouter(router, data); err != nil {
		rrm.removePendingMessage(messageKey)
		return err
	}

	// 设置超时重传定时器
	pending.mutex.Lock()
	pending.timer = time.AfterFunc(rrm.messageTimeout, func() {
		rrm.handleMessageTimeout(messageKey)
	})
	pending.mutex.Unlock()

	return nil
}

// handleMessageTimeout 处理消息超时
func (rrm *RouterReliabilityManager) handleMessageTimeout(messageKey string) {
	rrm.pendingMutex.RLock()
	pending, exists := rrm.pendingMessages[messageKey]
	rrm.pendingMutex.RUnlock()

	if !exists {
		return
	}

	pending.mutex.Lock()
	defer pending.mutex.Unlock()

	pending.retries++

	if pending.retries <= rrm.maxRetries {
		// 重传消息
		router := rrm.routerManager.getActiveRouter()
		if router != nil && router.addr == pending.targetAddr {
			// 同一路由节点重传
			if err := rrm.routerManager.sendToRouter(router, pending.data); err == nil {
				// 重传成功，重新设置定时器
				if pending.timer != nil {
					pending.timer.Stop()
				}
				pending.timer = time.AfterFunc(rrm.messageTimeout, func() {
					rrm.handleMessageTimeout(messageKey)
				})
				return
			}
		}

		// 切换路由节点后重传
		if err := rrm.routerManager.switchToNextRouter(); err == nil {
			newRouter := rrm.routerManager.getActiveRouter()
			if newRouter != nil {
				pending.targetAddr = newRouter.addr
				if err := rrm.routerManager.sendToRouter(newRouter, pending.data); err == nil {
					// 重传成功
					if pending.timer != nil {
						pending.timer.Stop()
					}
					pending.timer = time.AfterFunc(rrm.messageTimeout, func() {
						rrm.handleMessageTimeout(messageKey)
					})
					return
				}
			}
		}
	}

	// 重传失败，移除消息
	rrm.removePendingMessage(messageKey)
	log.Printf("Router message failed after %d retries: %s", pending.retries, messageKey)
}

// removePendingMessage 移除待确认消息
func (rrm *RouterReliabilityManager) removePendingMessage(messageKey string) *PendingRouterMessage {
	rrm.pendingMutex.Lock()
	defer rrm.pendingMutex.Unlock()

	pending, exists := rrm.pendingMessages[messageKey]
	if exists {
		delete(rrm.pendingMessages, messageKey)
		if pending != nil {
			pending.mutex.Lock()
			if pending.timer != nil {
				pending.timer.Stop()
			}
			pending.mutex.Unlock()
		}
	}
	return pending
}

// cleanup 清理过期消息
func (rrm *RouterReliabilityManager) cleanup() {
	now := time.Now()
	maxAge := 5 * time.Minute

	rrm.pendingMutex.Lock()
	defer rrm.pendingMutex.Unlock()

	expiredKeys := make([]string, 0)
	for key, pending := range rrm.pendingMessages {
		if now.Sub(pending.sentTime) > maxAge {
			pending.mutex.Lock()
			if pending.timer != nil {
				pending.timer.Stop()
			}
			pending.mutex.Unlock()
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		delete(rrm.pendingMessages, key)
	}

	if len(expiredKeys) > 0 {
		log.Printf("Cleaned up %d expired router messages", len(expiredKeys))
	}
}

// RouterClientHandler 路由客户端事件处理器
type RouterClientHandler struct {
	routerConn *RouterConnection
}

func (h *RouterClientHandler) OnConnected(client *api.Client) {
	atomic.StoreInt32(&h.routerConn.connected, 1)
	h.routerConn.connection = client.GetConnection()
	log.Printf("Connected to router: %s", h.routerConn.addr)
}

func (h *RouterClientHandler) OnDisconnected(client *api.Client, err error) {
	atomic.StoreInt32(&h.routerConn.connected, 0)
	h.routerConn.connection = nil
	if err != nil {
		log.Printf("Disconnected from router %s: %v", h.routerConn.addr, err)
	} else {
		log.Printf("Disconnected from router: %s", h.routerConn.addr)
	}
}

func (h *RouterClientHandler) OnReconnected(client *api.Client, attempt int) {
	atomic.StoreInt32(&h.routerConn.connected, 1)
	h.routerConn.connection = client.GetConnection()
	log.Printf("Reconnected to router %s after %d attempts", h.routerConn.addr, attempt)
}

func (h *RouterClientHandler) OnMessageReceived(client *api.Client, msg protocol.Message) error {
	atomic.AddInt64(&h.routerConn.messagesReceived, 1)
	// 处理从路由节点接收到的消息
	return nil
}

func (h *RouterClientHandler) OnHeartbeatSent(client *api.Client) {
	h.routerConn.lastHeartbeat = time.Now()
}

func (h *RouterClientHandler) OnHeartbeatReceived(client *api.Client) {
	// 心跳接收处理
}

func (h *RouterClientHandler) OnError(client *api.Client, err error) {
	log.Printf("Router client error for %s: %v", h.routerConn.addr, err)
}
