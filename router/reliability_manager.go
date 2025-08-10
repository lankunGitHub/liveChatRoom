package router

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// RouterReliabilityManager 路由层可靠性管理器 - 实现统一的重传+fetch策略
type RouterReliabilityManager struct {
	server *RouterServer
	config *RouterConfig

	// 待确认消息
	pendingMessages map[string]*PendingRouterMessage
	pendingMutex    sync.RWMutex

	// 统计信息
	totalRetries   int64
	totalFetches   int64
	totalFailed    int64
	totalConfirmed int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// PendingRouterMessage 路由层待确认消息
type PendingRouterMessage struct {
	envelope     *message.MessageEnvelope
	data         []byte
	targetNode   *NodeConnection
	targetCenter *MessageCenterConnection
	sentTime     time.Time
	retries      int
	timer        *time.Timer
	messageType  string // "node" 或 "center"
	mutex        sync.Mutex
}

// NewRouterReliabilityManager 创建路由层可靠性管理器
func NewRouterReliabilityManager(server *RouterServer, config *RouterConfig) *RouterReliabilityManager {
	return &RouterReliabilityManager{
		server:          server,
		config:          config,
		pendingMessages: make(map[string]*PendingRouterMessage),
	}
}

// Start 启动可靠性管理器
func (rrm *RouterReliabilityManager) Start(ctx context.Context) {
	rrm.ctx, rrm.cancel = context.WithCancel(ctx)

	// 启动清理协程
	rrm.wg.Add(1)
	go rrm.cleanupLoop()

	log.Printf("RouterReliabilityManager started")
}

// Stop 停止可靠性管理器
func (rrm *RouterReliabilityManager) Stop() {
	if rrm.cancel != nil {
		rrm.cancel()
	}
	rrm.wg.Wait()

	log.Printf("RouterReliabilityManager stopped")
}

// SendReliableMessage 发送可靠消息到连接节点
func (rrm *RouterReliabilityManager) SendReliableMessage(node *NodeConnection, envelope *message.MessageEnvelope) error {
	// 序列化消息
	data, err := rrm.server.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 生成消息键
	messageKey := rrm.getMessageKey(envelope)

	// 创建待确认消息
	pending := &PendingRouterMessage{
		envelope:    envelope,
		data:        data,
		targetNode:  node,
		sentTime:    time.Now(),
		retries:     0,
		messageType: "node",
	}

	// 添加到待确认列表
	rrm.pendingMutex.Lock()
	rrm.pendingMessages[messageKey] = pending
	rrm.pendingMutex.Unlock()

	// 发送消息
	if err := node.SendSync(data); err != nil {
		rrm.removePendingMessage(messageKey)
		return fmt.Errorf("failed to send message to node: %v", err)
	}

	// 设置超时重传定时器
	pending.mutex.Lock()
	pending.timer = time.AfterFunc(rrm.config.MessageTimeout, func() {
		rrm.handleMessageTimeout(messageKey)
	})
	pending.mutex.Unlock()

	return nil
}

// SendReliableMessageToCenter 发送可靠消息到消息中心
func (rrm *RouterReliabilityManager) SendReliableMessageToCenter(center *MessageCenterConnection, envelope *message.MessageEnvelope) error {
	// 序列化消息
	data, err := rrm.server.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 生成消息键
	messageKey := rrm.getMessageKey(envelope)

	// 创建待确认消息
	pending := &PendingRouterMessage{
		envelope:     envelope,
		data:         data,
		targetCenter: center,
		sentTime:     time.Now(),
		retries:      0,
		messageType:  "center",
	}

	// 添加到待确认列表
	rrm.pendingMutex.Lock()
	rrm.pendingMessages[messageKey] = pending
	rrm.pendingMutex.Unlock()

	// 发送消息
	if err := center.SendSync(data); err != nil {
		rrm.removePendingMessage(messageKey)
		return fmt.Errorf("failed to send message to center: %v", err)
	}

	// 设置超时重传定时器
	pending.mutex.Lock()
	pending.timer = time.AfterFunc(rrm.config.MessagePersistTimeout, func() {
		rrm.handleMessageTimeout(messageKey)
	})
	pending.mutex.Unlock()

	return nil
}

// HandleACK 处理ACK确认消息
func (rrm *RouterReliabilityManager) HandleACK(envelope *message.MessageEnvelope) bool {
	if !rrm.isACKMessage(envelope) {
		return false
	}

	// 提取原始消息键
	originalMessageKey := rrm.getOriginalMessageKey(envelope)
	if originalMessageKey == "" {
		return true // 无法识别的ACK，但仍视为已处理
	}

	// 移除待确认消息
	pending := rrm.removePendingMessage(originalMessageKey)
	if pending != nil {
		// 停止重传定时器
		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		atomic.AddInt64(&rrm.totalConfirmed, 1)
		log.Printf("Message confirmed: %s", originalMessageKey)
	}

	return true
}

// handleMessageTimeout 处理消息超时
func (rrm *RouterReliabilityManager) handleMessageTimeout(messageKey string) {
	rrm.pendingMutex.RLock()
	pending, exists := rrm.pendingMessages[messageKey]
	rrm.pendingMutex.RUnlock()

	if !exists {
		return // 消息已被处理
	}

	pending.mutex.Lock()
	defer pending.mutex.Unlock()

	pending.retries++
	atomic.AddInt64(&rrm.totalRetries, 1)

	if pending.retries <= rrm.config.MaxRetries {
		// 重传消息
		log.Printf("Retrying message %s (attempt %d/%d)", messageKey, pending.retries, rrm.config.MaxRetries)

		var err error
		if pending.messageType == "node" && pending.targetNode != nil {
			err = pending.targetNode.SendSync(pending.data)
		} else if pending.messageType == "center" && pending.targetCenter != nil {
			err = pending.targetCenter.SendSync(pending.data)
		}

		if err != nil {
			log.Printf("Retry failed for message %s: %v", messageKey, err)
			// 如果重传失败，尝试切换节点
			rrm.attemptNodeSwitch(messageKey, pending)
		} else {
			// 重传成功，重新设置定时器
			if pending.timer != nil {
				pending.timer.Stop()
			}

			timeout := rrm.config.MessageTimeout
			if pending.messageType == "center" {
				timeout = rrm.config.MessagePersistTimeout
			}

			pending.timer = time.AfterFunc(timeout, func() {
				rrm.handleMessageTimeout(messageKey)
			})
		}
	} else {
		// 重传次数用尽，尝试fetch确认
		log.Printf("Max retries reached for message %s, attempting fetch", messageKey)
		rrm.attemptFetchConfirmation(messageKey, pending)
	}
}

// attemptNodeSwitch 尝试切换节点
func (rrm *RouterReliabilityManager) attemptNodeSwitch(messageKey string, pending *PendingRouterMessage) {
	if pending.messageType == "node" {
		// 切换到其他连接节点
		nodes := rrm.server.nodeManager.GetHealthyConnectionNodes()
		for _, node := range nodes {
			if node != pending.targetNode {
				pending.targetNode = node
				log.Printf("Switched to different connection node for message %s", messageKey)

				// 重新发送
				if err := node.SendSync(pending.data); err == nil {
					// 重新设置定时器
					if pending.timer != nil {
						pending.timer.Stop()
					}
					pending.timer = time.AfterFunc(rrm.config.MessageTimeout, func() {
						rrm.handleMessageTimeout(messageKey)
					})
					return
				}
			}
		}
	} else if pending.messageType == "center" {
		// 切换到其他消息中心
		centers := rrm.server.nodeManager.GetHealthyMessageCenters()
		for _, center := range centers {
			if center != pending.targetCenter {
				pending.targetCenter = center
				log.Printf("Switched to different message center for message %s", messageKey)

				// 重新发送
				if err := center.SendSync(pending.data); err == nil {
					// 重新设置定时器
					if pending.timer != nil {
						pending.timer.Stop()
					}
					pending.timer = time.AfterFunc(rrm.config.MessagePersistTimeout, func() {
						rrm.handleMessageTimeout(messageKey)
					})
					return
				}
			}
		}
	}

	// 无法切换节点，直接尝试fetch
	log.Printf("Failed to switch node for message %s, attempting fetch", messageKey)
	rrm.attemptFetchConfirmation(messageKey, pending)
}

// attemptFetchConfirmation 尝试通过fetch确认消息状态
func (rrm *RouterReliabilityManager) attemptFetchConfirmation(messageKey string, pending *PendingRouterMessage) {
	atomic.AddInt64(&rrm.totalFetches, 1)

	// 创建fetch请求
	fetchMsg := &message.MessageFetchRequest{
		MessageId: pending.envelope.MessageId,
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	// 创建fetch消息信封
	fetchEnvelope, err := rrm.server.codec.CreateEnvelope(
		0, // 系统查询
		0, // 无特定房间
		0, // 系统登录ID
		rrm.server.connSeqGenerator.Next(),
		fetchMsg,
	)
	if err != nil {
		rrm.handleMessageFailure(messageKey, fmt.Errorf("failed to create fetch envelope: %v", err))
		return
	}

	// 向消息中心发送fetch请求
	messageCenter := rrm.server.loadBalancer.SelectMessageCenter()
	if messageCenter == nil {
		rrm.handleMessageFailure(messageKey, fmt.Errorf("no available message center for fetch"))
		return
	}

	if err := rrm.server.sendMessageToCenterSync(messageCenter, fetchEnvelope); err != nil {
		rrm.handleMessageFailure(messageKey, fmt.Errorf("failed to send fetch request: %v", err))
		return
	}

	// 设置fetch超时
	pending.timer = time.AfterFunc(rrm.config.FetchTimeout, func() {
		rrm.handleFetchTimeout(messageKey)
	})

	log.Printf("Fetch request sent for message %s", messageKey)
}

// handleFetchTimeout fetch超时处理
func (rrm *RouterReliabilityManager) handleFetchTimeout(messageKey string) {
	rrm.handleMessageFailure(messageKey, fmt.Errorf("fetch timeout"))
}

// HandleFetchResponse 处理fetch响应
func (rrm *RouterReliabilityManager) HandleFetchResponse(envelope *message.MessageEnvelope) {
	switch msg := envelope.Message.(type) {
	case *message.MessageEnvelope_MessageFetchResponse:
		rrm.handleMessageFetchResponse(msg.MessageFetchResponse)
	case *message.MessageEnvelope_RoomMessageFetchResponse:
		// 房间消息fetch响应，暂时不处理具体逻辑
		log.Printf("Received room message fetch response")
	}
}

// handleMessageFetchResponse 处理消息fetch响应
func (rrm *RouterReliabilityManager) handleMessageFetchResponse(response *message.MessageFetchResponse) {
	messageKey := fmt.Sprintf("%x", response.MessageId)

	pending := rrm.removePendingMessage(messageKey)
	if pending == nil {
		return // 消息已被处理
	}

	pending.mutex.Lock()
	if pending.timer != nil {
		pending.timer.Stop()
	}
	pending.mutex.Unlock()

	if response.Exists && response.Status == message.MessageStatus_MESSAGE_STATUS_SUCCESS {
		// 消息存在且成功，说明发送成功
		atomic.AddInt64(&rrm.totalConfirmed, 1)
		log.Printf("Message confirmed via fetch: %s", messageKey)
	} else {
		// 消息不存在或失败，说明发送失败
		rrm.handleMessageFailure(messageKey, fmt.Errorf("message not found or failed on server"))
	}
}

// handleMessageFailure 处理消息失败
func (rrm *RouterReliabilityManager) handleMessageFailure(messageKey string, err error) {
	pending := rrm.removePendingMessage(messageKey)
	if pending != nil {
		// 停止定时器
		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		atomic.AddInt64(&rrm.totalFailed, 1)
		log.Printf("Message failed: %s, error: %v", messageKey, err)
	}
}

// removePendingMessage 移除待确认消息
func (rrm *RouterReliabilityManager) removePendingMessage(messageKey string) *PendingRouterMessage {
	rrm.pendingMutex.Lock()
	defer rrm.pendingMutex.Unlock()

	pending, exists := rrm.pendingMessages[messageKey]
	if exists {
		delete(rrm.pendingMessages, messageKey)
	}
	return pending
}

// getMessageKey 获取消息键
func (rrm *RouterReliabilityManager) getMessageKey(envelope *message.MessageEnvelope) string {
	return fmt.Sprintf("%x", envelope.MessageId)
}

// getOriginalMessageKey 从ACK消息中提取原始消息键
func (rrm *RouterReliabilityManager) getOriginalMessageKey(envelope *message.MessageEnvelope) string {
	// 根据不同的ACK类型提取原始消息ID
	switch ack := envelope.Message.(type) {
	case *message.MessageEnvelope_AllocateRoomIdAck:
		return fmt.Sprintf("room_alloc_%d", ack.AllocateRoomIdAck.RoomId)
	case *message.MessageEnvelope_CreateRoomAck:
		return fmt.Sprintf("room_create_%d", ack.CreateRoomAck.RoomId)
	case *message.MessageEnvelope_JoinRoomAck:
		return fmt.Sprintf("room_join_%d", ack.JoinRoomAck.RoomId)
	case *message.MessageEnvelope_LeaveRoomAck:
		return fmt.Sprintf("room_leave_%d", ack.LeaveRoomAck.RoomId)
	case *message.MessageEnvelope_CloseRoomAck:
		return fmt.Sprintf("room_close_%d", ack.CloseRoomAck.RoomId)
	case *message.MessageEnvelope_ChatMessageAck:
		return fmt.Sprintf("chat_%d", ack.ChatMessageAck.RoomId)
	case *message.MessageEnvelope_HeartbeatAck:
		// 心跳ACK，可以忽略
		return ""
	default:
		// 其他ACK类型，尝试使用消息ID
		return fmt.Sprintf("%x", envelope.MessageId)
	}
}

// isACKMessage 检查是否是ACK消息
func (rrm *RouterReliabilityManager) isACKMessage(envelope *message.MessageEnvelope) bool {
	switch envelope.Message.(type) {
	case *message.MessageEnvelope_AllocateRoomIdAck,
		*message.MessageEnvelope_CreateRoomAck,
		*message.MessageEnvelope_JoinRoomAck,
		*message.MessageEnvelope_LeaveRoomAck,
		*message.MessageEnvelope_CloseRoomAck,
		*message.MessageEnvelope_ChatMessageAck,
		*message.MessageEnvelope_MessageFetchResponse,
		*message.MessageEnvelope_RoomMessageFetchResponse,
		*message.MessageEnvelope_HeartbeatAck:
		return true
	default:
		return false
	}
}

// cleanupLoop 清理循环
func (rrm *RouterReliabilityManager) cleanupLoop() {
	defer rrm.wg.Done()

	ticker := time.NewTicker(1 * time.Minute) // 每分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-rrm.ctx.Done():
			return
		case <-ticker.C:
			rrm.cleanup()
		}
	}
}

// cleanup 执行清理
func (rrm *RouterReliabilityManager) cleanup() {
	now := time.Now()
	maxAge := rrm.config.PendingMessageTTL

	rrm.pendingMutex.Lock()
	defer rrm.pendingMutex.Unlock()

	expiredKeys := make([]string, 0)
	for key, pending := range rrm.pendingMessages {
		if now.Sub(pending.sentTime) > maxAge {
			// 停止定时器
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
		log.Printf("Cleaned up %d expired pending messages", len(expiredKeys))
	}
}

// GetStats 获取统计信息
func (rrm *RouterReliabilityManager) GetStats() map[string]interface{} {
	rrm.pendingMutex.RLock()
	pendingCount := len(rrm.pendingMessages)
	rrm.pendingMutex.RUnlock()

	return map[string]interface{}{
		"total_retries":    atomic.LoadInt64(&rrm.totalRetries),
		"total_fetches":    atomic.LoadInt64(&rrm.totalFetches),
		"total_failed":     atomic.LoadInt64(&rrm.totalFailed),
		"total_confirmed":  atomic.LoadInt64(&rrm.totalConfirmed),
		"pending_messages": pendingCount,
	}
}
