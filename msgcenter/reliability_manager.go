package msgcenter

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// CenterReliabilityManager 消息中心可靠性管理器 - 实现统一的重传+fetch策略
type CenterReliabilityManager struct {
	server *MessageCenterServer
	config *MessageCenterConfig

	// 待确认消息
	pendingMessages map[string]*PendingCenterMessage
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

// PendingCenterMessage 消息中心待确认消息
type PendingCenterMessage struct {
	envelope     *message.MessageEnvelope
	data         []byte
	targetRouter *RouterConnection
	sentTime     time.Time
	retries      int
	timer        *time.Timer
	messageType  string // "response" 或 "notification"
	mutex        sync.Mutex
}

// NewCenterReliabilityManager 创建消息中心可靠性管理器
func NewCenterReliabilityManager(server *MessageCenterServer, config *MessageCenterConfig) *CenterReliabilityManager {
	return &CenterReliabilityManager{
		server:          server,
		config:          config,
		pendingMessages: make(map[string]*PendingCenterMessage),
	}
}

// Start 启动可靠性管理器
func (crm *CenterReliabilityManager) Start(ctx context.Context) {
	crm.ctx, crm.cancel = context.WithCancel(ctx)

	// 启动清理协程
	crm.wg.Add(1)
	go crm.cleanupLoop()

	log.Printf("CenterReliabilityManager started")
}

// Stop 停止可靠性管理器
func (crm *CenterReliabilityManager) Stop() {
	if crm.cancel != nil {
		crm.cancel()
	}
	crm.wg.Wait()

	log.Printf("CenterReliabilityManager stopped")
}

// SendReliableResponse 发送可靠响应到路由节点
func (crm *CenterReliabilityManager) SendReliableResponse(routerConn *RouterConnection, envelope *message.MessageEnvelope) error {
	// 序列化消息
	data, err := crm.server.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 生成消息键
	messageKey := crm.getMessageKey(envelope)

	// 创建待确认消息
	pending := &PendingCenterMessage{
		envelope:     envelope,
		data:         data,
		targetRouter: routerConn,
		sentTime:     time.Now(),
		retries:      0,
		messageType:  "response",
	}

	// 添加到待确认列表
	crm.pendingMutex.Lock()
	crm.pendingMessages[messageKey] = pending
	crm.pendingMutex.Unlock()

	// 发送消息
	if err := routerConn.SendSync(data); err != nil {
		crm.removePendingMessage(messageKey)
		return fmt.Errorf("failed to send message to router: %v", err)
	}

	// 设置超时重传定时器
	pending.mutex.Lock()
	pending.timer = time.AfterFunc(crm.config.MessageTimeout, func() {
		crm.handleMessageTimeout(messageKey)
	})
	pending.mutex.Unlock()

	return nil
}

// SendReliableNotification 发送可靠通知到路由节点
func (crm *CenterReliabilityManager) SendReliableNotification(routerConn *RouterConnection, envelope *message.MessageEnvelope) error {
	// 序列化消息
	data, err := crm.server.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 生成消息键
	messageKey := crm.getMessageKey(envelope)

	// 创建待确认消息
	pending := &PendingCenterMessage{
		envelope:     envelope,
		data:         data,
		targetRouter: routerConn,
		sentTime:     time.Now(),
		retries:      0,
		messageType:  "notification",
	}

	// 添加到待确认列表
	crm.pendingMutex.Lock()
	crm.pendingMessages[messageKey] = pending
	crm.pendingMutex.Unlock()

	// 发送消息
	if err := routerConn.SendSync(data); err != nil {
		crm.removePendingMessage(messageKey)
		return fmt.Errorf("failed to send notification to router: %v", err)
	}

	// 设置超时重传定时器
	pending.mutex.Lock()
	pending.timer = time.AfterFunc(crm.config.MessageTimeout, func() {
		crm.handleMessageTimeout(messageKey)
	})
	pending.mutex.Unlock()

	return nil
}

// HandleACK 处理ACK确认消息
func (crm *CenterReliabilityManager) HandleACK(envelope *message.MessageEnvelope) bool {
	if !crm.isACKMessage(envelope) {
		return false
	}

	// 提取原始消息键
	originalMessageKey := crm.getOriginalMessageKey(envelope)
	if originalMessageKey == "" {
		return true // 无法识别的ACK，但仍视为已处理
	}

	// 移除待确认消息
	pending := crm.removePendingMessage(originalMessageKey)
	if pending != nil {
		// 停止重传定时器
		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		atomic.AddInt64(&crm.totalConfirmed, 1)
		log.Printf("Center message confirmed: %s", originalMessageKey)
	}

	return true
}

// handleMessageTimeout 处理消息超时
func (crm *CenterReliabilityManager) handleMessageTimeout(messageKey string) {
	crm.pendingMutex.RLock()
	pending, exists := crm.pendingMessages[messageKey]
	crm.pendingMutex.RUnlock()

	if !exists {
		return // 消息已被处理
	}

	pending.mutex.Lock()
	defer pending.mutex.Unlock()

	pending.retries++
	atomic.AddInt64(&crm.totalRetries, 1)

	if pending.retries <= crm.config.MaxRetries {
		// 重传消息
		log.Printf("Retrying center message %s (attempt %d/%d)", messageKey, pending.retries, crm.config.MaxRetries)

		var err error
		if pending.targetRouter != nil {
			err = pending.targetRouter.SendSync(pending.data)
		}

		if err != nil {
			log.Printf("Retry failed for center message %s: %v", messageKey, err)
			// 消息中心的重传失败通常意味着路由节点不可用
			// 这里可以实现更复杂的故障处理逻辑
		}

		// 重新设置定时器
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.timer = time.AfterFunc(crm.config.MessageTimeout, func() {
			crm.handleMessageTimeout(messageKey)
		})
	} else {
		// 重传次数用尽，尝试其他处理方式
		log.Printf("Max retries reached for center message %s, attempting alternative handling", messageKey)
		crm.attemptAlternativeHandling(messageKey, pending)
	}
}

// attemptAlternativeHandling 尝试替代处理方式
func (crm *CenterReliabilityManager) attemptAlternativeHandling(messageKey string, pending *PendingCenterMessage) {
	atomic.AddInt64(&crm.totalFetches, 1)

	// 对于消息中心，"fetch"的概念不同于客户端和路由层
	// 这里可以实现：
	// 1. 记录失败的消息，等待路由节点重新连接后重发
	// 2. 将失败的消息存储到重试队列
	// 3. 发送到备用路由节点（如果有的话）

	// 简化实现：标记消息失败并清理
	crm.handleMessageFailure(messageKey, fmt.Errorf("router node unavailable after retries"))
}

// handleMessageFailure 处理消息失败
func (crm *CenterReliabilityManager) handleMessageFailure(messageKey string, err error) {
	pending := crm.removePendingMessage(messageKey)
	if pending != nil {
		// 停止定时器
		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		atomic.AddInt64(&crm.totalFailed, 1)
		log.Printf("Center message failed: %s, error: %v", messageKey, err)

		// 可以在这里实现失败消息的特殊处理
		// 例如：存储到失败队列，等待路由节点恢复后重试
	}
}

// removePendingMessage 移除待确认消息
func (crm *CenterReliabilityManager) removePendingMessage(messageKey string) *PendingCenterMessage {
	crm.pendingMutex.Lock()
	defer crm.pendingMutex.Unlock()

	pending, exists := crm.pendingMessages[messageKey]
	if exists {
		delete(crm.pendingMessages, messageKey)
	}
	return pending
}

// getMessageKey 获取消息键
func (crm *CenterReliabilityManager) getMessageKey(envelope *message.MessageEnvelope) string {
	return fmt.Sprintf("%x", envelope.MessageId)
}

// getOriginalMessageKey 从ACK消息中提取原始消息键
func (crm *CenterReliabilityManager) getOriginalMessageKey(envelope *message.MessageEnvelope) string {
	// 根据不同的ACK类型提取原始消息ID
	switch ack := envelope.Message.(type) {
	case *message.MessageEnvelope_MessageFetchResponse:
		return fmt.Sprintf("%x", ack.MessageFetchResponse.MessageId)
	case *message.MessageEnvelope_RoomMessageFetchResponse:
		return fmt.Sprintf("room_query_%d", ack.RoomMessageFetchResponse.RoomId)
	case *message.MessageEnvelope_HeartbeatAck:
		// 心跳ACK，可以忽略
		return ""
	default:
		// 其他ACK类型，尝试使用消息ID
		return fmt.Sprintf("%x", envelope.MessageId)
	}
}

// isACKMessage 检查是否是ACK消息
func (crm *CenterReliabilityManager) isACKMessage(envelope *message.MessageEnvelope) bool {
	switch envelope.Message.(type) {
	case *message.MessageEnvelope_MessageFetchResponse,
		*message.MessageEnvelope_RoomMessageFetchResponse,
		*message.MessageEnvelope_HeartbeatAck:
		return true
	default:
		return false
	}
}

// cleanupLoop 清理循环
func (crm *CenterReliabilityManager) cleanupLoop() {
	defer crm.wg.Done()

	ticker := time.NewTicker(1 * time.Minute) // 每分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-crm.ctx.Done():
			return
		case <-ticker.C:
			crm.cleanup()
		}
	}
}

// Cleanup 执行清理
func (crm *CenterReliabilityManager) Cleanup() {
	crm.cleanup()
}

// cleanup 执行清理
func (crm *CenterReliabilityManager) cleanup() {
	now := time.Now()
	maxAge := crm.config.PendingMessageTTL

	crm.pendingMutex.Lock()
	defer crm.pendingMutex.Unlock()

	expiredKeys := make([]string, 0)
	for key, pending := range crm.pendingMessages {
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
		delete(crm.pendingMessages, key)
	}

	if len(expiredKeys) > 0 {
		log.Printf("Cleaned up %d expired center pending messages", len(expiredKeys))
	}
}

// GetPendingCount 获取待确认消息数量
func (crm *CenterReliabilityManager) GetPendingCount() int {
	crm.pendingMutex.RLock()
	defer crm.pendingMutex.RUnlock()
	return len(crm.pendingMessages)
}

// GetStats 获取统计信息
func (crm *CenterReliabilityManager) GetStats() map[string]interface{} {
	crm.pendingMutex.RLock()
	pendingCount := len(crm.pendingMessages)
	crm.pendingMutex.RUnlock()

	return map[string]interface{}{
		"total_retries":    atomic.LoadInt64(&crm.totalRetries),
		"total_fetches":    atomic.LoadInt64(&crm.totalFetches),
		"total_failed":     atomic.LoadInt64(&crm.totalFailed),
		"total_confirmed":  atomic.LoadInt64(&crm.totalConfirmed),
		"pending_messages": pendingCount,
	}
}
