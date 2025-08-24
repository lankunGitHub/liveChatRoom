package client

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ReliabilityManager 消息可靠性管理器
// 实现统一的重传+fetch策略：发送消息 -> 等待ACK -> 重传 -> 切换节点并fetch确认
type ReliabilityManager struct {
	client *LiveChatClient
	config *ClientConfig

	// 待确认消息
	pendingMessages map[string]*PendingMessage
	pendingMutex    sync.RWMutex

	// 统计信息
	totalRetries int64
	totalFetches int64
}

// PendingMessage 待确认消息
type PendingMessage struct {
	envelope *message.MessageEnvelope
	data     []byte
	sentTime time.Time
	retries  int
	timer    *time.Timer
	mutex    sync.Mutex
}

// NewReliabilityManager 创建可靠性管理器
func NewReliabilityManager(client *LiveChatClient, config *ClientConfig) *ReliabilityManager {
	return &ReliabilityManager{
		client:          client,
		config:          config,
		pendingMessages: make(map[string]*PendingMessage),
	}
}

// Start 启动可靠性管理器
func (rm *ReliabilityManager) Start(ctx context.Context) {
	go rm.cleanupLoop(ctx)
}

// SendMessage 发送消息（带可靠性保证）
func (rm *ReliabilityManager) SendMessage(envelope *message.MessageEnvelope, data []byte) error {
	// 生成消息键
	messageKey := rm.getMessageKey(envelope)

	// 创建待确认消息
	pending := &PendingMessage{
		envelope: envelope,
		data:     data,
		sentTime: time.Now(),
		retries:  0,
	}

	// 添加到待确认列表
	rm.pendingMutex.Lock()
	rm.pendingMessages[messageKey] = pending
	rm.pendingMutex.Unlock()

	// 发送消息
	if err := rm.sendToActiveConnection(data); err != nil {
		// 发送失败，移除待确认消息
		rm.removePendingMessage(messageKey)
		return fmt.Errorf("failed to send message: %v", err)
	}

	// 设置超时重传定时器
	pending.mutex.Lock()
	pending.timer = time.AfterFunc(rm.config.MessageTimeout, func() {
		rm.handleMessageTimeout(messageKey)
	})
	pending.mutex.Unlock()

	// 更新统计
	atomic.AddInt64(&rm.client.stats.TotalMessagesSent, 1)

	// 触发事件
	if rm.client.eventHandler != nil {
		rm.client.eventHandler.OnMessageSent(envelope)
	}

	return nil
}

// HandleACK 处理ACK确认消息
func (rm *ReliabilityManager) HandleACK(envelope *message.MessageEnvelope) bool {
	// 检查是否是ACK消息
	if !rm.isACKMessage(envelope) {
		return false
	}

	// 提取原始消息ID
	originalMessageKey := rm.getOriginalMessageKey(envelope)
	if originalMessageKey == "" {
		return true // 无法识别的ACK，但仍视为已处理
	}

	// 移除待确认消息
	pending := rm.removePendingMessage(originalMessageKey)
	if pending != nil {
		// 停止重传定时器
		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		// 触发消息已送达事件
		if rm.client.eventHandler != nil {
			rm.client.eventHandler.OnMessageDelivered(pending.envelope)
		}

		log.Printf("Message confirmed: %s", originalMessageKey)
	}

	return true
}

// handleMessageTimeout 处理消息超时
func (rm *ReliabilityManager) handleMessageTimeout(messageKey string) {
	rm.pendingMutex.RLock()
	pending, exists := rm.pendingMessages[messageKey]
	rm.pendingMutex.RUnlock()

	if !exists {
		return // 消息已被处理
	}

	pending.mutex.Lock()
	defer pending.mutex.Unlock()

	pending.retries++
	atomic.AddInt64(&rm.totalRetries, 1)

	if pending.retries <= rm.config.MaxRetries {
		// 重传消息
		log.Printf("Retrying message %s (attempt %d/%d)", messageKey, pending.retries, rm.config.MaxRetries)

		if err := rm.sendToActiveConnection(pending.data); err != nil {
			log.Printf("Retry failed for message %s: %v", messageKey, err)
			// 尝试切换连接
			if err := rm.client.switchToNextAvailableConnection(); err != nil {
				log.Printf("Failed to switch connection: %v", err)
				rm.handleMessageFailure(messageKey, fmt.Errorf("all connections failed"))
				return
			}
		}

		// 重新设置定时器
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.timer = time.AfterFunc(rm.config.MessageTimeout, func() {
			rm.handleMessageTimeout(messageKey)
		})
	} else {
		// 重传次数用尽，尝试fetch确认
		log.Printf("Max retries reached for message %s, attempting fetch", messageKey)
		rm.attemptFetchConfirmation(messageKey, pending)
	}
}

// attemptFetchConfirmation 尝试通过fetch确认消息状态
func (rm *ReliabilityManager) attemptFetchConfirmation(messageKey string, pending *PendingMessage) {
	atomic.AddInt64(&rm.totalFetches, 1)

	// 尝试连接到其他节点进行fetch
	if err := rm.client.switchToNextAvailableConnection(); err != nil {
		rm.handleMessageFailure(messageKey, fmt.Errorf("no available connection for fetch: %v", err))
		return
	}

	// 发送fetch请求
	fetchMsg := &message.MessageFetchRequest{
		MessageId: pending.envelope.MessageId,
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	// 创建fetch消息信封
	fetchEnvelope, err := rm.client.codec.CreateEnvelope(
		rm.client.userID,
		rm.client.roomID,
		rm.client.loginID,
		rm.client.connSeqGenerator.Next(),
		fetchMsg,
	)
	if err != nil {
		rm.handleMessageFailure(messageKey, fmt.Errorf("failed to create fetch envelope: %v", err))
		return
	}

	// 序列化fetch消息
	fetchData, err := rm.client.codec.Serialize(fetchEnvelope)
	if err != nil {
		rm.handleMessageFailure(messageKey, fmt.Errorf("failed to serialize fetch message: %v", err))
		return
	}

	// 发送fetch请求（不进入可靠性管理，避免递归）
	if err := rm.sendToActiveConnection(fetchData); err != nil {
		rm.handleMessageFailure(messageKey, fmt.Errorf("failed to send fetch request: %v", err))
		return
	}

	// 设置fetch超时
	pending.timer = time.AfterFunc(rm.config.FetchTimeout, func() {
		rm.handleFetchTimeout(messageKey)
	})

	log.Printf("Fetch request sent for message %s", messageKey)
}

// handleFetchTimeout fetch超时处理
func (rm *ReliabilityManager) handleFetchTimeout(messageKey string) {
	// 最终失败
	rm.handleMessageFailure(messageKey, fmt.Errorf("fetch timeout"))
}

// handleMessageFailure 处理消息失败
func (rm *ReliabilityManager) handleMessageFailure(messageKey string, err error) {
	pending := rm.removePendingMessage(messageKey)
	if pending != nil {
		// 停止定时器
		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		// 触发失败事件
		if rm.client.eventHandler != nil {
			rm.client.eventHandler.OnMessageFailed(pending.envelope, err)
		}

		log.Printf("Message failed: %s, error: %v", messageKey, err)
	}
}

// HandleFetchResponse 处理fetch响应
func (rm *ReliabilityManager) HandleFetchResponse(envelope *message.MessageEnvelope) {
	switch msg := envelope.Message.(type) {
	case *message.MessageEnvelope_MessageFetchResponse:
		rm.handleMessageFetchResponse(msg.MessageFetchResponse)
	default:
		// 非fetch响应消息
	}
}

// handleMessageFetchResponse 处理消息fetch响应
func (rm *ReliabilityManager) handleMessageFetchResponse(response *message.MessageFetchResponse) {
	messageKey := fmt.Sprintf("%x", response.MessageId)

	rm.pendingMutex.RLock()
	pending, exists := rm.pendingMessages[messageKey]
	rm.pendingMutex.RUnlock()

	if !exists {
		return // 消息已被处理
	}

	if response.Exists {
		// 消息存在，说明发送成功
		rm.removePendingMessage(messageKey)

		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		if rm.client.eventHandler != nil {
			rm.client.eventHandler.OnMessageDelivered(pending.envelope)
		}

		log.Printf("Message confirmed via fetch: %s", messageKey)
	} else {
		// 消息不存在，说明发送失败
		rm.handleMessageFailure(messageKey, fmt.Errorf("message not found on server"))
	}
}

// sendToActiveConnection 发送数据到活跃连接
func (rm *ReliabilityManager) sendToActiveConnection(data []byte) error {
	node := rm.client.getActiveConnection()
	if node == nil {
		return fmt.Errorf("no active connection")
	}

	if node.connection == nil {
		return fmt.Errorf("connection not established")
	}

	// 使用连接对象发送数据
	_, err := node.connection.Write(data)
	return err
}

// getMessageKey 获取消息键
func (rm *ReliabilityManager) getMessageKey(envelope *message.MessageEnvelope) string {
	return fmt.Sprintf("%x", envelope.MessageId)
}

// getOriginalMessageKey 从ACK消息中提取原始消息键
func (rm *ReliabilityManager) getOriginalMessageKey(envelope *message.MessageEnvelope) string {
	// 根据不同的ACK类型提取原始消息ID
	switch ack := envelope.Message.(type) {
	case *message.MessageEnvelope_CreateRoomAck:
		// 对于房间ACK，使用房间ID作为键
		return fmt.Sprintf("room_%d", ack.CreateRoomAck.RoomId)
	case *message.MessageEnvelope_JoinRoomAck:
		return fmt.Sprintf("room_%d", ack.JoinRoomAck.RoomId)
	case *message.MessageEnvelope_ChatMessageAck:
		return fmt.Sprintf("room_%d", ack.ChatMessageAck.RoomId)
	case *message.MessageEnvelope_HeartbeatAck:
		// 心跳ACK，可以忽略或使用时间戳
		return ""
	default:
		// 其他ACK类型，尝试使用消息ID
		return fmt.Sprintf("%x", envelope.MessageId)
	}
}

// isACKMessage 检查是否是ACK消息
func (rm *ReliabilityManager) isACKMessage(envelope *message.MessageEnvelope) bool {
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

// removePendingMessage 移除待确认消息
func (rm *ReliabilityManager) removePendingMessage(messageKey string) *PendingMessage {
	rm.pendingMutex.Lock()
	defer rm.pendingMutex.Unlock()

	pending, exists := rm.pendingMessages[messageKey]
	if exists {
		delete(rm.pendingMessages, messageKey)
	}
	return pending
}

// cleanupLoop 清理循环，清理超时的待确认消息
func (rm *ReliabilityManager) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute) // 每分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rm.cleanup()
		}
	}
}

// cleanup 清理超时的待确认消息
func (rm *ReliabilityManager) cleanup() {
	now := time.Now()
	maxAge := 5 * time.Minute // 最大存活时间

	rm.pendingMutex.Lock()
	defer rm.pendingMutex.Unlock()

	for key, pending := range rm.pendingMessages {
		if now.Sub(pending.sentTime) > maxAge {
			pending.mutex.Lock()
			if pending.timer != nil {
				pending.timer.Stop()
			}
			pending.mutex.Unlock()

			delete(rm.pendingMessages, key)
			log.Printf("Cleaned up expired pending message: %s", key)
		}
	}
}

// GetStats 获取统计信息
func (rm *ReliabilityManager) GetStats() (retries, fetches int64, pending int) {
	rm.pendingMutex.RLock()
	pending = len(rm.pendingMessages)
	rm.pendingMutex.RUnlock()

	return atomic.LoadInt64(&rm.totalRetries),
		atomic.LoadInt64(&rm.totalFetches),
		pending
}
