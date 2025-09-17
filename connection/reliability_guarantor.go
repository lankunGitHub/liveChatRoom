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

// ReliabilityGuarantor 可靠性保证器 - 连接层的统一可靠性策略实现
// 实现：发送消息 -> 等待ACK -> 重传 -> 切换路由节点 -> fetch确认
type ReliabilityGuarantor struct {
	server *ConnectionServer
	config *ConnectionConfig

	// 待确认消息
	pendingMessages map[string]*PendingServerMessage
	pendingMutex    sync.RWMutex

	// 房间内待确认消息队列（FIFO）
	// ACK只带room_id，不携带原始消息ID，
	// 消息顺序发送、顺序确认，ACK到达时确认队首消息
	roomPendingQueues map[uint32][]string
	queueMutex        sync.RWMutex

	// 统计信息
	totalRetries int64
	totalFetches int64
	totalFailed  int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// PendingServerMessage 服务端待确认消息
type PendingServerMessage struct {
	envelope    *message.MessageEnvelope
	data        []byte
	targetConn  *ClientConnection
	sentTime    time.Time
	retries     int
	timer       *time.Timer
	messageType string // "client" 或 "router"
	mutex       sync.Mutex
}

// NewReliabilityGuarantor 创建可靠性保证器
func NewReliabilityGuarantor(server *ConnectionServer, config *ConnectionConfig) *ReliabilityGuarantor {
	return &ReliabilityGuarantor{
		server:            server,
		config:            config,
		pendingMessages:   make(map[string]*PendingServerMessage),
		roomPendingQueues: make(map[uint32][]string),
	}
}

// Start 启动可靠性保证器
func (rg *ReliabilityGuarantor) Start(ctx context.Context) {
	rg.ctx, rg.cancel = context.WithCancel(ctx)

	// 启动清理协程
	rg.wg.Add(1)
	go rg.cleanupLoop()

	log.Printf("ReliabilityGuarantor started")
}

// Stop 停止可靠性保证器
func (rg *ReliabilityGuarantor) Stop() {
	if rg.cancel != nil {
		rg.cancel()
	}
	rg.wg.Wait()

	log.Printf("ReliabilityGuarantor stopped")
}

// SendReliableMessageToClient 向客户端发送可靠消息
func (rg *ReliabilityGuarantor) SendReliableMessageToClient(conn *ClientConnection, envelope *message.MessageEnvelope) error {
	if !rg.config.EnableReliability {
		// 可靠性未启用，直接发送
		return rg.server.SendMessageToClient(conn, envelope)
	}

	// 序列化消息
	data, err := rg.server.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 生成消息键
	messageKey := rg.getMessageKey(envelope)

	// 创建待确认消息
	pending := &PendingServerMessage{
		envelope:    envelope,
		data:        data,
		targetConn:  conn,
		sentTime:    time.Now(),
		retries:     0,
		messageType: "client",
	}

	// 添加到待确认列表
	rg.pendingMutex.Lock()
	rg.pendingMessages[messageKey] = pending
	rg.pendingMutex.Unlock()

	// 房间级消息加入FIFO队列，用于ACK匹配
	rg.enqueueRoomPending(envelope, messageKey)

	// 发送消息
	if err := conn.SendSync(data); err != nil {
		rg.removePendingMessage(messageKey)
		return fmt.Errorf("failed to send message: %v", err)
	}

	// 设置超时重传定时器
	pending.mutex.Lock()
	pending.timer = time.AfterFunc(rg.config.MessageTimeout, func() {
		rg.handleMessageTimeout(messageKey)
	})
	pending.mutex.Unlock()

	return nil
}

// SendReliableMessageToRouter 向路由节点发送可靠消息
func (rg *ReliabilityGuarantor) SendReliableMessageToRouter(envelope *message.MessageEnvelope) error {
	if !rg.config.EnableReliability {
		// 可靠性未启用，直接发送
		return rg.server.routerManager.SendMessageToRouterSync(envelope)
	}

	// 序列化消息
	data, err := rg.server.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 生成消息键
	messageKey := rg.getMessageKey(envelope)

	// 创建待确认消息
	pending := &PendingServerMessage{
		envelope:    envelope,
		data:        data,
		targetConn:  nil, // 路由消息
		sentTime:    time.Now(),
		retries:     0,
		messageType: "router",
	}

	// 添加到待确认列表
	rg.pendingMutex.Lock()
	rg.pendingMessages[messageKey] = pending
	rg.pendingMutex.Unlock()

	// 房间级消息加入FIFO队列，用于ACK匹配
	rg.enqueueRoomPending(envelope, messageKey)

	// 发送消息
	if err := rg.server.routerManager.SendMessageToRouterSync(envelope); err != nil {
		rg.removePendingMessage(messageKey)
		return fmt.Errorf("failed to send message to router: %v", err)
	}

	// 设置超时重传定时器
	pending.mutex.Lock()
	pending.timer = time.AfterFunc(rg.config.MessageTimeout, func() {
		rg.handleMessageTimeout(messageKey)
	})
	pending.mutex.Unlock()

	return nil
}

// HandleACK 处理ACK确认消息
func (rg *ReliabilityGuarantor) HandleACK(envelope *message.MessageEnvelope) bool {
	if !rg.isACKMessage(envelope) {
		return false
	}

	// 提取原始消息键
	originalMessageKey := rg.getOriginalMessageKey(envelope)
	if originalMessageKey == "" {
		return true // 无法识别的ACK，但仍视为已处理
	}

	// 移除待确认消息
	pending := rg.removePendingMessage(originalMessageKey)
	if pending != nil {
		// 停止重传定时器
		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		log.Printf("Message confirmed: %s", originalMessageKey)
	}

	return true
}

// handleMessageTimeout 处理消息超时
func (rg *ReliabilityGuarantor) handleMessageTimeout(messageKey string) {
	rg.pendingMutex.RLock()
	pending, exists := rg.pendingMessages[messageKey]
	rg.pendingMutex.RUnlock()

	if !exists {
		return // 消息已被处理
	}

	pending.mutex.Lock()
	defer pending.mutex.Unlock()

	pending.retries++
	atomic.AddInt64(&rg.totalRetries, 1)

	if pending.retries <= rg.config.MaxRetries {
		// 重传消息
		log.Printf("Retrying message %s (attempt %d/%d)", messageKey, pending.retries, rg.config.MaxRetries)

		var err error
		if pending.messageType == "client" && pending.targetConn != nil {
			err = pending.targetConn.SendSync(pending.data)
		} else if pending.messageType == "router" {
			err = rg.server.routerManager.SendMessageToRouterSync(pending.envelope)
		}

		if err != nil {
			log.Printf("Retry failed for message %s: %v", messageKey, err)
		}

		// 重新设置定时器
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.timer = time.AfterFunc(rg.config.MessageTimeout, func() {
			rg.handleMessageTimeout(messageKey)
		})
	} else {
		// 重传次数用尽，尝试fetch确认
		log.Printf("Max retries reached for message %s, attempting fetch", messageKey)
		rg.attemptFetchConfirmation(messageKey, pending)
	}
}

// attemptFetchConfirmation 尝试通过fetch确认消息状态
func (rg *ReliabilityGuarantor) attemptFetchConfirmation(messageKey string, pending *PendingServerMessage) {
	atomic.AddInt64(&rg.totalFetches, 1)

	// 创建fetch请求
	fetchMsg := &message.MessageFetchRequest{
		MessageId: pending.envelope.MessageId,
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	// 创建fetch消息信封
	fetchEnvelope, err := rg.server.codec.CreateEnvelope(
		0, // 系统查询
		0, // 无特定房间
		0, // 系统登录ID
		rg.server.connSeqGenerator.Next(),
		fetchMsg,
	)
	if err != nil {
		rg.handleMessageFailure(messageKey, fmt.Errorf("failed to create fetch envelope: %v", err))
		return
	}

	// 根据消息类型选择fetch目标
	if pending.messageType == "router" {
		// 路由消息，向路由节点fetch
		if err := rg.server.routerManager.SendMessageToRouterSync(fetchEnvelope); err != nil {
			rg.handleMessageFailure(messageKey, fmt.Errorf("failed to send fetch to router: %v", err))
			return
		}
	} else {
		// 客户端消息，向消息中心fetch（通过路由节点）
		if err := rg.server.routerManager.SendMessageToRouterSync(fetchEnvelope); err != nil {
			rg.handleMessageFailure(messageKey, fmt.Errorf("failed to send fetch request: %v", err))
			return
		}
	}

	// 设置fetch超时
	pending.timer = time.AfterFunc(rg.config.FetchTimeout, func() {
		rg.handleFetchTimeout(messageKey)
	})

	log.Printf("Fetch request sent for message %s", messageKey)
}

// handleFetchTimeout fetch超时处理
func (rg *ReliabilityGuarantor) handleFetchTimeout(messageKey string) {
	rg.handleMessageFailure(messageKey, fmt.Errorf("fetch timeout"))
}

// HandleFetchResponse 处理fetch响应
func (rg *ReliabilityGuarantor) HandleFetchResponse(envelope *message.MessageEnvelope) {
	switch msg := envelope.Message.(type) {
	case *message.MessageEnvelope_MessageFetchResponse:
		rg.handleMessageFetchResponse(msg.MessageFetchResponse)
	default:
		// 非fetch响应消息
	}
}

// handleMessageFetchResponse 处理消息fetch响应
func (rg *ReliabilityGuarantor) handleMessageFetchResponse(response *message.MessageFetchResponse) {
	messageKey := fmt.Sprintf("%x", response.MessageId)

	pending := rg.removePendingMessage(messageKey)
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
		log.Printf("Message confirmed via fetch: %s", messageKey)
	} else {
		// 消息不存在或失败，说明发送失败
		rg.handleMessageFailure(messageKey, fmt.Errorf("message not found or failed on server"))
	}
}

// handleMessageFailure 处理消息失败
func (rg *ReliabilityGuarantor) handleMessageFailure(messageKey string, err error) {
	pending := rg.removePendingMessage(messageKey)
	if pending != nil {
		// 停止定时器
		pending.mutex.Lock()
		if pending.timer != nil {
			pending.timer.Stop()
		}
		pending.mutex.Unlock()

		atomic.AddInt64(&rg.totalFailed, 1)
		log.Printf("Message failed: %s, error: %v", messageKey, err)
	}
}

// removePendingMessage 移除待确认消息
func (rg *ReliabilityGuarantor) removePendingMessage(messageKey string) *PendingServerMessage {
	rg.pendingMutex.Lock()
	pending, exists := rg.pendingMessages[messageKey]
	if exists {
		delete(rg.pendingMessages, messageKey)
	}
	rg.pendingMutex.Unlock()

	if pending != nil {
		// 同时从房间FIFO队列中移除
		if roomID, ok := rg.getEnvelopeRoom(pending.envelope); ok {
			rg.queueMutex.Lock()
			queue := rg.roomPendingQueues[roomID]
			for i, key := range queue {
				if key == messageKey {
					queue = append(queue[:i], queue[i+1:]...)
					break
				}
			}
			if len(queue) == 0 {
				delete(rg.roomPendingQueues, roomID)
			} else {
				rg.roomPendingQueues[roomID] = queue
			}
			rg.queueMutex.Unlock()
		}
	}

	return pending
}

// getMessageKey 获取消息键
func (rg *ReliabilityGuarantor) getMessageKey(envelope *message.MessageEnvelope) string {
	return fmt.Sprintf("%x", envelope.MessageId)
}

// getEnvelopeRoom 获取消息所属房间ID（房间级消息返回roomID，其他返回false）
func (rg *ReliabilityGuarantor) getEnvelopeRoom(envelope *message.MessageEnvelope) (uint32, bool) {
	switch msg := envelope.Message.(type) {
	case *message.MessageEnvelope_CreateRoom:
		return uint32(msg.CreateRoom.RoomId), true
	case *message.MessageEnvelope_JoinRoom, *message.MessageEnvelope_LeaveRoom, *message.MessageEnvelope_ChatMessage:
		// 这类消息的房间ID在MessageId中
		if globalID, err := message.FromBytes(envelope.MessageId); err == nil {
			return globalID.ExtractRoomID(), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// enqueueRoomPending 将房间级消息加入FIFO队列
func (rg *ReliabilityGuarantor) enqueueRoomPending(envelope *message.MessageEnvelope, messageKey string) {
	roomID, ok := rg.getEnvelopeRoom(envelope)
	if !ok {
		return
	}

	rg.queueMutex.Lock()
	rg.roomPendingQueues[roomID] = append(rg.roomPendingQueues[roomID], messageKey)
	rg.queueMutex.Unlock()
}

// getOriginalMessageKey 从ACK消息中提取原始消息键
func (rg *ReliabilityGuarantor) getOriginalMessageKey(envelope *message.MessageEnvelope) string {
	// 房间级ACK只带room_id，不携带原始消息ID
	// 由于消息是顺序发送、顺序确认的，使用房间FIFO队列队首作为被确认的原始消息
	var roomID uint32
	switch ack := envelope.Message.(type) {
	case *message.MessageEnvelope_CreateRoomAck:
		roomID = uint32(ack.CreateRoomAck.RoomId)
	case *message.MessageEnvelope_JoinRoomAck:
		roomID = uint32(ack.JoinRoomAck.RoomId)
	case *message.MessageEnvelope_LeaveRoomAck:
		roomID = uint32(ack.LeaveRoomAck.RoomId)
	case *message.MessageEnvelope_CloseRoomAck:
		roomID = uint32(ack.CloseRoomAck.RoomId)
	case *message.MessageEnvelope_ChatMessageAck:
		roomID = uint32(ack.ChatMessageAck.RoomId)
	case *message.MessageEnvelope_HeartbeatAck:
		// 心跳ACK，可以忽略
		return ""
	default:
		// 其他ACK类型，尝试使用消息ID
		return fmt.Sprintf("%x", envelope.MessageId)
	}

	rg.queueMutex.Lock()
	defer rg.queueMutex.Unlock()

	queue := rg.roomPendingQueues[roomID]
	if len(queue) == 0 {
		// 队列为空，可能是重传后才收到的重复ACK
		return ""
	}
	return queue[0]
}

// isACKMessage 检查是否是ACK消息
func (rg *ReliabilityGuarantor) isACKMessage(envelope *message.MessageEnvelope) bool {
	switch envelope.Message.(type) {
	case *message.MessageEnvelope_AllocateRoomIdAck,
		*message.MessageEnvelope_CreateRoomAck,
		*message.MessageEnvelope_JoinRoomAck,
		*message.MessageEnvelope_LeaveRoomAck,
		*message.MessageEnvelope_CloseRoomAck,
		*message.MessageEnvelope_ChatMessageAck,
		*message.MessageEnvelope_MessageFetchResponse,
		*message.MessageEnvelope_HeartbeatAck:
		return true
	default:
		// RoomMessageFetchResponse是历史消息数据，不是确认，交给消息处理器
		return false
	}
}

// cleanupLoop 清理循环
func (rg *ReliabilityGuarantor) cleanupLoop() {
	defer rg.wg.Done()

	ticker := time.NewTicker(1 * time.Minute) // 每分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-rg.ctx.Done():
			return
		case <-ticker.C:
			rg.cleanup()
		}
	}
}

// Cleanup 清理过期的待确认消息
func (rg *ReliabilityGuarantor) Cleanup() {
	rg.cleanup()
}

// cleanup 执行清理
func (rg *ReliabilityGuarantor) cleanup() {
	now := time.Now()
	maxAge := rg.config.PendingMessageTTL

	// 收集过期key后统一经removePendingMessage移除，
	// 保证房间FIFO队列同步清理，避免残留key造成错位确认
	rg.pendingMutex.RLock()
	expiredKeys := make([]string, 0)
	for key, pending := range rg.pendingMessages {
		if now.Sub(pending.sentTime) > maxAge {
			expiredKeys = append(expiredKeys, key)
		}
	}
	rg.pendingMutex.RUnlock()

	for _, key := range expiredKeys {
		pending := rg.removePendingMessage(key)
		if pending != nil {
			pending.mutex.Lock()
			if pending.timer != nil {
				pending.timer.Stop()
			}
			pending.mutex.Unlock()
		}
	}

	if len(expiredKeys) > 0 {
		log.Printf("Cleaned up %d expired pending messages", len(expiredKeys))
	}
}

// GetStats 获取统计信息
func (rg *ReliabilityGuarantor) GetStats() map[string]interface{} {
	rg.pendingMutex.RLock()
	pendingCount := len(rg.pendingMessages)
	rg.pendingMutex.RUnlock()

	return map[string]interface{}{
		"total_retries":    atomic.LoadInt64(&rg.totalRetries),
		"total_fetches":    atomic.LoadInt64(&rg.totalFetches),
		"total_failed":     atomic.LoadInt64(&rg.totalFailed),
		"pending_messages": pendingCount,
	}
}
