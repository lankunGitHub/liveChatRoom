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

	// 房间内待确认消息队列（FIFO）
	// 服务器返回的ACK只带room_id，不携带原始消息ID，
	// 因此按房间维护发送顺序，ACK到达时确认队首消息
	roomPendingQueues map[uint32][]string
	// 房间ID分配请求队列（AllocateRoomIdAck不携带任何关联信息，只能FIFO确认）
	allocateQueue []string
	// 房间历史拉取请求（RoomMessageFetchResponse按room匹配确认）
	roomFetchPending map[uint32]string
	queueMutex       sync.RWMutex

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
		client:            client,
		config:            config,
		pendingMessages:   make(map[string]*PendingMessage),
		roomPendingQueues: make(map[uint32][]string),
		roomFetchPending:  make(map[uint32]string),
	}
}

// Start 启动可靠性管理器
func (rm *ReliabilityManager) Start(ctx context.Context) {
	go rm.cleanupLoop(ctx)
}

// SendMessage 发送消息（带可靠性保证）
func (rm *ReliabilityManager) SendMessage(envelope *message.MessageEnvelope, data []byte) error {
	// 心跳不进入可靠性追踪：心跳是周期性的，
	// 丢一个心跳还有下一个，走重传反而制造大量噪音
	if _, ok := envelope.Message.(*message.MessageEnvelope_Heartbeat); ok {
		if err := rm.sendToActiveConnection(data); err != nil {
			return fmt.Errorf("failed to send heartbeat: %v", err)
		}
		return nil
	}

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

	// 按消息类型加入对应的匹配队列
	rm.enqueueForACKMatching(envelope, messageKey)

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

// enqueueForACKMatching 按消息类型加入对应的ACK匹配队列
func (rm *ReliabilityManager) enqueueForACKMatching(envelope *message.MessageEnvelope, messageKey string) {
	switch envelope.Message.(type) {
	case *message.MessageEnvelope_AllocateRoomId:
		rm.queueMutex.Lock()
		rm.allocateQueue = append(rm.allocateQueue, messageKey)
		rm.queueMutex.Unlock()

	case *message.MessageEnvelope_RoomMessageFetchRequest:
		// 拉取请求按房间记录（同一房间的多次拉取，后者覆盖前者，
		// 拉取是可重放的查询，最坏情况只是多一次拉取）
		if globalID, err := message.FromBytes(envelope.MessageId); err == nil {
			rm.queueMutex.Lock()
			rm.roomFetchPending[globalID.ExtractRoomID()] = messageKey
			rm.queueMutex.Unlock()
		}

	default:
		// 房间级消息加入FIFO队列
		if roomID, ok := rm.getEnvelopeRoom(envelope); ok {
			rm.queueMutex.Lock()
			rm.roomPendingQueues[roomID] = append(rm.roomPendingQueues[roomID], messageKey)
			rm.queueMutex.Unlock()
		}
	}
}

// HandleACK 处理ACK确认消息
func (rm *ReliabilityManager) HandleACK(envelope *message.MessageEnvelope) bool {
	// 检查是否是ACK消息
	if !rm.isACKMessage(envelope) {
		return false
	}

	// fetch响应按原消息ID精确匹配，单独处理
	if fetchResp, ok := envelope.Message.(*message.MessageEnvelope_MessageFetchResponse); ok {
		rm.handleMessageFetchResponse(fetchResp.MessageFetchResponse)
		return true
	}

	// 房间历史拉取响应：确认该房间的拉取请求pending
	if roomResp, ok := envelope.Message.(*message.MessageEnvelope_RoomMessageFetchResponse); ok {
		rm.handleRoomFetchResponse(roomResp.RoomMessageFetchResponse)
		return true
	}

	// 提取原始消息key
	originalMessageKey := rm.getOriginalMessageKey(envelope)
	if originalMessageKey == "" {
		return true // 无法识别的ACK，但仍视为已处理
	}

	// 移除待确认消息
	pending := rm.removePendingMessage(originalMessageKey)
	if pending != nil {
		rm.confirmMessage(pending, originalMessageKey)
	}

	return true
}

// handleRoomFetchResponse 处理房间历史拉取响应
// 响应到达即证明服务器已处理请求，确认该房间的拉取pending
func (rm *ReliabilityManager) handleRoomFetchResponse(response *message.RoomMessageFetchResponse) {
	rm.queueMutex.Lock()
	messageKey := rm.roomFetchPending[uint32(response.RoomId)]
	delete(rm.roomFetchPending, uint32(response.RoomId))
	rm.queueMutex.Unlock()

	if messageKey == "" {
		return // 无待确认的拉取请求（可能已超时清理）
	}

	pending := rm.removePendingMessage(messageKey)
	if pending != nil {
		rm.confirmMessage(pending, messageKey)
	}
}

// confirmMessage 确认单条消息送达
func (rm *ReliabilityManager) confirmMessage(pending *PendingMessage, messageKey string) {
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

	log.Printf("Message confirmed: %s", messageKey)
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
	atomic.AddInt64(&rm.client.stats.TotalRetries, 1)

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
	atomic.AddInt64(&rm.client.stats.TotalFetches, 1)

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
		rm.confirmMessage(pending, messageKey)
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

// getEnvelopeRoom 获取消息所属房间ID（房间级消息返回roomID，其他返回false）
func (rm *ReliabilityManager) getEnvelopeRoom(envelope *message.MessageEnvelope) (uint32, bool) {
	switch msg := envelope.Message.(type) {
	case *message.MessageEnvelope_CreateRoom:
		return uint32(msg.CreateRoom.RoomId), true
	case *message.MessageEnvelope_JoinRoom, *message.MessageEnvelope_LeaveRoom,
		*message.MessageEnvelope_CloseRoom, *message.MessageEnvelope_ChatMessage:
		// 这些消息本身不带房间ID（或发送时已写入客户端roomID），
		// 房间ID从MessageId中提取
		if globalID, err := message.FromBytes(envelope.MessageId); err == nil {
			return globalID.ExtractRoomID(), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// getOriginalMessageKey 从ACK消息中提取原始消息键
func (rm *ReliabilityManager) getOriginalMessageKey(envelope *message.MessageEnvelope) string {
	// 房间ID分配ACK不携带任何关联信息，只能FIFO确认
	if _, ok := envelope.Message.(*message.MessageEnvelope_AllocateRoomIdAck); ok {
		rm.queueMutex.Lock()
		defer rm.queueMutex.Unlock()
		if len(rm.allocateQueue) == 0 {
			return ""
		}
		key := rm.allocateQueue[0]
		rm.allocateQueue = rm.allocateQueue[1:]
		return key
	}

	// 房间级ACK只带room_id，不携带原始消息ID
	// 由于同一连接上的消息是顺序发送、顺序确认的，
	// 使用房间FIFO队列队首作为被确认的原始消息
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
		// 心跳不进入可靠性追踪，忽略
		return ""
	default:
		// 其他ACK类型，尝试使用消息ID
		return fmt.Sprintf("%x", envelope.MessageId)
	}

	rm.queueMutex.Lock()
	defer rm.queueMutex.Unlock()

	queue := rm.roomPendingQueues[roomID]
	if len(queue) == 0 {
		// 队列为空，可能是重传后才收到的重复ACK
		return ""
	}
	return queue[0]
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
	pending, exists := rm.pendingMessages[messageKey]
	if exists {
		delete(rm.pendingMessages, messageKey)
	}
	rm.pendingMutex.Unlock()

	if pending == nil {
		return nil
	}

	// 从各匹配队列中清理该key，防止残留造成错位确认
	switch pending.envelope.Message.(type) {
	case *message.MessageEnvelope_AllocateRoomId:
		rm.queueMutex.Lock()
		for i, key := range rm.allocateQueue {
			if key == messageKey {
				rm.allocateQueue = append(rm.allocateQueue[:i], rm.allocateQueue[i+1:]...)
				break
			}
		}
		rm.queueMutex.Unlock()

	case *message.MessageEnvelope_RoomMessageFetchRequest:
		if globalID, err := message.FromBytes(pending.envelope.MessageId); err == nil {
			rm.queueMutex.Lock()
			if rm.roomFetchPending[globalID.ExtractRoomID()] == messageKey {
				delete(rm.roomFetchPending, globalID.ExtractRoomID())
			}
			rm.queueMutex.Unlock()
		}

	default:
		if roomID, ok := rm.getEnvelopeRoom(pending.envelope); ok {
			rm.queueMutex.Lock()
			queue := rm.roomPendingQueues[roomID]
			for i, key := range queue {
				if key == messageKey {
					queue = append(queue[:i], queue[i+1:]...)
					break
				}
			}
			if len(queue) == 0 {
				delete(rm.roomPendingQueues, roomID)
			} else {
				rm.roomPendingQueues[roomID] = queue
			}
			rm.queueMutex.Unlock()
		}
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

	// 收集过期key后统一经removePendingMessage移除，
	// 保证各匹配队列同步清理，避免残留key造成错位确认
	rm.pendingMutex.RLock()
	var expired []string
	for key, pending := range rm.pendingMessages {
		if now.Sub(pending.sentTime) > maxAge {
			expired = append(expired, key)
		}
	}
	rm.pendingMutex.RUnlock()

	for _, key := range expired {
		rm.removePendingMessage(key)
		log.Printf("Cleaned up expired pending message: %s", key)
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
