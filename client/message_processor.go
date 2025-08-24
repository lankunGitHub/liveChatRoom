package client

import (
	"context"
	"liveChatroom/message"
	"log"
	"sort"
	"sync"
	"time"
)

// MessageProcessor 消息处理器 - 负责消息排序、去重和有序处理
type MessageProcessor struct {
	// 消息缓冲区
	messageBuffer []*message.MessageEnvelope
	bufferMutex   sync.RWMutex
	maxBufferSize int

	// 消息去重
	seenMessages map[string]bool
	seenMutex    sync.RWMutex

	// 顺序处理
	lastProcessedSeq uint64
	expectedNextSeq  uint64
	sequenceMutex    sync.RWMutex

	// 控制通道
	processChan chan *message.MessageEnvelope
	ctx         context.Context
}

// NewMessageProcessor 创建消息处理器
func NewMessageProcessor(bufferSize int) *MessageProcessor {
	return &MessageProcessor{
		messageBuffer: make([]*message.MessageEnvelope, 0, bufferSize),
		maxBufferSize: bufferSize,
		seenMessages:  make(map[string]bool),
		processChan:   make(chan *message.MessageEnvelope, bufferSize),
	}
}

// Start 启动消息处理器
func (mp *MessageProcessor) Start(ctx context.Context) {
	mp.ctx = ctx
	go mp.processLoop()
	go mp.cleanupLoop()
}

// ProcessMessage 处理接收到的消息
func (mp *MessageProcessor) ProcessMessage(envelope *message.MessageEnvelope) {
	// 检查消息重复
	if mp.isDuplicateMessage(envelope) {
		log.Printf("Duplicate message ignored: %x", envelope.MessageId)
		return
	}

	// 标记消息已见
	mp.markMessageSeen(envelope)

	// 发送到处理通道
	select {
	case mp.processChan <- envelope:
		// 成功发送到处理通道
	case <-mp.ctx.Done():
		// 上下文已取消
		return
	default:
		// 通道满了，直接处理
		mp.handleMessage(envelope)
	}
}

// processLoop 消息处理循环
func (mp *MessageProcessor) processLoop() {
	ticker := time.NewTicker(100 * time.Millisecond) // 每100ms检查一次缓冲区
	defer ticker.Stop()

	for {
		select {
		case <-mp.ctx.Done():
			return
		case envelope := <-mp.processChan:
			mp.addToBuffer(envelope)
		case <-ticker.C:
			mp.processBufferedMessages()
		}
	}
}

// addToBuffer 添加消息到缓冲区
func (mp *MessageProcessor) addToBuffer(envelope *message.MessageEnvelope) {
	mp.bufferMutex.Lock()
	defer mp.bufferMutex.Unlock()

	// 检查缓冲区大小
	if len(mp.messageBuffer) >= mp.maxBufferSize {
		// 缓冲区满了，移除最老的消息
		mp.messageBuffer = mp.messageBuffer[1:]
	}

	// 添加新消息
	mp.messageBuffer = append(mp.messageBuffer, envelope)

	// 按时间戳排序
	sort.Slice(mp.messageBuffer, func(i, j int) bool {
		id1, _ := message.FromBytes(mp.messageBuffer[i].MessageId)
		id2, _ := message.FromBytes(mp.messageBuffer[j].MessageId)
		return id1.Compare(id2) < 0
	})
}

// processBufferedMessages 处理缓冲区中的消息
func (mp *MessageProcessor) processBufferedMessages() {
	mp.bufferMutex.Lock()
	defer mp.bufferMutex.Unlock()

	now := time.Now().UnixMilli()
	processed := 0

	for i, envelope := range mp.messageBuffer {
		// 检查消息是否可以处理（时间窗口内）
		globalID, err := message.FromBytes(envelope.MessageId)
		if err != nil {
			continue
		}

		messageTime := globalID.ExtractTimestamp()

		// 如果消息太新（可能还有更早的消息在路上），等待一段时间
		if now-messageTime < 50 { // 50ms等待窗口
			break
		}

		// 处理消息
		mp.handleMessage(envelope)
		processed = i + 1
	}

	// 移除已处理的消息
	if processed > 0 {
		mp.messageBuffer = mp.messageBuffer[processed:]
	}
}

// handleMessage 处理单个消息
func (mp *MessageProcessor) handleMessage(envelope *message.MessageEnvelope) {
	// 根据消息类型进行处理
	switch msg := envelope.Message.(type) {
	case *message.MessageEnvelope_ChatMessage:
		mp.handleChatMessage(envelope, msg.ChatMessage)
	case *message.MessageEnvelope_JoinRoomAck:
		mp.handleJoinRoomAck(envelope, msg.JoinRoomAck)
	case *message.MessageEnvelope_LeaveRoomAck:
		mp.handleLeaveRoomAck(envelope, msg.LeaveRoomAck)
	case *message.MessageEnvelope_RoomMessageFetchResponse:
		mp.handleRoomMessageFetchResponse(envelope, msg.RoomMessageFetchResponse)
	default:
		// 其他消息类型的通用处理
		mp.handleGenericMessage(envelope)
	}
}

// handleChatMessage 处理聊天消息
func (mp *MessageProcessor) handleChatMessage(envelope *message.MessageEnvelope, chatMsg *message.ChatMessage) {
	// 提取消息信息
	globalID, err := message.FromBytes(envelope.MessageId)
	if err != nil {
		log.Printf("Invalid message ID in chat message: %v", err)
		return
	}

	userID := globalID.ExtractUserID()
	roomID := globalID.ExtractRoomID()
	timestamp := globalID.ExtractTimestamp()

	log.Printf("Chat message from user %d in room %d: %s (time: %d)",
		userID, roomID, chatMsg.Content, timestamp)

	// 这里可以添加具体的消息处理逻辑
	// 例如：更新UI、存储到本地数据库等
}

// handleJoinRoomAck 处理加入房间ACK
func (mp *MessageProcessor) handleJoinRoomAck(envelope *message.MessageEnvelope, ack *message.JoinRoomAck) {
	if ack.Status == message.JoinRoomStatus_JOIN_ROOM_SUCCESS {
		log.Printf("Successfully joined room %d", ack.RoomId)

		// 触发房间加入事件
		// 这里可以通知UI更新房间状态
	} else {
		log.Printf("Failed to join room %d: %s", ack.RoomId, ack.Reason)
	}
}

// handleLeaveRoomAck 处理离开房间ACK
func (mp *MessageProcessor) handleLeaveRoomAck(envelope *message.MessageEnvelope, ack *message.LeaveRoomAck) {
	if ack.Status == message.LeaveRoomStatus_LEAVE_ROOM_SUCCESS {
		log.Printf("Successfully left room %d", ack.RoomId)
	} else {
		log.Printf("Failed to leave room %d: %s", ack.RoomId, ack.Reason)
	}
}

// handleRoomMessageFetchResponse 处理房间消息拉取响应
func (mp *MessageProcessor) handleRoomMessageFetchResponse(envelope *message.MessageEnvelope, response *message.RoomMessageFetchResponse) {
	if response.Status == message.RoomMessageFetchStatus_ROOM_MESSAGE_FETCH_SUCCESS {
		log.Printf("Fetched %d messages for room %d", len(response.Messages), response.RoomId)

		// 处理拉取到的历史消息
		for _, msgContent := range response.Messages {
			mp.handleHistoryMessage(msgContent)
		}
	} else {
		log.Printf("Failed to fetch messages for room %d: %s", response.RoomId, response.Reason)
	}
}

// handleHistoryMessage 处理历史消息
func (mp *MessageProcessor) handleHistoryMessage(msgContent *message.MessageContent) {
	log.Printf("History message from user %d: %s (time: %d)",
		msgContent.UserId, msgContent.Content, msgContent.Timestamp)

	// 这里可以添加历史消息的处理逻辑
	// 例如：插入到本地消息列表、更新UI等
}

// handleGenericMessage 处理通用消息
func (mp *MessageProcessor) handleGenericMessage(envelope *message.MessageEnvelope) {
	// 提取基本信息
	globalID, err := message.FromBytes(envelope.MessageId)
	if err == nil {
		userID := globalID.ExtractUserID()
		roomID := globalID.ExtractRoomID()
		timestamp := globalID.ExtractTimestamp()

		log.Printf("Generic message from user %d in room %d (time: %d, type: %T)",
			userID, roomID, timestamp, envelope.Message)
	}
}

// isDuplicateMessage 检查消息是否重复
func (mp *MessageProcessor) isDuplicateMessage(envelope *message.MessageEnvelope) bool {
	messageKey := mp.getMessageKey(envelope)

	mp.seenMutex.RLock()
	defer mp.seenMutex.RUnlock()

	return mp.seenMessages[messageKey]
}

// markMessageSeen 标记消息已见
func (mp *MessageProcessor) markMessageSeen(envelope *message.MessageEnvelope) {
	messageKey := mp.getMessageKey(envelope)

	mp.seenMutex.Lock()
	defer mp.seenMutex.Unlock()

	mp.seenMessages[messageKey] = true
}

// getMessageKey 获取消息键
func (mp *MessageProcessor) getMessageKey(envelope *message.MessageEnvelope) string {
	// 使用消息ID和连接序列号组合作为键，确保唯一性
	return string(envelope.MessageId) + ":" + string(rune(envelope.ConnectionSeqId))
}

// cleanupLoop 清理循环
func (mp *MessageProcessor) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute) // 每5分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-mp.ctx.Done():
			return
		case <-ticker.C:
			mp.cleanupSeenMessages()
		}
	}
}

// cleanupSeenMessages 清理已见消息记录
func (mp *MessageProcessor) cleanupSeenMessages() {
	mp.seenMutex.Lock()
	defer mp.seenMutex.Unlock()

	// 如果已见消息过多，清理一半
	if len(mp.seenMessages) > 10000 {
		count := 0
		for key := range mp.seenMessages {
			delete(mp.seenMessages, key)
			count++
			if count >= len(mp.seenMessages)/2 {
				break
			}
		}
		log.Printf("Cleaned up %d seen messages", count)
	}
}

// GetStats 获取统计信息
func (mp *MessageProcessor) GetStats() (buffered, seen int) {
	mp.bufferMutex.RLock()
	buffered = len(mp.messageBuffer)
	mp.bufferMutex.RUnlock()

	mp.seenMutex.RLock()
	seen = len(mp.seenMessages)
	mp.seenMutex.RUnlock()

	return buffered, seen
}
