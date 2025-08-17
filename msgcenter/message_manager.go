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

// MessageManager 消息管理器 - 负责消息持久化和处理
type MessageManager struct {
	server *MessageCenterServer
	config *MessageCenterConfig

	// 待处理消息队列
	messageQueue chan *PersistMessage
	batchQueue   []*PersistMessage
	batchMutex   sync.Mutex

	// 统计信息
	messagesQueued    int64
	messagesPersisted int64
	messagesDropped   int64
	batchCount        int64
	avgPersistTime    int64 // 纳秒

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// PersistMessage 待持久化消息
type PersistMessage struct {
	envelope     *message.MessageEnvelope
	routerConn   *RouterConnection
	receivedTime time.Time
	messageInfo  *MessageInfo
}

// MessageInfo 消息信息提取
type MessageInfo struct {
	MessageID   []byte
	UserID      uint32
	RoomID      uint32
	LoginID     uint8
	Timestamp   int64
	MessageType string
	Content     string
	Data        []byte
}

// NewMessageManager 创建消息管理器
func NewMessageManager(server *MessageCenterServer, config *MessageCenterConfig) *MessageManager {
	return &MessageManager{
		server:       server,
		config:       config,
		messageQueue: make(chan *PersistMessage, 10000), // 缓冲10000条消息
		batchQueue:   make([]*PersistMessage, 0, config.BatchInsertSize),
	}
}

// Start 启动消息管理器
func (mm *MessageManager) Start(ctx context.Context) {
	mm.ctx, mm.cancel = context.WithCancel(ctx)

	// 启动消息处理协程
	mm.wg.Add(1)
	go mm.messageProcessLoop()

	// 启动批量写入协程
	mm.wg.Add(1)
	go mm.batchProcessLoop()

	log.Printf("MessageManager started")
}

// Stop 停止消息管理器
func (mm *MessageManager) Stop() {
	if mm.cancel != nil {
		mm.cancel()
	}

	// 关闭消息队列
	close(mm.messageQueue)

	mm.wg.Wait()

	// 处理剩余的批量消息
	mm.flushBatch()

	log.Printf("MessageManager stopped")
}

// PersistMessage 持久化消息
func (mm *MessageManager) PersistMessage(routerConn *RouterConnection, envelope *message.MessageEnvelope) error {
	// 提取消息信息
	messageInfo, err := mm.extractMessageInfo(envelope)
	if err != nil {
		return fmt.Errorf("failed to extract message info: %v", err)
	}

	// 创建待持久化消息
	persistMsg := &PersistMessage{
		envelope:     envelope,
		routerConn:   routerConn,
		receivedTime: time.Now(),
		messageInfo:  messageInfo,
	}

	// 加入队列
	select {
	case mm.messageQueue <- persistMsg:
		atomic.AddInt64(&mm.messagesQueued, 1)
		return nil
	default:
		atomic.AddInt64(&mm.messagesDropped, 1)
		return fmt.Errorf("message queue full")
	}
}

// extractMessageInfo 提取消息信息
func (mm *MessageManager) extractMessageInfo(envelope *message.MessageEnvelope) (*MessageInfo, error) {
	// 提取基本信息
	userID, roomID, loginID, timestamp, err := mm.server.codec.ExtractMessageInfo(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to extract basic info: %v", err)
	}

	info := &MessageInfo{
		MessageID: envelope.MessageId,
		UserID:    userID,
		RoomID:    roomID,
		LoginID:   loginID,
		Timestamp: timestamp,
	}

	// 根据消息类型提取内容
	switch msg := envelope.Message.(type) {
	case *message.MessageEnvelope_ChatMessage:
		info.MessageType = "chat"
		info.Content = msg.ChatMessage.Content
		if data, err := mm.server.codec.Serialize(envelope); err == nil {
			info.Data = data
		}

	case *message.MessageEnvelope_CreateRoom:
		info.MessageType = "create_room"
		info.Content = fmt.Sprintf("Created room: %s", msg.CreateRoom.Name)
		if data, err := mm.server.codec.Serialize(envelope); err == nil {
			info.Data = data
		}

	case *message.MessageEnvelope_JoinRoom:
		info.MessageType = "join_room"
		info.Content = fmt.Sprintf("User %d joined room", userID)
		if data, err := mm.server.codec.Serialize(envelope); err == nil {
			info.Data = data
		}

	case *message.MessageEnvelope_LeaveRoom:
		info.MessageType = "leave_room"
		info.Content = fmt.Sprintf("User %d left room", userID)
		if data, err := mm.server.codec.Serialize(envelope); err == nil {
			info.Data = data
		}

	case *message.MessageEnvelope_CloseRoom:
		info.MessageType = "close_room"
		info.Content = fmt.Sprintf("Room %d closed", roomID)
		if data, err := mm.server.codec.Serialize(envelope); err == nil {
			info.Data = data
		}

	default:
		info.MessageType = "unknown"
		info.Content = fmt.Sprintf("Unknown message type: %T", envelope.Message)
		if data, err := mm.server.codec.Serialize(envelope); err == nil {
			info.Data = data
		}
	}

	return info, nil
}

// messageProcessLoop 消息处理循环
func (mm *MessageManager) messageProcessLoop() {
	defer mm.wg.Done()

	for {
		select {
		case <-mm.ctx.Done():
			return

		case persistMsg, ok := <-mm.messageQueue:
			if !ok {
				return // 队列已关闭
			}

			// 处理消息
			mm.processMessage(persistMsg)
		}
	}
}

// processMessage 处理单个消息
func (mm *MessageManager) processMessage(persistMsg *PersistMessage) {
	start := time.Now()

	// 同时发送到Kafka和数据库批量队列
	go mm.sendToKafka(persistMsg)
	mm.addToBatch(persistMsg)

	// 更新统计信息
	duration := time.Since(start)
	atomic.AddInt64(&mm.avgPersistTime, duration.Nanoseconds())
	atomic.AddInt64(&mm.server.stats.MessagesPersisted, 1)
}

// sendToKafka 发送到Kafka
func (mm *MessageManager) sendToKafka(persistMsg *PersistMessage) {
	if err := mm.server.kafkaManager.SendMessage(persistMsg); err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
		atomic.AddInt64(&mm.server.stats.KafkaErrors, 1)
	} else {
		atomic.AddInt64(&mm.server.stats.KafkaMessages, 1)
	}
}

// addToBatch 添加到批量队列
func (mm *MessageManager) addToBatch(persistMsg *PersistMessage) {
	mm.batchMutex.Lock()
	defer mm.batchMutex.Unlock()

	mm.batchQueue = append(mm.batchQueue, persistMsg)

	// 如果批量队列满了，触发写入
	if len(mm.batchQueue) >= mm.config.BatchInsertSize {
		mm.triggerBatchWrite()
	}
}

// batchProcessLoop 批量处理循环
func (mm *MessageManager) batchProcessLoop() {
	defer mm.wg.Done()

	ticker := time.NewTicker(mm.config.Kafka.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-mm.ctx.Done():
			return

		case <-ticker.C:
			// 定时触发批量写入
			mm.batchMutex.Lock()
			if len(mm.batchQueue) > 0 {
				mm.triggerBatchWrite()
			}
			mm.batchMutex.Unlock()
		}
	}
}

// triggerBatchWrite 触发批量写入（需要持有batchMutex）
func (mm *MessageManager) triggerBatchWrite() {
	if len(mm.batchQueue) == 0 {
		return
	}

	// 复制当前批量队列
	batch := make([]*PersistMessage, len(mm.batchQueue))
	copy(batch, mm.batchQueue)

	// 清空队列
	mm.batchQueue = mm.batchQueue[:0]

	// 异步写入数据库
	go mm.batchWriteToDatabase(batch)
}

// batchWriteToDatabase 批量写入数据库
func (mm *MessageManager) batchWriteToDatabase(batch []*PersistMessage) {
	start := time.Now()

	if err := mm.server.databaseManager.BatchInsertMessages(batch); err != nil {
		log.Printf("Failed to batch insert messages: %v", err)
		atomic.AddInt64(&mm.messagesDropped, int64(len(batch)))
	} else {
		atomic.AddInt64(&mm.batchCount, 1)
		atomic.AddInt64(&mm.server.stats.DatabaseWrites, int64(len(batch)))
		log.Printf("Batch inserted %d messages in %v", len(batch), time.Since(start))
	}
}

// flushBatch 刷新批量队列
func (mm *MessageManager) flushBatch() {
	mm.batchMutex.Lock()
	defer mm.batchMutex.Unlock()

	if len(mm.batchQueue) > 0 {
		mm.batchWriteToDatabase(mm.batchQueue)
		mm.batchQueue = mm.batchQueue[:0]
	}
}

// GetPendingCount 获取待处理消息数量
func (mm *MessageManager) GetPendingCount() int {
	return len(mm.messageQueue)
}

// GetBatchCount 获取批量队列数量
func (mm *MessageManager) GetBatchCount() int {
	mm.batchMutex.Lock()
	defer mm.batchMutex.Unlock()
	return len(mm.batchQueue)
}

// GetStats 获取统计信息
func (mm *MessageManager) GetStats() map[string]interface{} {
	avgPersistTime := atomic.LoadInt64(&mm.avgPersistTime)
	persisted := atomic.LoadInt64(&mm.messagesPersisted)
	if persisted > 0 {
		avgPersistTime = avgPersistTime / persisted
	}

	return map[string]interface{}{
		"messages_queued":     atomic.LoadInt64(&mm.messagesQueued),
		"messages_persisted":  atomic.LoadInt64(&mm.messagesPersisted),
		"messages_dropped":    atomic.LoadInt64(&mm.messagesDropped),
		"batch_count":         atomic.LoadInt64(&mm.batchCount),
		"pending_count":       mm.GetPendingCount(),
		"batch_queue_count":   mm.GetBatchCount(),
		"avg_persist_time_ns": avgPersistTime,
	}
}
