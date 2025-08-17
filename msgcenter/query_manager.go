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

// QueryManager 查询管理器 - 负责处理消息查询请求
type QueryManager struct {
	server *MessageCenterServer
	config *MessageCenterConfig

	// 查询缓存
	messageCache map[string]*CachedMessage
	roomCache    map[uint64]*CachedRoomMessages
	cacheMutex   sync.RWMutex
	cacheExpiry  time.Duration

	// 统计信息
	queriesProcessed int64
	cacheHits        int64
	cacheMisses      int64
	avgQueryTime     int64 // 纳秒

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// CachedMessage 缓存的消息
type CachedMessage struct {
	record   *MessageRecord
	cachedAt time.Time
}

// CachedRoomMessages 缓存的房间消息
type CachedRoomMessages struct {
	messages  []*MessageRecord
	cachedAt  time.Time
	roomID    uint64
	startTime int64
	limit     int
}

// NewQueryManager 创建查询管理器
func NewQueryManager(server *MessageCenterServer, config *MessageCenterConfig) *QueryManager {
	return &QueryManager{
		server:       server,
		config:       config,
		messageCache: make(map[string]*CachedMessage),
		roomCache:    make(map[uint64]*CachedRoomMessages),
		cacheExpiry:  5 * time.Minute, // 缓存5分钟
	}
}

// Start 启动查询管理器
func (qm *QueryManager) Start(ctx context.Context) {
	qm.ctx, qm.cancel = context.WithCancel(ctx)

	// 启动缓存清理协程
	qm.wg.Add(1)
	go qm.cacheCleanupLoop()

	log.Printf("QueryManager started")
}

// Stop 停止查询管理器
func (qm *QueryManager) Stop() {
	if qm.cancel != nil {
		qm.cancel()
	}
	qm.wg.Wait()

	log.Printf("QueryManager stopped")
}

// HandleMessageFetchRequest 处理消息获取请求
func (qm *QueryManager) HandleMessageFetchRequest(routerConn *RouterConnection, envelope *message.MessageEnvelope, req *message.MessageFetchRequest) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		atomic.AddInt64(&qm.queriesProcessed, 1)
		atomic.AddInt64(&qm.avgQueryTime, duration.Nanoseconds())
		atomic.AddInt64(&qm.server.stats.MessagesQueried, 1)
	}()

	// 更新路由连接统计
	routerConn.IncrementQueriesProcessed()

	// 查询消息
	record, err := qm.queryMessage(req.MessageId)
	if err != nil {
		return qm.sendMessageFetchError(routerConn, envelope, fmt.Sprintf("Query failed: %v", err))
	}

	// 创建响应
	response := &message.MessageFetchResponse{
		MessageId: req.MessageId,
		Exists:    record != nil,
		Status:    message.MessageStatus_MESSAGE_STATUS_SUCCESS,
		Reason:    "Query completed",
	}

	if record != nil {
		response.MessageContent = qm.recordToMessageContent(record)
	}

	// 发送响应
	return qm.server.sendResponse(routerConn, envelope, response)
}

// HandleRoomMessageFetchRequest 处理房间消息获取请求
func (qm *QueryManager) HandleRoomMessageFetchRequest(routerConn *RouterConnection, envelope *message.MessageEnvelope, req *message.RoomMessageFetchRequest) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		atomic.AddInt64(&qm.queriesProcessed, 1)
		atomic.AddInt64(&qm.avgQueryTime, duration.Nanoseconds())
		atomic.AddInt64(&qm.server.stats.MessagesQueried, 1)
	}()

	// 更新路由连接统计
	routerConn.IncrementQueriesProcessed()

	// 设置默认参数
	limit := int(req.Limit)
	if limit <= 0 || limit > 1000 {
		limit = 100 // 默认100条，最多1000条
	}

	startTime := req.SinceTimestamp
	if startTime == 0 {
		startTime = uint64(time.Now().UnixMilli()) // 默认当前时间
	}

	// 查询房间消息
	records, err := qm.queryRoomMessages(req.RoomId, int64(startTime), limit)
	if err != nil {
		return qm.sendRoomMessageFetchError(routerConn, envelope, fmt.Sprintf("Query failed: %v", err))
	}

	// 创建响应
	response := &message.RoomMessageFetchResponse{
		RoomId:   req.RoomId,
		Messages: qm.recordsToMessageContents(records),
		Status:   message.RoomMessageFetchStatus_ROOM_MESSAGE_FETCH_SUCCESS,
		Reason:   fmt.Sprintf("Found %d messages", len(records)),
	}

	// 发送响应
	return qm.server.sendResponse(routerConn, envelope, response)
}

// queryMessage 查询单条消息（带缓存）
func (qm *QueryManager) queryMessage(messageID []byte) (*MessageRecord, error) {
	messageKey := fmt.Sprintf("%x", messageID)

	// 检查缓存
	qm.cacheMutex.RLock()
	if cached, exists := qm.messageCache[messageKey]; exists && time.Since(cached.cachedAt) < qm.cacheExpiry {
		qm.cacheMutex.RUnlock()
		atomic.AddInt64(&qm.cacheHits, 1)
		return cached.record, nil
	}
	qm.cacheMutex.RUnlock()

	// 缓存未命中，从数据库查询
	atomic.AddInt64(&qm.cacheMisses, 1)
	record, err := qm.server.databaseManager.QueryMessage(messageID)
	if err != nil {
		return nil, err
	}

	// 更新缓存
	if record != nil {
		qm.cacheMutex.Lock()
		qm.messageCache[messageKey] = &CachedMessage{
			record:   record,
			cachedAt: time.Now(),
		}
		qm.cacheMutex.Unlock()
	}

	return record, nil
}

// queryRoomMessages 查询房间消息（带缓存）
func (qm *QueryManager) queryRoomMessages(roomID uint64, startTime int64, limit int) ([]*MessageRecord, error) {
	// 检查缓存（精确匹配）
	qm.cacheMutex.RLock()
	if cached, exists := qm.roomCache[roomID]; exists &&
		cached.startTime == startTime &&
		cached.limit == limit &&
		time.Since(cached.cachedAt) < qm.cacheExpiry {
		qm.cacheMutex.RUnlock()
		atomic.AddInt64(&qm.cacheHits, 1)
		return cached.messages, nil
	}
	qm.cacheMutex.RUnlock()

	// 缓存未命中，从数据库查询
	atomic.AddInt64(&qm.cacheMisses, 1)
	records, err := qm.server.databaseManager.QueryRoomMessages(roomID, startTime, limit)
	if err != nil {
		return nil, err
	}

	// 更新缓存
	qm.cacheMutex.Lock()
	qm.roomCache[roomID] = &CachedRoomMessages{
		messages:  records,
		cachedAt:  time.Now(),
		roomID:    roomID,
		startTime: startTime,
		limit:     limit,
	}
	qm.cacheMutex.Unlock()

	return records, nil
}

// recordToMessageContent 将数据库记录转换为消息内容
func (qm *QueryManager) recordToMessageContent(record *MessageRecord) *message.MessageContent {
	// 创建消息内容
	content := &message.MessageContent{
		MessageId:   record.MessageID,
		UserId:      uint64(record.UserID),
		RoomId:      uint64(record.RoomID),
		Content:     record.Content,
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT, // 默认文本类型
		Timestamp:   uint64(record.Timestamp),
		Status:      message.MessageStatus_MESSAGE_STATUS_SUCCESS,
	}

	// 根据记录的消息类型设置正确的类型
	switch record.MessageType {
	case "chat":
		content.MessageType = message.MessageType_MESSAGE_TYPE_TEXT
	case "image":
		content.MessageType = message.MessageType_MESSAGE_TYPE_IMAGE
	case "file":
		content.MessageType = message.MessageType_MESSAGE_TYPE_FILE
	default:
		content.MessageType = message.MessageType_MESSAGE_TYPE_TEXT
	}

	return content
}

// recordsToMessageContents 将多个数据库记录转换为消息内容数组
func (qm *QueryManager) recordsToMessageContents(records []*MessageRecord) []*message.MessageContent {
	contents := make([]*message.MessageContent, 0, len(records))

	for _, record := range records {
		if content := qm.recordToMessageContent(record); content != nil {
			contents = append(contents, content)
		}
	}

	return contents
}

// cacheCleanupLoop 缓存清理循环
func (qm *QueryManager) cacheCleanupLoop() {
	defer qm.wg.Done()

	ticker := time.NewTicker(qm.cacheExpiry / 2) // 清理频率为过期时间的一半
	defer ticker.Stop()

	for {
		select {
		case <-qm.ctx.Done():
			return
		case <-ticker.C:
			qm.cleanupExpiredCache()
		}
	}
}

// cleanupExpiredCache 清理过期缓存
func (qm *QueryManager) cleanupExpiredCache() {
	now := time.Now()

	qm.cacheMutex.Lock()
	defer qm.cacheMutex.Unlock()

	// 清理消息缓存
	expiredMessages := make([]string, 0)
	for key, cached := range qm.messageCache {
		if now.Sub(cached.cachedAt) > qm.cacheExpiry {
			expiredMessages = append(expiredMessages, key)
		}
	}
	for _, key := range expiredMessages {
		delete(qm.messageCache, key)
	}

	// 清理房间消息缓存
	expiredRooms := make([]uint64, 0)
	for roomID, cached := range qm.roomCache {
		if now.Sub(cached.cachedAt) > qm.cacheExpiry {
			expiredRooms = append(expiredRooms, roomID)
		}
	}
	for _, roomID := range expiredRooms {
		delete(qm.roomCache, roomID)
	}

	if len(expiredMessages) > 0 || len(expiredRooms) > 0 {
		log.Printf("Cleaned up %d expired message cache entries and %d room cache entries",
			len(expiredMessages), len(expiredRooms))
	}
}

// ClearCache 清理所有缓存
func (qm *QueryManager) ClearCache() {
	qm.cacheMutex.Lock()
	defer qm.cacheMutex.Unlock()

	qm.messageCache = make(map[string]*CachedMessage)
	qm.roomCache = make(map[uint64]*CachedRoomMessages)

	log.Printf("Query cache cleared")
}

// sendMessageFetchError 发送消息获取错误响应
func (qm *QueryManager) sendMessageFetchError(routerConn *RouterConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	response := &message.MessageFetchResponse{
		MessageId: []byte{}, // 空的消息ID
		Exists:    false,
		Status:    message.MessageStatus_MESSAGE_STATUS_FAILED,
		Reason:    errorMsg,
	}
	return qm.server.sendResponse(routerConn, envelope, response)
}

// sendRoomMessageFetchError 发送房间消息获取错误响应
func (qm *QueryManager) sendRoomMessageFetchError(routerConn *RouterConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	response := &message.RoomMessageFetchResponse{
		RoomId:   0,
		Messages: nil,
		Status:   message.RoomMessageFetchStatus_ROOM_MESSAGE_FETCH_FAILED,
		Reason:   errorMsg,
	}
	return qm.server.sendResponse(routerConn, envelope, response)
}

// GetCacheStats 获取缓存统计
func (qm *QueryManager) GetCacheStats() map[string]interface{} {
	qm.cacheMutex.RLock()
	defer qm.cacheMutex.RUnlock()

	return map[string]interface{}{
		"message_cache_size": len(qm.messageCache),
		"room_cache_size":    len(qm.roomCache),
		"cache_expiry":       qm.cacheExpiry.String(),
	}
}

// GetStats 获取统计信息
func (qm *QueryManager) GetStats() map[string]interface{} {
	avgQueryTime := atomic.LoadInt64(&qm.avgQueryTime)
	queries := atomic.LoadInt64(&qm.queriesProcessed)
	if queries > 0 {
		avgQueryTime = avgQueryTime / queries
	}

	stats := map[string]interface{}{
		"queries_processed": queries,
		"cache_hits":        atomic.LoadInt64(&qm.cacheHits),
		"cache_misses":      atomic.LoadInt64(&qm.cacheMisses),
		"avg_query_time_ns": avgQueryTime,
	}

	// 合并缓存统计
	cacheStats := qm.GetCacheStats()
	for k, v := range cacheStats {
		stats[k] = v
	}

	return stats
}
