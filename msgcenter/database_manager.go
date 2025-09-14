package msgcenter

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// DatabaseManager 数据库管理器 - 负责MySQL数据库操作
type DatabaseManager struct {
	db     *sql.DB
	config *MessageCenterConfig

	// 预编译语句
	insertMessageStmt *sql.Stmt
	queryMessageStmt  *sql.Stmt
	queryRoomMsgStmt  *sql.Stmt

	// 统计信息
	insertsExecuted  int64
	queriesExecuted  int64
	transactionCount int64
	avgInsertTime    int64 // 纳秒
	avgQueryTime     int64 // 纳秒

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mutex  sync.RWMutex
}

// NewDatabaseManager 创建数据库管理器
func NewDatabaseManager(db *sql.DB, config *MessageCenterConfig) *DatabaseManager {
	return &DatabaseManager{
		db:     db,
		config: config,
	}
}

// Start 启动数据库管理器
func (dm *DatabaseManager) Start(ctx context.Context) {
	dm.ctx, dm.cancel = context.WithCancel(ctx)

	// 预编译SQL语句
	if err := dm.prepareStatements(); err != nil {
		log.Printf("Failed to prepare statements: %v", err)
	}

	// 启动清理协程
	dm.wg.Add(1)
	go dm.cleanupLoop()

	log.Printf("DatabaseManager started")
}

// Stop 停止数据库管理器
func (dm *DatabaseManager) Stop() {
	if dm.cancel != nil {
		dm.cancel()
	}
	dm.wg.Wait()

	// 关闭预编译语句
	dm.closeStatements()

	log.Printf("DatabaseManager stopped")
}

// prepareStatements 预编译SQL语句
func (dm *DatabaseManager) prepareStatements() error {
	var err error

	// 插入消息语句
	// INSERT IGNORE + message_id唯一索引：Kafka重复消费/重放时幂等，不产生重复数据
	insertSQL := `INSERT IGNORE INTO messages (message_id, user_id, room_id, login_id, message_type, content, data, created_at)
				  VALUES (?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(? / 1000))`
	dm.insertMessageStmt, err = dm.db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %v", err)
	}

	// 查询单条消息语句
	querySQL := `SELECT id, user_id, room_id, login_id, message_type, content, data, 
				 UNIX_TIMESTAMP(created_at) * 1000 as timestamp
				 FROM messages WHERE message_id = ?`
	dm.queryMessageStmt, err = dm.db.Prepare(querySQL)
	if err != nil {
		return fmt.Errorf("failed to prepare query statement: %v", err)
	}

	// 查询房间消息语句
	queryRoomSQL := `SELECT id, message_id, user_id, login_id, message_type, content, data,
					 UNIX_TIMESTAMP(created_at) * 1000 as timestamp
					 FROM messages 
					 WHERE room_id = ? AND created_at >= FROM_UNIXTIME(? / 1000) 
					 ORDER BY created_at DESC LIMIT ?`
	dm.queryRoomMsgStmt, err = dm.db.Prepare(queryRoomSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare room query statement: %v", err)
	}

	log.Printf("Database statements prepared")
	return nil
}

// closeStatements 关闭预编译语句
func (dm *DatabaseManager) closeStatements() {
	if dm.insertMessageStmt != nil {
		dm.insertMessageStmt.Close()
	}
	if dm.queryMessageStmt != nil {
		dm.queryMessageStmt.Close()
	}
	if dm.queryRoomMsgStmt != nil {
		dm.queryRoomMsgStmt.Close()
	}
}

// BatchInsertMessages 批量插入消息
func (dm *DatabaseManager) BatchInsertMessages(batch []*PersistMessage) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()

	// 开始事务
	tx, err := dm.db.BeginTx(dm.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// 预编译事务内语句
	stmt := tx.Stmt(dm.insertMessageStmt)
	defer stmt.Close()

	// 批量执行插入
	for _, persistMsg := range batch {
		info := persistMsg.messageInfo
		_, err := stmt.ExecContext(dm.ctx,
			info.MessageID,
			info.UserID,
			info.RoomID,
			info.LoginID,
			info.MessageType,
			info.Content,
			info.Data,
			info.Timestamp,
		)
		if err != nil {
			log.Printf("Failed to insert message %x: %v", info.MessageID, err)
			continue // 继续处理其他消息，不中断整个批次
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	// 更新统计信息
	duration := time.Since(start)
	atomic.AddInt64(&dm.insertsExecuted, int64(len(batch)))
	atomic.AddInt64(&dm.transactionCount, 1)
	atomic.AddInt64(&dm.avgInsertTime, duration.Nanoseconds())

	return nil
}

// QueryMessage 查询单条消息
func (dm *DatabaseManager) QueryMessage(messageID []byte) (*MessageRecord, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		atomic.AddInt64(&dm.queriesExecuted, 1)
		atomic.AddInt64(&dm.avgQueryTime, duration.Nanoseconds())
	}()

	dm.mutex.RLock()
	stmt := dm.queryMessageStmt
	dm.mutex.RUnlock()

	if stmt == nil {
		return nil, fmt.Errorf("query statement not prepared")
	}

	row := stmt.QueryRowContext(dm.ctx, messageID)

	var record MessageRecord
	var data []byte
	err := row.Scan(
		&record.ID,
		&record.UserID,
		&record.RoomID,
		&record.LoginID,
		&record.MessageType,
		&record.Content,
		&data,
		&record.Timestamp,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // 消息不存在
		}
		return nil, fmt.Errorf("failed to scan message: %v", err)
	}

	record.MessageID = make([]byte, len(messageID))
	copy(record.MessageID, messageID)
	record.Data = data

	return &record, nil
}

// QueryRoomMessages 查询房间消息
func (dm *DatabaseManager) QueryRoomMessages(roomID uint64, startTime int64, limit int) ([]*MessageRecord, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		atomic.AddInt64(&dm.queriesExecuted, 1)
		atomic.AddInt64(&dm.avgQueryTime, duration.Nanoseconds())
	}()

	dm.mutex.RLock()
	stmt := dm.queryRoomMsgStmt
	dm.mutex.RUnlock()

	if stmt == nil {
		return nil, fmt.Errorf("room query statement not prepared")
	}

	rows, err := stmt.QueryContext(dm.ctx, roomID, startTime, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query room messages: %v", err)
	}
	defer rows.Close()

	var messages []*MessageRecord
	for rows.Next() {
		var record MessageRecord
		var messageID []byte
		var data []byte

		err := rows.Scan(
			&record.ID,
			&messageID,
			&record.UserID,
			&record.LoginID,
			&record.MessageType,
			&record.Content,
			&data,
			&record.Timestamp,
		)

		if err != nil {
			log.Printf("Failed to scan room message: %v", err)
			continue
		}

		record.RoomID = uint32(roomID)
		record.MessageID = messageID
		record.Data = data
		messages = append(messages, &record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %v", err)
	}

	return messages, nil
}

// CleanupExpiredMessages 清理过期消息
func (dm *DatabaseManager) CleanupExpiredMessages() {
	cutoffTime := time.Now().AddDate(0, 0, -dm.config.MessageRetentionDays)
	cutoffTimestamp := cutoffTime.Unix() * 1000

	deleteSQL := `DELETE FROM messages WHERE created_at < FROM_UNIXTIME(? / 1000) LIMIT 10000`

	result, err := dm.db.ExecContext(dm.ctx, deleteSQL, cutoffTimestamp)
	if err != nil {
		log.Printf("Failed to cleanup expired messages: %v", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		log.Printf("Cleaned up %d expired messages", rowsAffected)
	}
}

// cleanupLoop 清理循环
func (dm *DatabaseManager) cleanupLoop() {
	defer dm.wg.Done()

	ticker := time.NewTicker(6 * time.Hour) // 每6小时清理一次
	defer ticker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.CleanupExpiredMessages()
		}
	}
}

// CreateUser 创建用户
func (dm *DatabaseManager) CreateUser(username, email, avatarURL string) (uint32, error) {
	insertSQL := `INSERT INTO users (username, email, avatar_url) VALUES (?, ?, ?)`

	result, err := dm.db.ExecContext(dm.ctx, insertSQL, username, email, avatarURL)
	if err != nil {
		return 0, fmt.Errorf("failed to create user: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get user ID: %v", err)
	}

	return uint32(userID), nil
}

// CreateRoom 创建房间
func (dm *DatabaseManager) CreateRoom(roomID uint64, name string, creatorID uint32, maxMembers uint32, password string) error {
	insertSQL := `INSERT INTO rooms (id, name, creator_id, max_members, password) VALUES (?, ?, ?, ?, ?)`

	_, err := dm.db.ExecContext(dm.ctx, insertSQL, roomID, name, creatorID, maxMembers, password)
	if err != nil {
		return fmt.Errorf("failed to create room: %v", err)
	}

	return nil
}

// GetRoom 获取房间信息
func (dm *DatabaseManager) GetRoom(roomID uint64) (*RoomRecord, error) {
	querySQL := `SELECT id, name, creator_id, max_members, password, status, 
				 UNIX_TIMESTAMP(created_at) * 1000 as created_at,
				 UNIX_TIMESTAMP(updated_at) * 1000 as updated_at
				 FROM rooms WHERE id = ?`

	row := dm.db.QueryRowContext(dm.ctx, querySQL, roomID)

	var room RoomRecord
	err := row.Scan(
		&room.ID,
		&room.Name,
		&room.CreatorID,
		&room.MaxMembers,
		&room.Password,
		&room.Status,
		&room.CreatedAt,
		&room.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan room: %v", err)
	}

	return &room, nil
}

// GetStats 获取统计信息
func (dm *DatabaseManager) GetStats() map[string]interface{} {
	avgInsertTime := atomic.LoadInt64(&dm.avgInsertTime)
	avgQueryTime := atomic.LoadInt64(&dm.avgQueryTime)
	inserts := atomic.LoadInt64(&dm.insertsExecuted)
	queries := atomic.LoadInt64(&dm.queriesExecuted)

	if inserts > 0 {
		avgInsertTime = avgInsertTime / inserts
	}
	if queries > 0 {
		avgQueryTime = avgQueryTime / queries
	}

	return map[string]interface{}{
		"inserts_executed":   inserts,
		"queries_executed":   queries,
		"transaction_count":  atomic.LoadInt64(&dm.transactionCount),
		"avg_insert_time_ns": avgInsertTime,
		"avg_query_time_ns":  avgQueryTime,
		"max_open_conns":     dm.config.Database.MaxOpenConns,
		"max_idle_conns":     dm.config.Database.MaxIdleConns,
	}
}

// MessageRecord 消息记录
type MessageRecord struct {
	ID          int64  `json:"id"`
	MessageID   []byte `json:"message_id"`
	UserID      uint32 `json:"user_id"`
	RoomID      uint32 `json:"room_id"`
	LoginID     uint8  `json:"login_id"`
	MessageType string `json:"message_type"`
	Content     string `json:"content"`
	Data        []byte `json:"data,omitempty"`
	Timestamp   int64  `json:"timestamp"`
}

// RoomRecord 房间记录
type RoomRecord struct {
	ID         uint64 `json:"id"`
	Name       string `json:"name"`
	CreatorID  uint32 `json:"creator_id"`
	MaxMembers uint32 `json:"max_members"`
	Password   string `json:"password,omitempty"`
	Status     int    `json:"status"`
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
}
