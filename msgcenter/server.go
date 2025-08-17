package msgcenter

import (
	"context"
	"database/sql"
	"fmt"
	"liveChatroom/message"
	"liveChatroom/util/net/api"
	"log"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MessageCenterServer 消息中心服务器 - 负责消息持久化、查询和数据管理
type MessageCenterServer struct {
	// 网络服务
	server *api.Server

	// 基础组件
	codec            *message.MessageCodec
	connSeqGenerator *message.ConnSeqGenerator

	// 数据库连接
	db *sql.DB

	// 核心管理器
	messageManager     *MessageManager
	kafkaManager       *KafkaManager
	databaseManager    *DatabaseManager
	queryManager       *QueryManager
	reliabilityManager *CenterReliabilityManager

	// 配置
	config *MessageCenterConfig

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 状态
	running int32  // 原子操作
	nodeID  string // 消息中心节点ID

	// 统计信息
	stats *MessageCenterStats
}

// MessageCenterConfig 消息中心配置
type MessageCenterConfig struct {
	// 基础配置
	Host   string
	Port   int
	NodeID string

	// 数据库配置
	Database struct {
		Driver       string // "mysql"
		Host         string
		Port         int
		Username     string
		Password     string
		Database     string
		Charset      string
		MaxOpenConns int
		MaxIdleConns int
		MaxLifetime  time.Duration
	}

	// Kafka配置
	Kafka struct {
		Brokers      []string
		Topic        string
		Partition    int
		GroupID      string
		BatchSize    int
		BatchTimeout time.Duration
	}

	// 消息配置
	MessageTimeout    time.Duration
	MaxRetries        int
	HeartbeatInterval time.Duration
	QueryTimeout      time.Duration

	// 存储配置
	MessageRetentionDays int
	BatchInsertSize      int
	CleanupInterval      time.Duration

	// 性能配置
	MaxConnections  int
	ReadBufferSize  int
	WriteBufferSize int
	WorkerPoolSize  int

	// 可靠性配置
	EnableReliability bool
	FetchTimeout      time.Duration
	PendingMessageTTL time.Duration
}

// MessageCenterStats 消息中心统计信息
type MessageCenterStats struct {
	// 基础统计
	StartTime     time.Time
	LastHeartbeat time.Time

	// 连接统计
	TotalConnections  int64
	ActiveConnections int64
	RouterConnections int64

	// 消息统计
	MessagesReceived  int64
	MessagesPersisted int64
	MessagesQueried   int64
	MessagesDropped   int64
	MessagesFailed    int64

	// 存储统计
	DatabaseWrites int64
	DatabaseReads  int64
	KafkaMessages  int64
	KafkaErrors    int64

	// 性能统计
	AvgPersistTime  time.Duration
	AvgQueryTime    time.Duration
	PendingMessages int64
}

// NewMessageCenterServer 创建消息中心服务器
func NewMessageCenterServer(config *MessageCenterConfig) (*MessageCenterServer, error) {
	if config == nil {
		config = DefaultMessageCenterConfig()
	}

	// 生成节点ID
	if config.NodeID == "" {
		config.NodeID = fmt.Sprintf("msgcenter_%s_%d_%d", config.Host, config.Port, time.Now().Unix())
	}

	ctx, cancel := context.WithCancel(context.Background())

	mcs := &MessageCenterServer{
		config:           config,
		nodeID:           config.NodeID,
		ctx:              ctx,
		cancel:           cancel,
		stats:            &MessageCenterStats{StartTime: time.Now()},
		codec:            message.NewMessageCodec(),
		connSeqGenerator: message.NewConnSeqGenerator(),
	}

	// 初始化数据库
	if err := mcs.initDatabase(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to init database: %v", err)
	}

	// 初始化网络服务器
	if err := mcs.initServer(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to init server: %v", err)
	}

	// 初始化管理器
	mcs.initManagers()

	return mcs, nil
}

// DefaultMessageCenterConfig 默认消息中心配置
func DefaultMessageCenterConfig() *MessageCenterConfig {
	return &MessageCenterConfig{
		Host:   "0.0.0.0",
		Port:   8082,
		NodeID: "",
		Database: struct {
			Driver       string
			Host         string
			Port         int
			Username     string
			Password     string
			Database     string
			Charset      string
			MaxOpenConns int
			MaxIdleConns int
			MaxLifetime  time.Duration
		}{
			Driver:       "mysql",
			Host:         "localhost",
			Port:         3306,
			Username:     "livechat",
			Password:     "password",
			Database:     "livechat",
			Charset:      "utf8mb4",
			MaxOpenConns: 100,
			MaxIdleConns: 10,
			MaxLifetime:  time.Hour,
		},
		Kafka: struct {
			Brokers      []string
			Topic        string
			Partition    int
			GroupID      string
			BatchSize    int
			BatchTimeout time.Duration
		}{
			Brokers:      []string{"localhost:9092"},
			Topic:        "livechat_messages",
			Partition:    0,
			GroupID:      "msgcenter_group",
			BatchSize:    100,
			BatchTimeout: 5 * time.Second,
		},
		MessageTimeout:       10 * time.Second,
		MaxRetries:           3,
		HeartbeatInterval:    30 * time.Second,
		QueryTimeout:         5 * time.Second,
		MessageRetentionDays: 30,
		BatchInsertSize:      1000,
		CleanupInterval:      24 * time.Hour,
		MaxConnections:       1000,
		ReadBufferSize:       4096,
		WriteBufferSize:      4096,
		WorkerPoolSize:       50,
		EnableReliability:    true,
		FetchTimeout:         5 * time.Second,
		PendingMessageTTL:    10 * time.Minute,
	}
}

// initDatabase 初始化数据库
func (mcs *MessageCenterServer) initDatabase() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true&loc=Local",
		mcs.config.Database.Username,
		mcs.config.Database.Password,
		mcs.config.Database.Host,
		mcs.config.Database.Port,
		mcs.config.Database.Database,
		mcs.config.Database.Charset,
	)

	db, err := sql.Open(mcs.config.Database.Driver, dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// 配置连接池
	db.SetMaxOpenConns(mcs.config.Database.MaxOpenConns)
	db.SetMaxIdleConns(mcs.config.Database.MaxIdleConns)
	db.SetConnMaxLifetime(mcs.config.Database.MaxLifetime)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("database connection failed: %v", err)
	}

	mcs.db = db
	log.Printf("Database connected: %s@%s:%d/%s",
		mcs.config.Database.Username,
		mcs.config.Database.Host,
		mcs.config.Database.Port,
		mcs.config.Database.Database)

	// 初始化数据库表
	if err := mcs.initDatabaseTables(); err != nil {
		return fmt.Errorf("failed to init database tables: %v", err)
	}

	return nil
}

// initDatabaseTables 初始化数据库表
func (mcs *MessageCenterServer) initDatabaseTables() error {
	tables := []string{
		// 消息表
		`CREATE TABLE IF NOT EXISTS messages (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			message_id BINARY(16) NOT NULL UNIQUE,
			user_id INT UNSIGNED NOT NULL,
			room_id BIGINT UNSIGNED NOT NULL,
			login_id TINYINT UNSIGNED NOT NULL,
			message_type VARCHAR(50) NOT NULL,
			content TEXT,
			data BLOB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX idx_message_id (message_id),
			INDEX idx_user_room_time (user_id, room_id, created_at),
			INDEX idx_room_time (room_id, created_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		// 房间表
		`CREATE TABLE IF NOT EXISTS rooms (
			id BIGINT UNSIGNED PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			creator_id INT UNSIGNED NOT NULL,
			max_members INT UNSIGNED NOT NULL DEFAULT 100,
			password VARCHAR(255),
			status TINYINT NOT NULL DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX idx_creator (creator_id),
			INDEX idx_status (status)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		// 用户表
		`CREATE TABLE IF NOT EXISTS users (
			id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			username VARCHAR(100) NOT NULL UNIQUE,
			email VARCHAR(255),
			avatar_url VARCHAR(500),
			status TINYINT NOT NULL DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX idx_username (username),
			INDEX idx_status (status)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		// 房间成员表
		`CREATE TABLE IF NOT EXISTS room_members (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			room_id BIGINT UNSIGNED NOT NULL,
			user_id INT UNSIGNED NOT NULL,
			role TINYINT NOT NULL DEFAULT 0,
			joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			left_at TIMESTAMP NULL,
			UNIQUE KEY uk_room_user (room_id, user_id),
			INDEX idx_room (room_id),
			INDEX idx_user (user_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}

	for _, table := range tables {
		if _, err := mcs.db.Exec(table); err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
	}

	log.Printf("Database tables initialized")
	return nil
}

// initServer 初始化网络服务器
func (mcs *MessageCenterServer) initServer() error {
	serverConfig := &api.ServerConfig{
		Address:         fmt.Sprintf("%s:%d", mcs.config.Host, mcs.config.Port),
		MaxConnections:  mcs.config.MaxConnections,
		ReadTimeout:     mcs.config.MessageTimeout,
		WriteTimeout:    mcs.config.MessageTimeout,
		IdleTimeout:     mcs.config.HeartbeatInterval * 2,
		ReactorCount:    4, // 使用4个反应器
		WorkerCount:     mcs.config.WorkerPoolSize,
		ReadBufferSize:  mcs.config.ReadBufferSize,
		WriteBufferSize: mcs.config.WriteBufferSize,
	}

	// 创建事件处理器
	eventHandler := NewMessageCenterEventHandler(mcs)

	// 创建服务器
	var err error
	mcs.server, err = api.NewServer(serverConfig, eventHandler)
	return err
}

// initManagers 初始化管理器
func (mcs *MessageCenterServer) initManagers() {
	// 数据库管理器
	mcs.databaseManager = NewDatabaseManager(mcs.db, mcs.config)

	// Kafka管理器
	mcs.kafkaManager = NewKafkaManager(mcs.config)

	// 消息管理器
	mcs.messageManager = NewMessageManager(mcs, mcs.config)

	// 查询管理器
	mcs.queryManager = NewQueryManager(mcs, mcs.config)

	// 可靠性管理器
	if mcs.config.EnableReliability {
		mcs.reliabilityManager = NewCenterReliabilityManager(mcs, mcs.config)
	}
}

// Start 启动消息中心服务器
func (mcs *MessageCenterServer) Start() error {
	if !atomic.CompareAndSwapInt32(&mcs.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	log.Printf("Starting MessageCenterServer %s on %s:%d", mcs.nodeID, mcs.config.Host, mcs.config.Port)

	// 启动网络服务器
	if err := mcs.server.Start(); err != nil {
		atomic.StoreInt32(&mcs.running, 0)
		return fmt.Errorf("failed to start server: %v", err)
	}

	// 启动管理器
	mcs.databaseManager.Start(mcs.ctx)
	mcs.kafkaManager.Start(mcs.ctx)
	mcs.messageManager.Start(mcs.ctx)
	mcs.queryManager.Start(mcs.ctx)

	if mcs.reliabilityManager != nil {
		mcs.reliabilityManager.Start(mcs.ctx)
	}

	// 启动后台任务
	mcs.wg.Add(1)
	go mcs.backgroundTasks()

	log.Printf("MessageCenterServer %s started successfully", mcs.nodeID)
	return nil
}

// Stop 停止消息中心服务器
func (mcs *MessageCenterServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&mcs.running, 1, 0) {
		return fmt.Errorf("server not running")
	}

	log.Printf("Stopping MessageCenterServer %s...", mcs.nodeID)

	// 取消上下文
	mcs.cancel()

	// 停止网络服务器
	if mcs.server != nil {
		mcs.server.Stop()
	}

	// 关闭数据库连接
	if mcs.db != nil {
		mcs.db.Close()
	}

	// 等待后台任务结束
	mcs.wg.Wait()

	log.Printf("MessageCenterServer %s stopped", mcs.nodeID)
	return nil
}

// backgroundTasks 后台任务
func (mcs *MessageCenterServer) backgroundTasks() {
	defer mcs.wg.Done()

	heartbeatTicker := time.NewTicker(mcs.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	cleanupTicker := time.NewTicker(mcs.config.CleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-mcs.ctx.Done():
			return

		case <-heartbeatTicker.C:
			mcs.performHeartbeat()

		case <-cleanupTicker.C:
			mcs.performCleanup()
		}
	}
}

// performHeartbeat 执行心跳
func (mcs *MessageCenterServer) performHeartbeat() {
	mcs.stats.LastHeartbeat = time.Now()

	// 更新统计信息
	atomic.StoreInt64(&mcs.stats.PendingMessages, int64(mcs.messageManager.GetPendingCount()))

	log.Printf("MessageCenter heartbeat: pending=%d, persisted=%d",
		atomic.LoadInt64(&mcs.stats.PendingMessages),
		atomic.LoadInt64(&mcs.stats.MessagesPersisted))
}

// performCleanup 执行清理
func (mcs *MessageCenterServer) performCleanup() {
	log.Printf("Performing message center cleanup...")

	// 清理过期消息
	mcs.databaseManager.CleanupExpiredMessages()

	// 清理过期的待确认消息
	if mcs.reliabilityManager != nil {
		mcs.reliabilityManager.Cleanup()
	}

	// 压缩Kafka日志（如果支持）
	mcs.kafkaManager.Cleanup()

	log.Printf("Message center cleanup completed")
}

// HandleMessage 处理来自路由层的消息
func (mcs *MessageCenterServer) HandleMessage(routerConn *RouterConnection, data []byte) error {
	// 反序列化消息
	envelope, err := mcs.codec.Deserialize(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %v", err)
	}

	// 更新统计信息
	atomic.AddInt64(&mcs.stats.MessagesReceived, 1)

	// 委托给消息管理器或查询管理器处理
	switch msg := envelope.Message.(type) {
	// 消息查询请求
	case *message.MessageEnvelope_MessageFetchRequest:
		return mcs.queryManager.HandleMessageFetchRequest(routerConn, envelope, msg.MessageFetchRequest)
	case *message.MessageEnvelope_RoomMessageFetchRequest:
		return mcs.queryManager.HandleRoomMessageFetchRequest(routerConn, envelope, msg.RoomMessageFetchRequest)

	// 心跳
	case *message.MessageEnvelope_Heartbeat:
		return mcs.handleHeartbeat(routerConn, envelope, msg.Heartbeat)

	// ACK消息（可靠性相关）
	case *message.MessageEnvelope_MessageFetchResponse,
		*message.MessageEnvelope_RoomMessageFetchResponse,
		*message.MessageEnvelope_HeartbeatAck:
		return mcs.handleACKMessage(routerConn, envelope)

	// 其他消息需要持久化
	default:
		return mcs.messageManager.PersistMessage(routerConn, envelope)
	}
}

// handleHeartbeat 处理心跳
func (mcs *MessageCenterServer) handleHeartbeat(routerConn *RouterConnection, envelope *message.MessageEnvelope, req *message.Heartbeat) error {
	// 更新路由连接心跳时间
	routerConn.UpdateHeartbeat()

	// 发送心跳响应
	ack := &message.HeartbeatAck{
		Timestamp:  req.Timestamp,
		ServerTime: uint64(time.Now().UnixMilli()),
	}

	return mcs.sendResponse(routerConn, envelope, ack)
}

// handleACKMessage 处理ACK消息
func (mcs *MessageCenterServer) handleACKMessage(routerConn *RouterConnection, envelope *message.MessageEnvelope) error {
	// 委托给可靠性管理器处理
	if mcs.reliabilityManager != nil {
		mcs.reliabilityManager.HandleACK(envelope)
	}
	return nil
}

// sendResponse 发送响应消息
func (mcs *MessageCenterServer) sendResponse(routerConn *RouterConnection, originalEnvelope *message.MessageEnvelope, responseMessage interface{}) error {
	responseEnvelope, err := mcs.codec.CreateEnvelope(
		0, // 系统响应
		0, // 无特定房间
		0, // 系统登录ID
		mcs.connSeqGenerator.Next(),
		responseMessage,
	)
	if err != nil {
		return fmt.Errorf("failed to create response envelope: %v", err)
	}

	data, err := mcs.codec.Serialize(responseEnvelope)
	if err != nil {
		return fmt.Errorf("failed to serialize response: %v", err)
	}

	return routerConn.Send(data)
}

// GetStats 获取统计信息
func (mcs *MessageCenterServer) GetStats() *MessageCenterStats {
	stats := *mcs.stats
	stats.ActiveConnections = atomic.LoadInt64(&mcs.stats.ActiveConnections)
	stats.PendingMessages = atomic.LoadInt64(&mcs.stats.PendingMessages)
	return &stats
}

// IsRunning 检查服务器是否运行中
func (mcs *MessageCenterServer) IsRunning() bool {
	return atomic.LoadInt32(&mcs.running) == 1
}

// GetNodeID 获取节点ID
func (mcs *MessageCenterServer) GetNodeID() string {
	return mcs.nodeID
}

// GetMessageManager 获取消息管理器
func (mcs *MessageCenterServer) GetMessageManager() *MessageManager {
	return mcs.messageManager
}

// GetQueryManager 获取查询管理器
func (mcs *MessageCenterServer) GetQueryManager() *QueryManager {
	return mcs.queryManager
}

// GetDatabaseManager 获取数据库管理器
func (mcs *MessageCenterServer) GetDatabaseManager() *DatabaseManager {
	return mcs.databaseManager
}

// GetKafkaManager 获取Kafka管理器
func (mcs *MessageCenterServer) GetKafkaManager() *KafkaManager {
	return mcs.kafkaManager
}
