package connection

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"liveChatroom/util/net/api"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConnectionServer 连接服务器 - 负责客户端连接管理和消息处理
type ConnectionServer struct {
	// 网络服务
	server *api.Server

	// 基础组件
	codec            *message.MessageCodec
	connSeqGenerator *message.ConnSeqGenerator

	// Redis客户端
	redis *redis.Client

	// 核心管理器
	connectionManager *ConnectionManager
	roomManager       *RoomManager
	routerManager     *RouterManager
	messageHandler    *MessageHandler

	// 可靠性保证器
	reliabilityGuarantor *ReliabilityGuarantor

	// 配置
	config *ConnectionConfig

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 状态
	running int32 // 原子操作

	// 统计信息
	stats *ConnectionStats
}

// ConnectionConfig 连接服务器配置
type ConnectionConfig struct {
	// 基础配置
	Host string
	Port int

	// Redis配置
	Redis struct {
		Addr     string
		Password string
		DB       int
	}

	// 消息配置
	MessageTimeout    time.Duration
	MaxRetries        int
	HeartbeatInterval time.Duration

	// 房间配置
	MaxRoomsPerNode     int
	RoomCleanupInterval time.Duration

	// 路由配置
	RouterAddrs          []string
	RouterConnectTimeout time.Duration
	RouterRetryInterval  time.Duration

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

// ConnectionStats 连接服务器统计信息
type ConnectionStats struct {
	// 连接统计
	TotalConnections    int64
	ActiveConnections   int64
	ConnectionsAccepted int64
	ConnectionsRejected int64

	// 消息统计
	MessagesReceived int64
	MessagesSent     int64
	MessagesDropped  int64
	MessagesRetried  int64
	MessagesFetched  int64

	// 房间统计
	ActiveRooms  int64
	TotalRooms   int64
	RoomMessages int64

	// 路由统计
	RouterConnections int64
	RouterMessages    int64

	// 时间统计
	StartTime       time.Time
	LastMessageTime time.Time
	LastHeartbeat   time.Time
}

// NewConnectionServer 创建连接服务器
func NewConnectionServer(config *ConnectionConfig) (*ConnectionServer, error) {
	if config == nil {
		config = DefaultConnectionConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	cs := &ConnectionServer{
		config:           config,
		ctx:              ctx,
		cancel:           cancel,
		stats:            &ConnectionStats{StartTime: time.Now()},
		codec:            message.NewMessageCodec(),
		connSeqGenerator: message.NewConnSeqGenerator(),
	}

	// 初始化Redis客户端
	if err := cs.initRedis(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to init redis: %v", err)
	}

	// 初始化网络服务器
	if err := cs.initServer(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to init server: %v", err)
	}

	// 初始化管理器
	cs.initManagers()

	return cs, nil
}

// DefaultConnectionConfig 默认连接配置
func DefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		Host: "0.0.0.0",
		Port: 8080,
		Redis: struct {
			Addr     string
			Password string
			DB       int
		}{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
		MessageTimeout:       10 * time.Second,
		MaxRetries:           3,
		HeartbeatInterval:    30 * time.Second,
		MaxRoomsPerNode:      1000,
		RoomCleanupInterval:  5 * time.Minute,
		RouterAddrs:          []string{"ws://localhost:8081"},
		RouterConnectTimeout: 10 * time.Second,
		RouterRetryInterval:  5 * time.Second,
		MaxConnections:       10000,
		ReadBufferSize:       4096,
		WriteBufferSize:      4096,
		WorkerPoolSize:       100,
		EnableReliability:    true,
		FetchTimeout:         5 * time.Second,
		PendingMessageTTL:    5 * time.Minute,
	}
}

// initRedis 初始化Redis客户端
func (cs *ConnectionServer) initRedis() error {
	cs.redis = redis.NewClient(&redis.Options{
		Addr:     cs.config.Redis.Addr,
		Password: cs.config.Redis.Password,
		DB:       cs.config.Redis.DB,
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cs.redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis connection failed: %v", err)
	}

	log.Printf("Redis connected: %s", cs.config.Redis.Addr)
	return nil
}

// initServer 初始化网络服务器
func (cs *ConnectionServer) initServer() error {
	serverConfig := &api.ServerConfig{
		Address:         fmt.Sprintf("%s:%d", cs.config.Host, cs.config.Port),
		MaxConnections:  cs.config.MaxConnections,
		ReadTimeout:     cs.config.MessageTimeout,
		WriteTimeout:    cs.config.MessageTimeout,
		IdleTimeout:     cs.config.HeartbeatInterval * 2,
		ReactorCount:    4, // 使用4个反应器
		WorkerCount:     cs.config.WorkerPoolSize,
		ReadBufferSize:  cs.config.ReadBufferSize,
		WriteBufferSize: cs.config.WriteBufferSize,
	}

	// 创建事件处理器
	eventHandler := NewConnectionEventHandler(cs)

	// 创建服务器
	var err error
	cs.server, err = api.NewServer(serverConfig, eventHandler)
	return err
}

// initManagers 初始化管理器
func (cs *ConnectionServer) initManagers() {
	// 连接管理器
	cs.connectionManager = NewConnectionManager(cs.config.MaxConnections)

	// 房间管理器
	cs.roomManager = NewRoomManager(cs.config.MaxRoomsPerNode, cs.redis)

	// 路由管理器
	cs.routerManager = NewRouterManager(cs.config.RouterAddrs, cs.config)

	// 消息处理器
	cs.messageHandler = NewMessageHandler(cs)

	// 可靠性保证器
	if cs.config.EnableReliability {
		cs.reliabilityGuarantor = NewReliabilityGuarantor(cs, cs.config)
	}
}

// Start 启动连接服务器
func (cs *ConnectionServer) Start() error {
	if !atomic.CompareAndSwapInt32(&cs.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	log.Printf("Starting ConnectionServer on %s:%d", cs.config.Host, cs.config.Port)

	// 启动网络服务器
	if err := cs.server.Start(); err != nil {
		atomic.StoreInt32(&cs.running, 0)
		return fmt.Errorf("failed to start server: %v", err)
	}

	// 启动管理器
	cs.connectionManager.Start(cs.ctx)
	cs.roomManager.Start(cs.ctx)
	cs.routerManager.Start(cs.ctx)
	cs.messageHandler.Start(cs.ctx)

	if cs.reliabilityGuarantor != nil {
		cs.reliabilityGuarantor.Start(cs.ctx)
	}

	// 启动后台任务
	cs.wg.Add(1)
	go cs.backgroundTasks()

	log.Printf("ConnectionServer started successfully")
	return nil
}

// Stop 停止连接服务器
func (cs *ConnectionServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&cs.running, 1, 0) {
		return fmt.Errorf("server not running")
	}

	log.Printf("Stopping ConnectionServer...")

	// 取消上下文
	cs.cancel()

	// 停止网络服务器
	if cs.server != nil {
		cs.server.Stop()
	}

	// 关闭Redis连接
	if cs.redis != nil {
		cs.redis.Close()
	}

	// 等待后台任务结束
	cs.wg.Wait()

	log.Printf("ConnectionServer stopped")
	return nil
}

// backgroundTasks 后台任务
func (cs *ConnectionServer) backgroundTasks() {
	defer cs.wg.Done()

	heartbeatTicker := time.NewTicker(cs.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	cleanupTicker := time.NewTicker(cs.config.RoomCleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			return

		case <-heartbeatTicker.C:
			cs.performHeartbeat()

		case <-cleanupTicker.C:
			cs.performCleanup()
		}
	}
}

// performHeartbeat 执行心跳
func (cs *ConnectionServer) performHeartbeat() {
	cs.stats.LastHeartbeat = time.Now()

	// 向所有活跃连接发送心跳
	cs.connectionManager.BroadcastHeartbeat()

	// 向路由节点发送心跳
	cs.routerManager.SendHeartbeat()

	// 更新Redis中的节点状态
	cs.updateNodeStatus()
}

// performCleanup 执行清理
func (cs *ConnectionServer) performCleanup() {
	// 清理过期连接
	cs.connectionManager.CleanupExpiredConnections()

	// 清理空房间
	cs.roomManager.CleanupEmptyRooms()

	// 清理过期的待确认消息
	if cs.reliabilityGuarantor != nil {
		cs.reliabilityGuarantor.Cleanup()
	}
}

// updateNodeStatus 更新节点状态
func (cs *ConnectionServer) updateNodeStatus() {
	nodeInfo := map[string]interface{}{
		"host":               cs.config.Host,
		"port":               cs.config.Port,
		"active_connections": atomic.LoadInt64(&cs.stats.ActiveConnections),
		"active_rooms":       atomic.LoadInt64(&cs.stats.ActiveRooms),
		"last_heartbeat":     time.Now().Unix(),
		"version":            "1.0.0",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodeKey := fmt.Sprintf("connection_nodes:%s:%d", cs.config.Host, cs.config.Port)
	if err := cs.redis.HMSet(ctx, nodeKey, nodeInfo).Err(); err != nil {
		log.Printf("Failed to update node status: %v", err)
	}

	// 设置TTL
	cs.redis.Expire(ctx, nodeKey, cs.config.HeartbeatInterval*3)
}

// HandleMessage 处理客户端消息
func (cs *ConnectionServer) HandleMessage(conn *ClientConnection, data []byte) error {
	// 反序列化消息
	envelope, err := cs.codec.Deserialize(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %v", err)
	}

	// 更新统计信息
	atomic.AddInt64(&cs.stats.MessagesReceived, 1)
	cs.stats.LastMessageTime = time.Now()

	// 提取消息信息
	userID, roomID, loginID, timestamp, err := cs.codec.ExtractMessageInfo(envelope)
	if err != nil {
		return fmt.Errorf("failed to extract message info: %v", err)
	}

	// 更新连接信息
	conn.UpdateLastActivity(userID, uint64(roomID), loginID, timestamp)

	// 委托给消息处理器
	return cs.messageHandler.HandleMessage(conn, envelope)
}

// SendMessageToClient 发送消息给客户端
func (cs *ConnectionServer) SendMessageToClient(conn *ClientConnection, envelope *message.MessageEnvelope) error {
	// 序列化消息
	data, err := cs.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 发送数据
	if err := conn.Send(data); err != nil {
		atomic.AddInt64(&cs.stats.MessagesDropped, 1)
		return fmt.Errorf("failed to send message: %v", err)
	}

	atomic.AddInt64(&cs.stats.MessagesSent, 1)
	return nil
}

// BroadcastToRoom 向房间广播消息
func (cs *ConnectionServer) BroadcastToRoom(roomID uint64, envelope *message.MessageEnvelope, excludeConn *ClientConnection) error {
	room := cs.roomManager.GetRoom(roomID)
	if room == nil {
		return fmt.Errorf("room %d not found", roomID)
	}

	// 序列化消息
	data, err := cs.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 广播给房间内所有连接
	sent := 0
	for _, connID := range room.GetMembers() {
		if conn := cs.connectionManager.GetConnection(connID); conn != nil {
			if excludeConn != nil && conn.GetID() == excludeConn.GetID() {
				continue // 排除指定连接
			}

			if err := conn.Send(data); err != nil {
				log.Printf("Failed to send message to connection %s: %v", connID, err)
				atomic.AddInt64(&cs.stats.MessagesDropped, 1)
			} else {
				sent++
			}
		}
	}

	atomic.AddInt64(&cs.stats.MessagesSent, int64(sent))
	atomic.AddInt64(&cs.stats.RoomMessages, 1)
	return nil
}

// GetStats 获取统计信息
func (cs *ConnectionServer) GetStats() *ConnectionStats {
	stats := *cs.stats
	stats.ActiveConnections = atomic.LoadInt64(&cs.stats.ActiveConnections)
	stats.ActiveRooms = atomic.LoadInt64(&cs.stats.ActiveRooms)
	return &stats
}

// IsRunning 检查服务器是否运行中
func (cs *ConnectionServer) IsRunning() bool {
	return atomic.LoadInt32(&cs.running) == 1
}

// GetConnectionManager 获取连接管理器
func (cs *ConnectionServer) GetConnectionManager() *ConnectionManager {
	return cs.connectionManager
}

// GetRoomManager 获取房间管理器
func (cs *ConnectionServer) GetRoomManager() *RoomManager {
	return cs.roomManager
}

// GetRouterManager 获取路由管理器
func (cs *ConnectionServer) GetRouterManager() *RouterManager {
	return cs.routerManager
}

// GetMessageHandler 获取消息处理器
func (cs *ConnectionServer) GetMessageHandler() *MessageHandler {
	return cs.messageHandler
}
