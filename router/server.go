package router

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

// RouterServer 路由服务器 - 负责消息分发、节点管理和负载均衡
type RouterServer struct {
	// 网络服务
	server *api.Server

	// 基础组件
	codec            *message.MessageCodec
	connSeqGenerator *message.ConnSeqGenerator

	// Redis集群客户端
	redisCluster *redis.ClusterClient
	redis        *redis.Client

	// 核心管理器
	nodeManager        *NodeManager
	messageRouter      *MessageRouter
	loadBalancer       *LoadBalancer
	redisManager       *RedisManager
	reliabilityManager *RouterReliabilityManager
	messageClassifier  *MessageClassifier

	// 配置
	config *RouterConfig

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 状态
	running int32  // 原子操作
	nodeID  string // 路由节点ID

	// 统计信息
	stats *RouterStats
}

// RouterConfig 路由服务器配置
type RouterConfig struct {
	// 基础配置
	Host   string
	Port   int
	NodeID string

	// Redis配置
	Redis struct {
		Mode     string   // "single" 或 "cluster"
		Addrs    []string // Redis地址列表
		Password string
		DB       int // 仅单机模式有效
	}

	// 消息配置
	MessageTimeout    time.Duration
	MaxRetries        int
	HeartbeatInterval time.Duration
	NodeTimeout       time.Duration

	// 节点配置
	MaxConnectionNodes int
	MaxMessageCenters  int
	NodeRegistryTTL    time.Duration

	// 负载均衡配置
	LoadBalanceStrategy string // "round_robin", "least_connections", "consistent_hash"
	HealthCheckInterval time.Duration

	// 消息中心配置
	MessageCenterAddrs   []string
	MessageCenterTimeout time.Duration

	// 性能配置
	MaxConnections  int
	ReadBufferSize  int
	WriteBufferSize int
	WorkerPoolSize  int

	// 可靠性配置
	EnableReliability     bool
	FetchTimeout          time.Duration
	PendingMessageTTL     time.Duration
	MessagePersistTimeout time.Duration
}

// RouterStats 路由服务器统计信息
type RouterStats struct {
	// 基础统计
	StartTime     time.Time
	LastHeartbeat time.Time

	// 连接统计
	TotalConnections  int64
	ActiveConnections int64
	ConnectionNodes   int64
	MessageCenters    int64

	// 消息统计
	MessagesReceived  int64
	MessagesRouted    int64
	MessagesPersisted int64
	MessagesDropped   int64
	MessagesRetried   int64
	MessagesFetched   int64

	// 负载均衡统计
	LoadBalanceDecisions int64
	NodeSwitches         int64

	// 房间统计
	ActiveRooms  int64
	RoomMappings int64
}

// NewRouterServer 创建路由服务器
func NewRouterServer(config *RouterConfig) (*RouterServer, error) {
	if config == nil {
		config = DefaultRouterConfig()
	}

	// 生成节点ID
	if config.NodeID == "" {
		config.NodeID = fmt.Sprintf("router_%s_%d_%d", config.Host, config.Port, time.Now().Unix())
	}

	ctx, cancel := context.WithCancel(context.Background())

	rs := &RouterServer{
		config:           config,
		nodeID:           config.NodeID,
		ctx:              ctx,
		cancel:           cancel,
		stats:            &RouterStats{StartTime: time.Now()},
		codec:            message.NewMessageCodec(),
		connSeqGenerator: message.NewConnSeqGenerator(),
	}

	// 初始化Redis
	if err := rs.initRedis(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to init redis: %v", err)
	}

	// 初始化网络服务器
	if err := rs.initServer(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to init server: %v", err)
	}

	// 初始化管理器
	rs.initManagers()

	return rs, nil
}

// DefaultRouterConfig 默认路由配置
func DefaultRouterConfig() *RouterConfig {
	return &RouterConfig{
		Host:   "0.0.0.0",
		Port:   8081,
		NodeID: "",
		Redis: struct {
			Mode     string
			Addrs    []string
			Password string
			DB       int
		}{
			Mode:     "single",
			Addrs:    []string{"localhost:6379"},
			Password: "",
			DB:       0,
		},
		MessageTimeout:        10 * time.Second,
		MaxRetries:            3,
		HeartbeatInterval:     30 * time.Second,
		NodeTimeout:           90 * time.Second,
		MaxConnectionNodes:    100,
		MaxMessageCenters:     10,
		NodeRegistryTTL:       60 * time.Second,
		LoadBalanceStrategy:   "round_robin",
		HealthCheckInterval:   10 * time.Second,
		MessageCenterAddrs:    []string{"ws://localhost:8082"},
		MessageCenterTimeout:  5 * time.Second,
		MaxConnections:        10000,
		ReadBufferSize:        4096,
		WriteBufferSize:       4096,
		WorkerPoolSize:        200,
		EnableReliability:     true,
		FetchTimeout:          5 * time.Second,
		PendingMessageTTL:     5 * time.Minute,
		MessagePersistTimeout: 10 * time.Second,
	}
}

// initRedis 初始化Redis
func (rs *RouterServer) initRedis() error {
	if rs.config.Redis.Mode == "cluster" {
		// Redis集群模式
		rs.redisCluster = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    rs.config.Redis.Addrs,
			Password: rs.config.Redis.Password,
		})

		// 测试集群连接
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := rs.redisCluster.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis cluster connection failed: %v", err)
		}

		log.Printf("Redis cluster connected: %v", rs.config.Redis.Addrs)
	} else {
		// Redis单机模式
		if len(rs.config.Redis.Addrs) == 0 {
			return fmt.Errorf("redis address not configured")
		}

		rs.redis = redis.NewClient(&redis.Options{
			Addr:     rs.config.Redis.Addrs[0],
			Password: rs.config.Redis.Password,
			DB:       rs.config.Redis.DB,
		})

		// 测试连接
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := rs.redis.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis connection failed: %v", err)
		}

		log.Printf("Redis connected: %s", rs.config.Redis.Addrs[0])
	}

	return nil
}

// initServer 初始化网络服务器
func (rs *RouterServer) initServer() error {
	serverConfig := &api.ServerConfig{
		Address:         fmt.Sprintf("%s:%d", rs.config.Host, rs.config.Port),
		MaxConnections:  rs.config.MaxConnections,
		ReadTimeout:     rs.config.MessageTimeout,
		WriteTimeout:    rs.config.MessageTimeout,
		IdleTimeout:     rs.config.HeartbeatInterval * 2,
		ReactorCount:    8, // 使用8个反应器
		WorkerCount:     rs.config.WorkerPoolSize,
		ReadBufferSize:  rs.config.ReadBufferSize,
		WriteBufferSize: rs.config.WriteBufferSize,
	}

	// 创建事件处理器
	eventHandler := NewRouterEventHandler(rs)

	// 创建服务器
	var err error
	rs.server, err = api.NewServer(serverConfig, eventHandler)
	return err
}

// initManagers 初始化管理器
func (rs *RouterServer) initManagers() {
	// 节点管理器
	rs.nodeManager = NewNodeManager(rs, rs.config)

	// Redis管理器
	if rs.redisCluster != nil {
		rs.redisManager = NewRedisManager(rs.redisCluster, nil, rs.config)
	} else {
		rs.redisManager = NewRedisManager(nil, rs.redis, rs.config)
	}

	// 负载均衡器
	rs.loadBalancer = NewLoadBalancer(rs.config.LoadBalanceStrategy, rs.nodeManager)

	// 消息路由器
	rs.messageRouter = NewMessageRouter(rs, rs.config)

	// 可靠性管理器
	if rs.config.EnableReliability {
		rs.reliabilityManager = NewRouterReliabilityManager(rs, rs.config)
	}

	// 消息分类器
	rs.messageClassifier = NewMessageClassifier()
}

// Start 启动路由服务器
func (rs *RouterServer) Start() error {
	if !atomic.CompareAndSwapInt32(&rs.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	log.Printf("Starting RouterServer %s on %s:%d", rs.nodeID, rs.config.Host, rs.config.Port)

	// 启动网络服务器
	if err := rs.server.Start(); err != nil {
		atomic.StoreInt32(&rs.running, 0)
		return fmt.Errorf("failed to start server: %v", err)
	}

	// 启动管理器
	rs.nodeManager.Start(rs.ctx)
	rs.redisManager.Start(rs.ctx)
	rs.loadBalancer.Start(rs.ctx)
	rs.messageRouter.Start(rs.ctx)

	if rs.reliabilityManager != nil {
		rs.reliabilityManager.Start(rs.ctx)
	}

	// 注册节点到Redis
	if err := rs.registerNode(); err != nil {
		log.Printf("Failed to register node: %v", err)
	}

	// 启动后台任务
	rs.wg.Add(1)
	go rs.backgroundTasks()

	log.Printf("RouterServer %s started successfully", rs.nodeID)
	return nil
}

// Stop 停止路由服务器
func (rs *RouterServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&rs.running, 1, 0) {
		return fmt.Errorf("server not running")
	}

	log.Printf("Stopping RouterServer %s...", rs.nodeID)

	// 取消上下文
	rs.cancel()

	// 停止网络服务器
	if rs.server != nil {
		rs.server.Stop()
	}

	// 注销节点
	rs.unregisterNode()

	// 关闭Redis连接
	if rs.redisCluster != nil {
		rs.redisCluster.Close()
	}
	if rs.redis != nil {
		rs.redis.Close()
	}

	// 等待后台任务结束
	rs.wg.Wait()

	log.Printf("RouterServer %s stopped", rs.nodeID)
	return nil
}

// backgroundTasks 后台任务
func (rs *RouterServer) backgroundTasks() {
	defer rs.wg.Done()

	heartbeatTicker := time.NewTicker(rs.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	healthCheckTicker := time.NewTicker(rs.config.HealthCheckInterval)
	defer healthCheckTicker.Stop()

	registryTicker := time.NewTicker(rs.config.NodeRegistryTTL / 2) // 续期频率为TTL的一半
	defer registryTicker.Stop()

	for {
		select {
		case <-rs.ctx.Done():
			return

		case <-heartbeatTicker.C:
			rs.performHeartbeat()

		case <-healthCheckTicker.C:
			rs.performHealthCheck()

		case <-registryTicker.C:
			rs.renewNodeRegistration()
		}
	}
}

// performHeartbeat 执行心跳
func (rs *RouterServer) performHeartbeat() {
	rs.stats.LastHeartbeat = time.Now()

	// 向所有连接节点发送心跳
	rs.nodeManager.SendHeartbeatToConnectionNodes()

	// 向所有消息中心发送心跳
	rs.nodeManager.SendHeartbeatToMessageCenters()

	// 更新统计信息
	atomic.StoreInt64(&rs.stats.ConnectionNodes, int64(rs.nodeManager.GetConnectionNodeCount()))
	atomic.StoreInt64(&rs.stats.MessageCenters, int64(rs.nodeManager.GetMessageCenterCount()))
	atomic.StoreInt64(&rs.stats.ActiveRooms, int64(rs.redisManager.GetActiveRoomCount()))
}

// performHealthCheck 执行健康检查
func (rs *RouterServer) performHealthCheck() {
	// 检查连接节点健康状态
	rs.nodeManager.CheckConnectionNodeHealth()

	// 检查消息中心健康状态
	rs.nodeManager.CheckMessageCenterHealth()

	// 清理不健康的节点
	rs.nodeManager.CleanupUnhealthyNodes()

	// 更新负载均衡器的节点状态
	rs.loadBalancer.UpdateNodeHealth()
}

// renewNodeRegistration 续期节点注册
func (rs *RouterServer) renewNodeRegistration() {
	if err := rs.registerNode(); err != nil {
		log.Printf("Failed to renew node registration: %v", err)
	}
}

// registerNode 注册节点到Redis
func (rs *RouterServer) registerNode() error {
	nodeInfo := map[string]interface{}{
		"node_id":            rs.nodeID,
		"host":               rs.config.Host,
		"port":               rs.config.Port,
		"type":               "router",
		"active_connections": atomic.LoadInt64(&rs.stats.ActiveConnections),
		"messages_processed": atomic.LoadInt64(&rs.stats.MessagesRouted),
		"last_heartbeat":     time.Now().Unix(),
		"version":            "1.0.0",
		"status":             "active",
	}

	return rs.redisManager.RegisterNode(rs.nodeID, nodeInfo, rs.config.NodeRegistryTTL)
}

// unregisterNode 注销节点
func (rs *RouterServer) unregisterNode() {
	if err := rs.redisManager.UnregisterNode(rs.nodeID); err != nil {
		log.Printf("Failed to unregister node: %v", err)
	}
}

// HandleMessage 处理来自连接节点的消息
func (rs *RouterServer) HandleMessage(nodeConn *NodeConnection, data []byte) error {
	// 反序列化消息
	envelope, err := rs.codec.Deserialize(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %v", err)
	}

	// 更新统计信息
	atomic.AddInt64(&rs.stats.MessagesReceived, 1)

	// 委托给消息路由器处理
	return rs.messageRouter.RouteMessage(nodeConn, envelope)
}

// SendMessageToNode 发送消息到指定节点
func (rs *RouterServer) SendMessageToNode(nodeID string, envelope *message.MessageEnvelope) error {
	node := rs.nodeManager.GetConnectionNode(nodeID)
	if node == nil {
		return fmt.Errorf("connection node %s not found", nodeID)
	}

	// 如果启用可靠性，使用可靠性管理器发送
	if rs.reliabilityManager != nil {
		return rs.reliabilityManager.SendReliableMessage(node, envelope)
	}

	// 否则直接发送
	return rs.sendMessageToNodeSync(node, envelope)
}

// sendMessageToNodeSync 同步发送消息到节点
func (rs *RouterServer) sendMessageToNodeSync(node *NodeConnection, envelope *message.MessageEnvelope) error {
	data, err := rs.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	if err := node.Send(data); err != nil {
		atomic.AddInt64(&rs.stats.MessagesDropped, 1)
		return fmt.Errorf("failed to send message: %v", err)
	}

	atomic.AddInt64(&rs.stats.MessagesRouted, 1)
	return nil
}

// BroadcastToRoom 向房间内所有节点广播消息
func (rs *RouterServer) BroadcastToRoom(roomID uint64, envelope *message.MessageEnvelope, excludeNode *NodeConnection) error {
	// 获取房间所在的节点列表
	nodeIDs, err := rs.redisManager.GetRoomNodes(roomID)
	if err != nil {
		return fmt.Errorf("failed to get room nodes: %v", err)
	}

	if len(nodeIDs) == 0 {
		return fmt.Errorf("no nodes found for room %d", roomID)
	}

	// 序列化消息
	data, err := rs.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// 向所有节点发送消息
	sent := 0
	for _, nodeID := range nodeIDs {
		node := rs.nodeManager.GetConnectionNode(nodeID)
		if node == nil {
			continue
		}

		if excludeNode != nil && node.GetID() == excludeNode.GetID() {
			continue // 排除指定节点
		}

		if err := node.Send(data); err != nil {
			log.Printf("Failed to send message to node %s: %v", nodeID, err)
			atomic.AddInt64(&rs.stats.MessagesDropped, 1)
		} else {
			sent++
		}
	}

	atomic.AddInt64(&rs.stats.MessagesRouted, int64(sent))
	return nil
}

// PersistMessage 持久化消息到消息中心
func (rs *RouterServer) PersistMessage(envelope *message.MessageEnvelope) error {
	// 使用消息分类器判断是否需要持久化到Kafka
	if !rs.messageClassifier.ShouldPersistToKafka(envelope) {
		log.Printf("Message classified as system message, skipping Kafka persistence: %T", envelope.Message)
		return nil
	}

	// 选择消息中心节点
	messageCenter := rs.loadBalancer.SelectMessageCenter()
	if messageCenter == nil {
		return fmt.Errorf("no available message center")
	}

	// 记录分类信息（调试模式）
	if log.Default().Writer() != nil {
		rs.messageClassifier.LogClassification(envelope)
	}

	// 如果启用可靠性，使用可靠性管理器发送
	if rs.reliabilityManager != nil {
		return rs.reliabilityManager.SendReliableMessageToCenter(messageCenter, envelope)
	}

	// 否则直接发送
	return rs.sendMessageToCenterSync(messageCenter, envelope)
}

// sendMessageToCenterSync 同步发送消息到消息中心
func (rs *RouterServer) sendMessageToCenterSync(center *MessageCenterConnection, envelope *message.MessageEnvelope) error {
	data, err := rs.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	if err := center.Send(data); err != nil {
		atomic.AddInt64(&rs.stats.MessagesDropped, 1)
		return fmt.Errorf("failed to send message to message center: %v", err)
	}

	atomic.AddInt64(&rs.stats.MessagesPersisted, 1)
	return nil
}

// GetStats 获取统计信息
func (rs *RouterServer) GetStats() *RouterStats {
	stats := *rs.stats
	stats.ActiveConnections = atomic.LoadInt64(&rs.stats.ActiveConnections)
	stats.ConnectionNodes = atomic.LoadInt64(&rs.stats.ConnectionNodes)
	stats.MessageCenters = atomic.LoadInt64(&rs.stats.MessageCenters)
	stats.ActiveRooms = atomic.LoadInt64(&rs.stats.ActiveRooms)
	return &stats
}

// IsRunning 检查服务器是否运行中
func (rs *RouterServer) IsRunning() bool {
	return atomic.LoadInt32(&rs.running) == 1
}

// GetNodeID 获取节点ID
func (rs *RouterServer) GetNodeID() string {
	return rs.nodeID
}

// GetNodeManager 获取节点管理器
func (rs *RouterServer) GetNodeManager() *NodeManager {
	return rs.nodeManager
}

// GetMessageRouter 获取消息路由器
func (rs *RouterServer) GetMessageRouter() *MessageRouter {
	return rs.messageRouter
}

// GetLoadBalancer 获取负载均衡器
func (rs *RouterServer) GetLoadBalancer() *LoadBalancer {
	return rs.loadBalancer
}

// GetRedisManager 获取Redis管理器
func (rs *RouterServer) GetRedisManager() *RedisManager {
	return rs.redisManager
}
