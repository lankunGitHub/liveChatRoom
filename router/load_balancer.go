package router

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"sort"
	"sync"
	"sync/atomic"
)

// LoadBalancer 负载均衡器 - 为消息分发选择最佳节点
type LoadBalancer struct {
	strategy    string
	nodeManager *NodeManager

	// 轮询策略状态
	roundRobinIndex int32

	// 一致性哈希状态
	hashRing *ConsistentHashRing

	// 统计信息
	decisions    int64
	nodeSwitches int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	mutex  sync.RWMutex
}

// ConsistentHashRing 一致性哈希环
type ConsistentHashRing struct {
	nodes    map[uint32]string // hash -> nodeID
	keys     []uint32          // 排序的hash值
	replicas int               // 虚拟节点数量
	mutex    sync.RWMutex
}

// NewLoadBalancer 创建负载均衡器
func NewLoadBalancer(strategy string, nodeManager *NodeManager) *LoadBalancer {
	lb := &LoadBalancer{
		strategy:    strategy,
		nodeManager: nodeManager,
		hashRing:    NewConsistentHashRing(100), // 100个虚拟节点
	}

	return lb
}

// NewConsistentHashRing 创建一致性哈希环
func NewConsistentHashRing(replicas int) *ConsistentHashRing {
	return &ConsistentHashRing{
		nodes:    make(map[uint32]string),
		keys:     make([]uint32, 0),
		replicas: replicas,
	}
}

// Start 启动负载均衡器
func (lb *LoadBalancer) Start(ctx context.Context) {
	lb.ctx, lb.cancel = context.WithCancel(ctx)

	// 初始化一致性哈希环
	if lb.strategy == "consistent_hash" {
		lb.updateHashRing()
	}

	log.Printf("LoadBalancer started with strategy: %s", lb.strategy)
}

// Stop 停止负载均衡器
func (lb *LoadBalancer) Stop() {
	if lb.cancel != nil {
		lb.cancel()
	}
	log.Printf("LoadBalancer stopped")
}

// SelectConnectionNode 选择连接节点
func (lb *LoadBalancer) SelectConnectionNode(key string) *NodeConnection {
	atomic.AddInt64(&lb.decisions, 1)

	nodes := lb.nodeManager.GetHealthyConnectionNodes()
	if len(nodes) == 0 {
		return nil
	}

	switch lb.strategy {
	case "round_robin":
		return lb.selectByRoundRobin(nodes)
	case "least_connections":
		return lb.selectByLeastConnections(nodes)
	case "consistent_hash":
		return lb.selectByConsistentHash(nodes, key)
	default:
		// 默认使用轮询
		return lb.selectByRoundRobin(nodes)
	}
}

// SelectMessageCenter 选择消息中心
func (lb *LoadBalancer) SelectMessageCenter() *MessageCenterConnection {
	atomic.AddInt64(&lb.decisions, 1)

	centers := lb.nodeManager.GetHealthyMessageCenters()
	if len(centers) == 0 {
		return nil
	}

	switch lb.strategy {
	case "round_robin":
		return lb.selectMessageCenterByRoundRobin(centers)
	case "least_connections":
		return lb.selectMessageCenterByLeastConnections(centers)
	case "consistent_hash":
		// 消息中心选择使用轮询，因为没有特定的key
		return lb.selectMessageCenterByRoundRobin(centers)
	default:
		return lb.selectMessageCenterByRoundRobin(centers)
	}
}

// selectByRoundRobin 轮询选择连接节点
func (lb *LoadBalancer) selectByRoundRobin(nodes []*NodeConnection) *NodeConnection {
	if len(nodes) == 0 {
		return nil
	}

	index := atomic.AddInt32(&lb.roundRobinIndex, 1)
	return nodes[int(index)%len(nodes)]
}

// selectByLeastConnections 最少连接选择连接节点
func (lb *LoadBalancer) selectByLeastConnections(nodes []*NodeConnection) *NodeConnection {
	if len(nodes) == 0 {
		return nil
	}

	var bestNode *NodeConnection
	minConnections := int32(^uint32(0) >> 1) // 最大int32值

	for _, node := range nodes {
		connections := node.GetActiveConnections()
		if connections < minConnections {
			minConnections = connections
			bestNode = node
		}
	}

	return bestNode
}

// selectByConsistentHash 一致性哈希选择连接节点
func (lb *LoadBalancer) selectByConsistentHash(nodes []*NodeConnection, key string) *NodeConnection {
	if len(nodes) == 0 {
		return nil
	}

	lb.hashRing.mutex.RLock()
	defer lb.hashRing.mutex.RUnlock()

	if len(lb.hashRing.keys) == 0 {
		// 哈希环为空，使用轮询作为后备
		return lb.selectByRoundRobin(nodes)
	}

	hash := lb.hash(key)
	idx := sort.Search(len(lb.hashRing.keys), func(i int) bool {
		return lb.hashRing.keys[i] >= hash
	})

	if idx == len(lb.hashRing.keys) {
		idx = 0
	}

	nodeID := lb.hashRing.nodes[lb.hashRing.keys[idx]]

	// 查找对应的节点
	for _, node := range nodes {
		if node.GetID() == nodeID {
			return node
		}
	}

	// 节点不存在，使用轮询作为后备
	return lb.selectByRoundRobin(nodes)
}

// selectMessageCenterByRoundRobin 轮询选择消息中心
func (lb *LoadBalancer) selectMessageCenterByRoundRobin(centers []*MessageCenterConnection) *MessageCenterConnection {
	if len(centers) == 0 {
		return nil
	}

	index := atomic.AddInt32(&lb.roundRobinIndex, 1)
	return centers[int(index)%len(centers)]
}

// selectMessageCenterByLeastConnections 最少连接选择消息中心
func (lb *LoadBalancer) selectMessageCenterByLeastConnections(centers []*MessageCenterConnection) *MessageCenterConnection {
	if len(centers) == 0 {
		return nil
	}

	var bestCenter *MessageCenterConnection
	minQueueDepth := int64(^uint64(0) >> 1) // 最大int64值

	for _, center := range centers {
		queueDepth := center.GetQueueDepth()
		if queueDepth < minQueueDepth {
			minQueueDepth = queueDepth
			bestCenter = center
		}
	}

	return bestCenter
}

// UpdateNodeHealth 更新节点健康状态（重新构建哈希环）
func (lb *LoadBalancer) UpdateNodeHealth() {
	if lb.strategy == "consistent_hash" {
		lb.updateHashRing()
	}
}

// updateHashRing 更新一致性哈希环
func (lb *LoadBalancer) updateHashRing() {
	nodes := lb.nodeManager.GetHealthyConnectionNodes()

	lb.hashRing.mutex.Lock()
	defer lb.hashRing.mutex.Unlock()

	// 清空现有哈希环
	lb.hashRing.nodes = make(map[uint32]string)
	lb.hashRing.keys = make([]uint32, 0)

	// 为每个健康节点添加虚拟节点
	for _, node := range nodes {
		lb.addNodeToRing(node.GetID())
	}

	// 排序哈希值
	sort.Slice(lb.hashRing.keys, func(i, j int) bool {
		return lb.hashRing.keys[i] < lb.hashRing.keys[j]
	})

	log.Printf("Updated consistent hash ring with %d nodes, %d virtual nodes",
		len(nodes), len(lb.hashRing.keys))
}

// addNodeToRing 添加节点到哈希环
func (lb *LoadBalancer) addNodeToRing(nodeID string) {
	for i := 0; i < lb.hashRing.replicas; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeID, i)
		hash := lb.hash(virtualKey)
		lb.hashRing.nodes[hash] = nodeID
		lb.hashRing.keys = append(lb.hashRing.keys, hash)
	}
}

// removeNodeFromRing 从哈希环移除节点
func (lb *LoadBalancer) removeNodeFromRing(nodeID string) {
	for i := 0; i < lb.hashRing.replicas; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeID, i)
		hash := lb.hash(virtualKey)
		delete(lb.hashRing.nodes, hash)

		// 从keys中移除
		for j, key := range lb.hashRing.keys {
			if key == hash {
				lb.hashRing.keys = append(lb.hashRing.keys[:j], lb.hashRing.keys[j+1:]...)
				break
			}
		}
	}
}

// hash 计算字符串哈希值
func (lb *LoadBalancer) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// GetNodeLoad 获取节点负载信息
func (lb *LoadBalancer) GetNodeLoad(nodeID string) map[string]interface{} {
	node := lb.nodeManager.GetConnectionNode(nodeID)
	if node == nil {
		return nil
	}

	activeConns, cpuUsage, memoryUsage := node.GetLoadInfo()

	return map[string]interface{}{
		"node_id":            nodeID,
		"active_connections": activeConns,
		"cpu_usage":          cpuUsage,
		"memory_usage":       memoryUsage,
		"load_score":         node.GetLoadScore(),
		"is_healthy":         node.IsHealthy(),
	}
}

// GetAllNodeLoads 获取所有节点负载信息
func (lb *LoadBalancer) GetAllNodeLoads() []map[string]interface{} {
	nodes := lb.nodeManager.GetAllConnectionNodes()
	loads := make([]map[string]interface{}, len(nodes))

	for i, node := range nodes {
		loads[i] = lb.GetNodeLoad(node.GetID())
	}

	return loads
}

// GetLoadBalanceStats 获取负载均衡统计信息
func (lb *LoadBalancer) GetLoadBalanceStats() map[string]interface{} {
	lb.mutex.RLock()
	defer lb.mutex.RUnlock()

	stats := map[string]interface{}{
		"strategy":        lb.strategy,
		"decisions":       atomic.LoadInt64(&lb.decisions),
		"node_switches":   atomic.LoadInt64(&lb.nodeSwitches),
		"healthy_nodes":   len(lb.nodeManager.GetHealthyConnectionNodes()),
		"healthy_centers": len(lb.nodeManager.GetHealthyMessageCenters()),
	}

	if lb.strategy == "consistent_hash" {
		lb.hashRing.mutex.RLock()
		stats["hash_ring_size"] = len(lb.hashRing.keys)
		stats["hash_ring_nodes"] = len(lb.hashRing.nodes)
		lb.hashRing.mutex.RUnlock()
	}

	return stats
}

// SetStrategy 设置负载均衡策略
func (lb *LoadBalancer) SetStrategy(strategy string) error {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	validStrategies := map[string]bool{
		"round_robin":       true,
		"least_connections": true,
		"consistent_hash":   true,
	}

	if !validStrategies[strategy] {
		return fmt.Errorf("invalid load balance strategy: %s", strategy)
	}

	oldStrategy := lb.strategy
	lb.strategy = strategy

	// 如果切换到一致性哈希，需要构建哈希环
	if strategy == "consistent_hash" && oldStrategy != "consistent_hash" {
		lb.updateHashRing()
	}

	atomic.AddInt64(&lb.nodeSwitches, 1)
	log.Printf("Load balance strategy changed from %s to %s", oldStrategy, strategy)

	return nil
}

// SelectNodeForRoom 为房间选择节点（考虑房间亲和性）
func (lb *LoadBalancer) SelectNodeForRoom(roomID uint64) *NodeConnection {
	// 为房间选择节点时，使用房间ID作为一致性哈希的key
	// 这样同一个房间的消息会倾向于路由到同一个节点
	roomKey := fmt.Sprintf("room_%d", roomID)
	return lb.SelectConnectionNode(roomKey)
}

// SelectNodeForUser 为用户选择节点（考虑用户亲和性）
func (lb *LoadBalancer) SelectNodeForUser(userID uint32) *NodeConnection {
	// 为用户选择节点时，使用用户ID作为一致性哈希的key
	// 这样同一个用户的连接会倾向于路由到同一个节点
	userKey := fmt.Sprintf("user_%d", userID)
	return lb.SelectConnectionNode(userKey)
}

// Rebalance 重新平衡负载
func (lb *LoadBalancer) Rebalance() {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	if lb.strategy == "consistent_hash" {
		lb.updateHashRing()
	}

	// 重置轮询索引
	atomic.StoreInt32(&lb.roundRobinIndex, 0)

	log.Printf("Load balancer rebalanced")
}
