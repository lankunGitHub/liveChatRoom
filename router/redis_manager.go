package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisManager Redis管理器 - 负责Redis集群操作和数据管理
type RedisManager struct {
	redisCluster *redis.ClusterClient
	redis        *redis.Client
	config       *RouterConfig

	// 缓存
	roomMappingCache map[uint64][]string
	nodeInfoCache    map[string]map[string]interface{}
	cacheMutex       sync.RWMutex
	cacheExpiry      time.Duration

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRedisManager 创建Redis管理器
func NewRedisManager(cluster *redis.ClusterClient, single *redis.Client, config *RouterConfig) *RedisManager {
	return &RedisManager{
		redisCluster:     cluster,
		redis:            single,
		config:           config,
		roomMappingCache: make(map[uint64][]string),
		nodeInfoCache:    make(map[string]map[string]interface{}),
		cacheExpiry:      30 * time.Second, // 缓存30秒
	}
}

// Start 启动Redis管理器
func (rm *RedisManager) Start(ctx context.Context) {
	rm.ctx, rm.cancel = context.WithCancel(ctx)

	// 启动缓存清理协程
	rm.wg.Add(1)
	go rm.cacheCleanupLoop()

	log.Printf("RedisManager started")
}

// Stop 停止Redis管理器
func (rm *RedisManager) Stop() {
	if rm.cancel != nil {
		rm.cancel()
	}
	rm.wg.Wait()

	log.Printf("RedisManager stopped")
}

// getClient 获取Redis客户端
func (rm *RedisManager) getClient() redis.Cmdable {
	if rm.redisCluster != nil {
		return rm.redisCluster
	}
	return rm.redis
}

// 节点管理

// RegisterNode 注册节点
func (rm *RedisManager) RegisterNode(nodeID string, nodeInfo map[string]interface{}, ttl time.Duration) error {
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 序列化节点信息
	data, err := json.Marshal(nodeInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %v", err)
	}

	// 存储节点信息
	nodeKey := fmt.Sprintf("nodes:%s", nodeID)
	if err := client.Set(ctx, nodeKey, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to register node: %v", err)
	}

	// 添加到节点列表
	nodeType, _ := nodeInfo["type"].(string)
	if nodeType != "" {
		listKey := fmt.Sprintf("node_list:%s", nodeType)
		client.SAdd(ctx, listKey, nodeID)
		client.Expire(ctx, listKey, ttl*2) // 列表的TTL是节点TTL的2倍
	}

	// 更新缓存
	rm.cacheMutex.Lock()
	rm.nodeInfoCache[nodeID] = nodeInfo
	rm.cacheMutex.Unlock()

	return nil
}

// UnregisterNode 注销节点
func (rm *RedisManager) UnregisterNode(nodeID string) error {
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取节点信息
	nodeInfo, err := rm.GetNodeInfo(nodeID)
	if err == nil {
		// 从节点列表中移除
		if nodeType, exists := nodeInfo["type"].(string); exists {
			listKey := fmt.Sprintf("node_list:%s", nodeType)
			client.SRem(ctx, listKey, nodeID)
		}
	}

	// 删除节点信息
	nodeKey := fmt.Sprintf("nodes:%s", nodeID)
	if err := client.Del(ctx, nodeKey).Err(); err != nil {
		return fmt.Errorf("failed to unregister node: %v", err)
	}

	// 清理缓存
	rm.cacheMutex.Lock()
	delete(rm.nodeInfoCache, nodeID)
	rm.cacheMutex.Unlock()

	return nil
}

// GetNodeInfo 获取节点信息
func (rm *RedisManager) GetNodeInfo(nodeID string) (map[string]interface{}, error) {
	// 先检查缓存
	rm.cacheMutex.RLock()
	if info, exists := rm.nodeInfoCache[nodeID]; exists {
		rm.cacheMutex.RUnlock()
		return info, nil
	}
	rm.cacheMutex.RUnlock()

	// 从Redis获取
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodeKey := fmt.Sprintf("nodes:%s", nodeID)
	data, err := client.Get(ctx, nodeKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("node %s not found", nodeID)
		}
		return nil, fmt.Errorf("failed to get node info: %v", err)
	}

	var nodeInfo map[string]interface{}
	if err := json.Unmarshal(data, &nodeInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node info: %v", err)
	}

	// 更新缓存
	rm.cacheMutex.Lock()
	rm.nodeInfoCache[nodeID] = nodeInfo
	rm.cacheMutex.Unlock()

	return nodeInfo, nil
}

// GetNodesByType 根据类型获取节点列表
func (rm *RedisManager) GetNodesByType(nodeType string) ([]string, error) {
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	listKey := fmt.Sprintf("node_list:%s", nodeType)
	nodeIDs, err := client.SMembers(ctx, listKey).Result()
	if err != nil {
		if err == redis.Nil {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to get nodes by type: %v", err)
	}

	return nodeIDs, nil
}

// 房间映射管理

// RegisterRoomMapping 注册房间映射
func (rm *RedisManager) RegisterRoomMapping(roomID uint64, nodeID string) error {
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 房间到节点的映射
	roomKey := fmt.Sprintf("room_nodes:%d", roomID)
	if err := client.SAdd(ctx, roomKey, nodeID).Err(); err != nil {
		return fmt.Errorf("failed to register room mapping: %v", err)
	}

	// 设置过期时间
	client.Expire(ctx, roomKey, 24*time.Hour)

	// 节点到房间的映射
	nodeKey := fmt.Sprintf("node_rooms:%s", nodeID)
	client.SAdd(ctx, nodeKey, fmt.Sprintf("%d", roomID))
	client.Expire(ctx, nodeKey, 24*time.Hour)

	// 更新缓存
	rm.cacheMutex.Lock()
	if nodes, exists := rm.roomMappingCache[roomID]; exists {
		// 检查是否已存在
		found := false
		for _, n := range nodes {
			if n == nodeID {
				found = true
				break
			}
		}
		if !found {
			rm.roomMappingCache[roomID] = append(nodes, nodeID)
		}
	} else {
		rm.roomMappingCache[roomID] = []string{nodeID}
	}
	rm.cacheMutex.Unlock()

	return nil
}

// AddRoomNode 为房间添加节点
func (rm *RedisManager) AddRoomNode(roomID uint64, nodeID string) error {
	return rm.RegisterRoomMapping(roomID, nodeID)
}

// RemoveRoomNode 从房间移除节点
func (rm *RedisManager) RemoveRoomNode(roomID uint64, nodeID string) error {
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 从房间节点集合中移除
	roomKey := fmt.Sprintf("room_nodes:%d", roomID)
	if err := client.SRem(ctx, roomKey, nodeID).Err(); err != nil {
		return fmt.Errorf("failed to remove room node: %v", err)
	}

	// 从节点房间集合中移除
	nodeKey := fmt.Sprintf("node_rooms:%s", nodeID)
	client.SRem(ctx, nodeKey, fmt.Sprintf("%d", roomID))

	// 更新缓存
	rm.cacheMutex.Lock()
	if nodes, exists := rm.roomMappingCache[roomID]; exists {
		for i, n := range nodes {
			if n == nodeID {
				rm.roomMappingCache[roomID] = append(nodes[:i], nodes[i+1:]...)
				break
			}
		}
	}
	rm.cacheMutex.Unlock()

	return nil
}

// GetRoomNodes 获取房间所在的节点列表
func (rm *RedisManager) GetRoomNodes(roomID uint64) ([]string, error) {
	// 先检查缓存
	rm.cacheMutex.RLock()
	if nodes, exists := rm.roomMappingCache[roomID]; exists {
		rm.cacheMutex.RUnlock()
		return nodes, nil
	}
	rm.cacheMutex.RUnlock()

	// 从Redis获取
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	roomKey := fmt.Sprintf("room_nodes:%d", roomID)
	nodeIDs, err := client.SMembers(ctx, roomKey).Result()
	if err != nil {
		if err == redis.Nil {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to get room nodes: %v", err)
	}

	// 更新缓存
	rm.cacheMutex.Lock()
	rm.roomMappingCache[roomID] = nodeIDs
	rm.cacheMutex.Unlock()

	return nodeIDs, nil
}

// GetNodeRooms 获取节点管理的房间列表
func (rm *RedisManager) GetNodeRooms(nodeID string) ([]uint64, error) {
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodeKey := fmt.Sprintf("node_rooms:%s", nodeID)
	roomStrs, err := client.SMembers(ctx, nodeKey).Result()
	if err != nil {
		if err == redis.Nil {
			return []uint64{}, nil
		}
		return nil, fmt.Errorf("failed to get node rooms: %v", err)
	}

	roomIDs := make([]uint64, 0, len(roomStrs))
	for _, roomStr := range roomStrs {
		var roomID uint64
		if _, err := fmt.Sscanf(roomStr, "%d", &roomID); err == nil {
			roomIDs = append(roomIDs, roomID)
		}
	}

	return roomIDs, nil
}

// RemoveRoomMapping 移除房间映射
func (rm *RedisManager) RemoveRoomMapping(roomID uint64) error {
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 获取房间的所有节点
	nodeIDs, err := rm.GetRoomNodes(roomID)
	if err != nil {
		return err
	}

	// 从每个节点的房间集合中移除该房间
	for _, nodeID := range nodeIDs {
		nodeKey := fmt.Sprintf("node_rooms:%s", nodeID)
		client.SRem(ctx, nodeKey, fmt.Sprintf("%d", roomID))
	}

	// 删除房间节点映射
	roomKey := fmt.Sprintf("room_nodes:%d", roomID)
	if err := client.Del(ctx, roomKey).Err(); err != nil {
		return fmt.Errorf("failed to remove room mapping: %v", err)
	}

	// 清理缓存
	rm.cacheMutex.Lock()
	delete(rm.roomMappingCache, roomID)
	rm.cacheMutex.Unlock()

	return nil
}

// PreRegisterRoom 预注册房间（分配ID时）
func (rm *RedisManager) PreRegisterRoom(roomID uint64, nodeID string) error {
	client := rm.getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 预注册信息，有较短的TTL
	preRegKey := fmt.Sprintf("room_prereg:%d", roomID)
	preRegInfo := map[string]interface{}{
		"node_id":    nodeID,
		"pre_reg_at": time.Now().Unix(),
		"status":     "allocated",
	}

	data, err := json.Marshal(preRegInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal pre-registration info: %v", err)
	}

	if err := client.Set(ctx, preRegKey, data, 10*time.Minute).Err(); err != nil {
		return fmt.Errorf("failed to pre-register room: %v", err)
	}

	return nil
}

// 统计和监控

// GetActiveRoomCount 获取活跃房间数量
func (rm *RedisManager) GetActiveRoomCount() int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 使用模式匹配获取所有房间键
	pattern := "room_nodes:*"
	var count int

	if rm.redisCluster != nil {
		// 集群模式需要遍历所有节点
		err := rm.redisCluster.ForEachSlave(ctx, func(ctx context.Context, client *redis.Client) error {
			keys, err := client.Keys(ctx, pattern).Result()
			if err != nil {
				return err
			}
			count += len(keys)
			return nil
		})
		if err != nil {
			log.Printf("Failed to get active room count: %v", err)
			return 0
		}
	} else {
		// 单机模式
		keys, err := rm.redis.Keys(ctx, pattern).Result()
		if err != nil {
			log.Printf("Failed to get active room count: %v", err)
			return 0
		}
		count = len(keys)
	}

	return count
}

// GetRedisStats 获取Redis统计信息
func (rm *RedisManager) GetRedisStats() map[string]interface{} {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stats := map[string]interface{}{
		"mode": "single",
	}

	if rm.redisCluster != nil {
		stats["mode"] = "cluster"
		// 集群统计
		clusterInfo, err := rm.redisCluster.ClusterInfo(ctx).Result()
		if err == nil {
			stats["cluster_info"] = clusterInfo
		}
	} else {
		// 单机统计
		info, err := rm.redis.Info(ctx).Result()
		if err == nil {
			stats["info"] = info
		}
	}

	// 缓存统计
	rm.cacheMutex.RLock()
	stats["cache_room_mappings"] = len(rm.roomMappingCache)
	stats["cache_node_info"] = len(rm.nodeInfoCache)
	rm.cacheMutex.RUnlock()

	stats["active_rooms"] = rm.GetActiveRoomCount()

	return stats
}

// 缓存管理

// cacheCleanupLoop 缓存清理循环
func (rm *RedisManager) cacheCleanupLoop() {
	defer rm.wg.Done()

	ticker := time.NewTicker(rm.cacheExpiry)
	defer ticker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-ticker.C:
			rm.cleanupCache()
		}
	}
}

// cleanupCache 清理过期缓存
func (rm *RedisManager) cleanupCache() {
	rm.cacheMutex.Lock()
	defer rm.cacheMutex.Unlock()

	// 简单的全量清理策略
	// 在生产环境中可以实现更精细的TTL管理
	rm.roomMappingCache = make(map[uint64][]string)
	rm.nodeInfoCache = make(map[string]map[string]interface{})

	log.Printf("Redis cache cleaned up")
}

// ClearCache 手动清理缓存
func (rm *RedisManager) ClearCache() {
	rm.cleanupCache()
}

// SetCacheExpiry 设置缓存过期时间
func (rm *RedisManager) SetCacheExpiry(expiry time.Duration) {
	rm.cacheExpiry = expiry
}
