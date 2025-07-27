package connection

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// RoomManager 房间管理器
type RoomManager struct {
	// 房间存储
	rooms     map[uint64]*Room
	roomMutex sync.RWMutex

	// Redis客户端
	redis *redis.Client

	// 配置
	maxRooms        int
	cleanupInterval time.Duration

	// 统计信息
	totalRooms  int64
	activeRooms int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Room 房间信息
type Room struct {
	// 基础信息
	ID         uint64    `json:"id"`
	Name       string    `json:"name"`
	CreatorID  uint32    `json:"creator_id"`
	MaxMembers uint32    `json:"max_members"`
	Password   string    `json:"password,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`

	// 状态信息
	Status       RoomStatus `json:"status"`
	MemberCount  uint32     `json:"member_count"`
	LastActivity time.Time  `json:"last_activity"`

	// 成员管理
	members map[string]bool // connID -> true
	mutex   sync.RWMutex
}

// RoomStatus 房间状态
type RoomStatus int

const (
	RoomStatusOpen   RoomStatus = 0 // 开放
	RoomStatusClosed RoomStatus = 1 // 关闭
	RoomStatusFull   RoomStatus = 2 // 满员
)

// NewRoomManager 创建房间管理器
func NewRoomManager(maxRooms int, redis *redis.Client) *RoomManager {
	return &RoomManager{
		rooms:           make(map[uint64]*Room),
		redis:           redis,
		maxRooms:        maxRooms,
		cleanupInterval: 5 * time.Minute,
	}
}

// Start 启动房间管理器
func (rm *RoomManager) Start(ctx context.Context) {
	rm.ctx, rm.cancel = context.WithCancel(ctx)

	// 加载现有房间
	rm.loadRoomsFromRedis()

	// 启动清理协程
	rm.wg.Add(1)
	go rm.cleanupLoop()

	log.Printf("RoomManager started with %d rooms", len(rm.rooms))
}

// Stop 停止房间管理器
func (rm *RoomManager) Stop() {
	if rm.cancel != nil {
		rm.cancel()
	}
	rm.wg.Wait()

	// 保存房间状态到Redis
	rm.saveRoomsToRedis()

	log.Printf("RoomManager stopped")
}

// CreateRoom 创建房间
func (rm *RoomManager) CreateRoom(roomID uint64, name string, creatorID uint32, maxMembers uint32, password string) (*Room, error) {
	rm.roomMutex.Lock()
	defer rm.roomMutex.Unlock()

	// 检查房间是否已存在
	if _, exists := rm.rooms[roomID]; exists {
		return nil, fmt.Errorf("room %d already exists", roomID)
	}

	// 检查房间数量限制
	if len(rm.rooms) >= rm.maxRooms {
		return nil, fmt.Errorf("max rooms reached: %d", rm.maxRooms)
	}

	// 创建房间
	now := time.Now()
	room := &Room{
		ID:           roomID,
		Name:         name,
		CreatorID:    creatorID,
		MaxMembers:   maxMembers,
		Password:     password,
		CreatedAt:    now,
		UpdatedAt:    now,
		Status:       RoomStatusOpen,
		MemberCount:  0,
		LastActivity: now,
		members:      make(map[string]bool),
	}

	rm.rooms[roomID] = room
	atomic.AddInt64(&rm.totalRooms, 1)
	atomic.AddInt64(&rm.activeRooms, 1)

	// 保存到Redis
	rm.saveRoomToRedis(room)

	log.Printf("Room created: %d (%s) by user %d", roomID, name, creatorID)
	return room, nil
}

// GetRoom 获取房间
func (rm *RoomManager) GetRoom(roomID uint64) *Room {
	rm.roomMutex.RLock()
	defer rm.roomMutex.RUnlock()

	room, exists := rm.rooms[roomID]
	if !exists {
		// 尝试从Redis加载
		if loadedRoom := rm.loadRoomFromRedis(roomID); loadedRoom != nil {
			rm.rooms[roomID] = loadedRoom
			return loadedRoom
		}
		return nil
	}

	return room
}

// JoinRoom 加入房间
func (rm *RoomManager) JoinRoom(roomID uint64, connID string, password string) error {
	room := rm.GetRoom(roomID)
	if room == nil {
		return fmt.Errorf("room %d not found", roomID)
	}

	room.mutex.Lock()
	defer room.mutex.Unlock()

	// 检查房间状态
	if room.Status == RoomStatusClosed {
		return fmt.Errorf("room %d is closed", roomID)
	}

	if room.Status == RoomStatusFull {
		return fmt.Errorf("room %d is full", roomID)
	}

	// 检查密码
	if room.Password != "" && room.Password != password {
		return fmt.Errorf("invalid password for room %d", roomID)
	}

	// 检查是否已在房间中
	if room.members[connID] {
		return fmt.Errorf("connection %s already in room %d", connID, roomID)
	}

	// 检查房间容量
	if room.MemberCount >= room.MaxMembers {
		room.Status = RoomStatusFull
		return fmt.Errorf("room %d is full (%d/%d)", roomID, room.MemberCount, room.MaxMembers)
	}

	// 加入房间
	room.members[connID] = true
	room.MemberCount++
	room.LastActivity = time.Now()
	room.UpdatedAt = time.Now()

	// 检查是否满员
	if room.MemberCount >= room.MaxMembers {
		room.Status = RoomStatusFull
	}

	// 更新Redis
	rm.saveRoomToRedis(room)

	log.Printf("Connection %s joined room %d (%d/%d members)", connID, roomID, room.MemberCount, room.MaxMembers)
	return nil
}

// LeaveRoom 离开房间
func (rm *RoomManager) LeaveRoom(roomID uint64, connID string) error {
	room := rm.GetRoom(roomID)
	if room == nil {
		return fmt.Errorf("room %d not found", roomID)
	}

	room.mutex.Lock()
	defer room.mutex.Unlock()

	// 检查是否在房间中
	if !room.members[connID] {
		return fmt.Errorf("connection %s not in room %d", connID, roomID)
	}

	// 离开房间
	delete(room.members, connID)
	room.MemberCount--
	room.LastActivity = time.Now()
	room.UpdatedAt = time.Now()

	// 更新房间状态
	if room.Status == RoomStatusFull && room.MemberCount < room.MaxMembers {
		room.Status = RoomStatusOpen
	}

	// 更新Redis
	rm.saveRoomToRedis(room)

	log.Printf("Connection %s left room %d (%d/%d members)", connID, roomID, room.MemberCount, room.MaxMembers)
	return nil
}

// CloseRoom 关闭房间
func (rm *RoomManager) CloseRoom(roomID uint64, creatorID uint32) error {
	room := rm.GetRoom(roomID)
	if room == nil {
		return fmt.Errorf("room %d not found", roomID)
	}

	room.mutex.Lock()
	defer room.mutex.Unlock()

	// 检查权限
	if room.CreatorID != creatorID {
		return fmt.Errorf("user %d is not the creator of room %d", creatorID, roomID)
	}

	// 关闭房间
	room.Status = RoomStatusClosed
	room.UpdatedAt = time.Now()

	// 清空成员（但不立即删除房间，等待清理任务处理）
	memberCount := len(room.members)
	room.members = make(map[string]bool)
	room.MemberCount = 0

	// 更新Redis
	rm.saveRoomToRedis(room)

	log.Printf("Room %d closed by creator %d (%d members kicked)", roomID, creatorID, memberCount)
	return nil
}

// GetRoomMembers 获取房间成员列表
func (rm *RoomManager) GetRoomMembers(roomID uint64) []string {
	room := rm.GetRoom(roomID)
	if room == nil {
		return nil
	}

	room.mutex.RLock()
	defer room.mutex.RUnlock()

	members := make([]string, 0, len(room.members))
	for connID := range room.members {
		members = append(members, connID)
	}

	return members
}

// GetUserRooms 获取连接所在的房间列表
func (rm *RoomManager) GetUserRooms(connID string) []*Room {
	rm.roomMutex.RLock()
	defer rm.roomMutex.RUnlock()

	rooms := make([]*Room, 0)
	for _, room := range rm.rooms {
		room.mutex.RLock()
		if room.members[connID] {
			rooms = append(rooms, room)
		}
		room.mutex.RUnlock()
	}

	return rooms
}

// CleanupEmptyRooms 清理空房间
func (rm *RoomManager) CleanupEmptyRooms() {
	rm.roomMutex.Lock()
	defer rm.roomMutex.Unlock()

	emptyRooms := make([]uint64, 0)
	for roomID, room := range rm.rooms {
		room.mutex.RLock()
		isEmpty := room.MemberCount == 0
		isClosed := room.Status == RoomStatusClosed
		isOld := time.Since(room.LastActivity) > 10*time.Minute
		room.mutex.RUnlock()

		if (isEmpty && isOld) || isClosed {
			emptyRooms = append(emptyRooms, roomID)
		}
	}

	for _, roomID := range emptyRooms {
		delete(rm.rooms, roomID)
		atomic.AddInt64(&rm.activeRooms, -1)

		// 从Redis删除
		rm.deleteRoomFromRedis(roomID)
	}

	if len(emptyRooms) > 0 {
		log.Printf("Cleaned up %d empty/closed rooms", len(emptyRooms))
	}
}

// cleanupLoop 清理循环
func (rm *RoomManager) cleanupLoop() {
	defer rm.wg.Done()

	ticker := time.NewTicker(rm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-ticker.C:
			rm.CleanupEmptyRooms()
		}
	}
}

// Redis操作方法

// saveRoomToRedis 保存房间到Redis
func (rm *RoomManager) saveRoomToRedis(room *Room) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	roomData, err := json.Marshal(room)
	if err != nil {
		log.Printf("Failed to marshal room %d: %v", room.ID, err)
		return
	}

	key := fmt.Sprintf("rooms:%d", room.ID)
	if err := rm.redis.Set(ctx, key, roomData, 24*time.Hour).Err(); err != nil {
		log.Printf("Failed to save room %d to Redis: %v", room.ID, err)
	}
}

// loadRoomFromRedis 从Redis加载房间
func (rm *RoomManager) loadRoomFromRedis(roomID uint64) *Room {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := fmt.Sprintf("rooms:%d", roomID)
	data, err := rm.redis.Get(ctx, key).Bytes()
	if err != nil {
		return nil
	}

	var room Room
	if err := json.Unmarshal(data, &room); err != nil {
		log.Printf("Failed to unmarshal room %d: %v", roomID, err)
		return nil
	}

	// 初始化members map
	if room.members == nil {
		room.members = make(map[string]bool)
	}

	return &room
}

// loadRoomsFromRedis 从Redis加载所有房间
func (rm *RoomManager) loadRoomsFromRedis() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pattern := "rooms:*"
	keys, err := rm.redis.Keys(ctx, pattern).Result()
	if err != nil {
		log.Printf("Failed to load room keys from Redis: %v", err)
		return
	}

	loaded := 0
	for _, key := range keys {
		data, err := rm.redis.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}

		var room Room
		if err := json.Unmarshal(data, &room); err != nil {
			continue
		}

		// 初始化members map
		if room.members == nil {
			room.members = make(map[string]bool)
		}

		rm.rooms[room.ID] = &room
		loaded++
	}

	atomic.StoreInt64(&rm.activeRooms, int64(loaded))
	log.Printf("Loaded %d rooms from Redis", loaded)
}

// saveRoomsToRedis 保存所有房间到Redis
func (rm *RoomManager) saveRoomsToRedis() {
	rm.roomMutex.RLock()
	defer rm.roomMutex.RUnlock()

	for _, room := range rm.rooms {
		rm.saveRoomToRedis(room)
	}
}

// deleteRoomFromRedis 从Redis删除房间
func (rm *RoomManager) deleteRoomFromRedis(roomID uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := fmt.Sprintf("rooms:%d", roomID)
	if err := rm.redis.Del(ctx, key).Err(); err != nil {
		log.Printf("Failed to delete room %d from Redis: %v", roomID, err)
	}
}

// GetStats 获取统计信息
func (rm *RoomManager) GetStats() map[string]interface{} {
	rm.roomMutex.RLock()
	defer rm.roomMutex.RUnlock()

	return map[string]interface{}{
		"total_rooms":  atomic.LoadInt64(&rm.totalRooms),
		"active_rooms": atomic.LoadInt64(&rm.activeRooms),
		"room_count":   len(rm.rooms),
		"max_rooms":    rm.maxRooms,
	}
}

// GetActiveRoomCount 获取活跃房间数
func (rm *RoomManager) GetActiveRoomCount() int {
	return int(atomic.LoadInt64(&rm.activeRooms))
}

// Room 的方法

// GetMembers 获取房间成员列表
func (r *Room) GetMembers() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	members := make([]string, 0, len(r.members))
	for connID := range r.members {
		members = append(members, connID)
	}
	return members
}

// HasMember 检查是否包含指定成员
func (r *Room) HasMember(connID string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.members[connID]
}

// GetInfo 获取房间信息
func (r *Room) GetInfo() map[string]interface{} {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return map[string]interface{}{
		"id":            r.ID,
		"name":          r.Name,
		"creator_id":    r.CreatorID,
		"max_members":   r.MaxMembers,
		"member_count":  r.MemberCount,
		"status":        r.Status,
		"created_at":    r.CreatedAt.Unix(),
		"updated_at":    r.UpdatedAt.Unix(),
		"last_activity": r.LastActivity.Unix(),
		"has_password":  r.Password != "",
	}
}
