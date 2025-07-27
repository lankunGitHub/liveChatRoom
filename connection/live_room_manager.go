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

// LiveRoomManager 针对直播/游戏场景优化的房间管理器
type LiveRoomManager struct {
	// 基础房间管理器
	*RoomManager

	// 直播/游戏特有配置
	config *LiveRoomConfig

	// 分类存储
	liveRooms map[uint64]*LiveRoom // 直播房间
	gameRooms map[uint64]*GameRoom // 游戏房间
	chatRooms map[uint64]*Room     // 普通聊天房间

	// 成员内存管理
	memberManager *MemoryMemberManager

	// 统计信息
	liveRoomCount int64
	gameRoomCount int64
	totalMembers  int64

	mutex sync.RWMutex
}

// LiveRoomConfig 直播房间配置
type LiveRoomConfig struct {
	// TTL策略配置
	LiveRoomActiveTTL   time.Duration // 直播房间活跃TTL
	LiveRoomInactiveTTL time.Duration // 直播房间不活跃TTL
	GameRoomActiveTTL   time.Duration // 游戏房间活跃TTL
	GameRoomInactiveTTL time.Duration // 游戏房间不活跃TTL

	// 活跃判断阈值
	LiveActiveThreshold time.Duration // 直播房间活跃阈值
	GameActiveThreshold time.Duration // 游戏房间活跃阈值

	// 大房间阈值
	LargeRoomThreshold int32 // 大房间成员数阈值
	MegaRoomThreshold  int32 // 超大房间成员数阈值

	// 性能优化配置
	EnableMemoryFirst    bool // 启用内存优先策略
	EnableBatchOperation bool // 启用批量操作
	DisablePersistence   bool // 禁用持久化（测试环境）
}

// RoomType 房间类型
type RoomType int

const (
	RoomTypeChat RoomType = 0 // 普通聊天
	RoomTypeLive RoomType = 1 // 直播房间
	RoomTypeGame RoomType = 2 // 游戏房间
)

// LiveRoom 直播房间
type LiveRoom struct {
	*Room

	// 直播特有属性
	StreamerID   uint32 `json:"streamer_id"`   // 主播ID
	StreamStatus int    `json:"stream_status"` // 直播状态
	ViewerCount  int32  `json:"viewer_count"`  // 观众数
	StreamTitle  string `json:"stream_title"`  // 直播标题
	Category     string `json:"category"`      // 直播分类

	// 性能优化字段
	lastViewerUpdate time.Time // 最后观众数更新时间
	hotLevel         int       // 热度等级 1-5
}

// GameRoom 游戏房间
type GameRoom struct {
	*Room

	// 游戏特有属性
	GameType    string `json:"game_type"`    // 游戏类型
	GameMode    string `json:"game_mode"`    // 游戏模式
	MapName     string `json:"map_name"`     // 地图名称
	GameState   int    `json:"game_state"`   // 游戏状态
	RoundNumber int32  `json:"round_number"` // 回合数

	// 性能字段
	lastStateUpdate time.Time // 最后状态更新时间
	updateFrequency int32     // 更新频率/秒
}

// MemoryMemberManager 内存优先的成员管理器
type MemoryMemberManager struct {
	// 内存存储
	roomMembers map[uint64]*MemberSet // roomID -> members
	userRooms   map[uint32][]uint64   // userID -> roomIDs

	// Redis备份
	redis *redis.Client

	// 配置
	config *LiveRoomConfig

	// 统计
	totalMembers    int64
	totalMembership int64 // 总成员关系数

	mutex sync.RWMutex
}

// MemberSet 成员集合
type MemberSet struct {
	members    map[uint32]*LiveMember // userID -> member
	count      int32
	roomType   RoomType
	lastUpdate time.Time

	// 性能优化
	hotMembers map[uint32]bool // 活跃成员

	mutex sync.RWMutex
}

// LiveMember 直播/游戏成员
type LiveMember struct {
	UserID   uint32     `json:"user_id"`
	LoginID  uint8      `json:"login_id"`
	ConnID   string     `json:"conn_id"`
	Role     MemberRole `json:"role"`
	JoinedAt time.Time  `json:"joined_at"`
	LastSeen time.Time  `json:"last_seen"`
	Online   bool       `json:"online"`

	// 直播/游戏特有
	IsViewer   bool `json:"is_viewer"`  // 是否观众
	IsPlayer   bool `json:"is_player"`  // 是否玩家
	Permission int  `json:"permission"` // 权限级别
}

// MemberRole 成员角色
type MemberRole int

const (
	RoleViewer    MemberRole = 0 // 观众
	RolePlayer    MemberRole = 1 // 玩家
	RoleModerator MemberRole = 2 // 管理员
	RoleStreamer  MemberRole = 3 // 主播
	RoleOwner     MemberRole = 4 // 房主
)

// NewLiveRoomManager 创建直播房间管理器
func NewLiveRoomManager(baseManager *RoomManager, config *LiveRoomConfig) *LiveRoomManager {
	if config == nil {
		config = DefaultLiveRoomConfig()
	}

	return &LiveRoomManager{
		RoomManager:   baseManager,
		config:        config,
		liveRooms:     make(map[uint64]*LiveRoom),
		gameRooms:     make(map[uint64]*GameRoom),
		chatRooms:     make(map[uint64]*Room),
		memberManager: NewMemoryMemberManager(baseManager.redis, config),
	}
}

// DefaultLiveRoomConfig 默认直播房间配置
func DefaultLiveRoomConfig() *LiveRoomConfig {
	return &LiveRoomConfig{
		LiveRoomActiveTTL:    4 * time.Hour,
		LiveRoomInactiveTTL:  30 * time.Minute,
		GameRoomActiveTTL:    2 * time.Hour,
		GameRoomInactiveTTL:  10 * time.Minute,
		LiveActiveThreshold:  5 * time.Minute,
		GameActiveThreshold:  2 * time.Minute,
		LargeRoomThreshold:   1000,
		MegaRoomThreshold:    10000,
		EnableMemoryFirst:    true,
		EnableBatchOperation: true,
		DisablePersistence:   false,
	}
}

// NewMemoryMemberManager 创建内存成员管理器
func NewMemoryMemberManager(redis *redis.Client, config *LiveRoomConfig) *MemoryMemberManager {
	return &MemoryMemberManager{
		roomMembers: make(map[uint64]*MemberSet),
		userRooms:   make(map[uint32][]uint64),
		redis:       redis,
		config:      config,
	}
}

// CreateLiveRoom 创建直播房间
func (lrm *LiveRoomManager) CreateLiveRoom(roomID uint64, creatorID uint32, streamerID uint32, title string, category string) (*LiveRoom, error) {
	// 创建基础房间
	baseRoom := &Room{
		ID:           roomID,
		Name:         title,
		CreatorID:    creatorID,
		MaxMembers:   100000, // 直播房间支持大量观众
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		Status:       RoomStatusOpen,
		MemberCount:  0,
		LastActivity: time.Now(),
		members:      make(map[string]bool),
	}

	// 创建直播房间
	liveRoom := &LiveRoom{
		Room:             baseRoom,
		StreamerID:       streamerID,
		StreamStatus:     1, // 直播中
		ViewerCount:      0,
		StreamTitle:      title,
		Category:         category,
		lastViewerUpdate: time.Now(),
		hotLevel:         1,
	}

	lrm.mutex.Lock()
	lrm.liveRooms[roomID] = liveRoom
	lrm.mutex.Unlock()

	atomic.AddInt64(&lrm.liveRoomCount, 1)

	// 添加主播为成员
	if err := lrm.memberManager.AddMember(roomID, &LiveMember{
		UserID:     streamerID,
		LoginID:    0,
		Role:       RoleStreamer,
		JoinedAt:   time.Now(),
		LastSeen:   time.Now(),
		Online:     true,
		IsViewer:   false,
		IsPlayer:   false,
		Permission: 100,
	}); err != nil {
		return nil, fmt.Errorf("failed to add streamer to room: %v", err)
	}

	// 设置Redis TTL
	if err := lrm.setRoomTTL(roomID, RoomTypeLive); err != nil {
		log.Printf("Failed to set room TTL: %v", err)
	}

	log.Printf("Created live room %d for streamer %d: %s", roomID, streamerID, title)
	return liveRoom, nil
}

// CreateGameRoom 创建游戏房间
func (lrm *LiveRoomManager) CreateGameRoom(roomID uint64, creatorID uint32, gameType, gameMode, mapName string) (*GameRoom, error) {
	// 创建基础房间
	baseRoom := &Room{
		ID:           roomID,
		Name:         fmt.Sprintf("%s - %s", gameType, gameMode),
		CreatorID:    creatorID,
		MaxMembers:   50, // 游戏房间通常人数较少
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		Status:       RoomStatusOpen,
		MemberCount:  0,
		LastActivity: time.Now(),
		members:      make(map[string]bool),
	}

	// 创建游戏房间
	gameRoom := &GameRoom{
		Room:            baseRoom,
		GameType:        gameType,
		GameMode:        gameMode,
		MapName:         mapName,
		GameState:       0, // 等待开始
		RoundNumber:     0,
		lastStateUpdate: time.Now(),
		updateFrequency: 60, // 60fps更新
	}

	lrm.mutex.Lock()
	lrm.gameRooms[roomID] = gameRoom
	lrm.mutex.Unlock()

	atomic.AddInt64(&lrm.gameRoomCount, 1)

	// 添加房主为成员
	if err := lrm.memberManager.AddMember(roomID, &LiveMember{
		UserID:     creatorID,
		LoginID:    0,
		Role:       RoleOwner,
		JoinedAt:   time.Now(),
		LastSeen:   time.Now(),
		Online:     true,
		IsViewer:   false,
		IsPlayer:   true,
		Permission: 100,
	}); err != nil {
		return nil, fmt.Errorf("failed to add owner to game room: %v", err)
	}

	// 设置Redis TTL
	if err := lrm.setRoomTTL(roomID, RoomTypeGame); err != nil {
		log.Printf("Failed to set room TTL: %v", err)
	}

	log.Printf("Created game room %d: %s %s on %s", roomID, gameType, gameMode, mapName)
	return gameRoom, nil
}

// JoinAsViewer 作为观众加入直播房间
func (lrm *LiveRoomManager) JoinAsViewer(roomID uint64, userID uint32, loginID uint8, connID string) error {
	lrm.mutex.RLock()
	liveRoom, isLive := lrm.liveRooms[roomID]
	lrm.mutex.RUnlock()

	if !isLive {
		return fmt.Errorf("room %d is not a live room", roomID)
	}

	// 添加观众
	member := &LiveMember{
		UserID:     userID,
		LoginID:    loginID,
		ConnID:     connID,
		Role:       RoleViewer,
		JoinedAt:   time.Now(),
		LastSeen:   time.Now(),
		Online:     true,
		IsViewer:   true,
		IsPlayer:   false,
		Permission: 1,
	}

	if err := lrm.memberManager.AddMember(roomID, member); err != nil {
		return err
	}

	// 更新观众数
	newCount := atomic.AddInt32(&liveRoom.ViewerCount, 1)
	liveRoom.lastViewerUpdate = time.Now()

	// 更新热度等级
	lrm.updateHotLevel(liveRoom, newCount)

	// 更新活动时间，重新计算TTL
	liveRoom.LastActivity = time.Now()
	lrm.setRoomTTL(roomID, RoomTypeLive)

	log.Printf("User %d joined live room %d as viewer (total viewers: %d)", userID, roomID, newCount)
	return nil
}

// JoinAsPlayer 作为玩家加入游戏房间
func (lrm *LiveRoomManager) JoinAsPlayer(roomID uint64, userID uint32, loginID uint8, connID string) error {
	lrm.mutex.RLock()
	gameRoom, isGame := lrm.gameRooms[roomID]
	lrm.mutex.RUnlock()

	if !isGame {
		return fmt.Errorf("room %d is not a game room", roomID)
	}

	// 检查房间是否已满
	if gameRoom.MemberCount >= gameRoom.MaxMembers {
		return fmt.Errorf("game room %d is full", roomID)
	}

	// 添加玩家
	member := &LiveMember{
		UserID:     userID,
		LoginID:    loginID,
		ConnID:     connID,
		Role:       RolePlayer,
		JoinedAt:   time.Now(),
		LastSeen:   time.Now(),
		Online:     true,
		IsViewer:   false,
		IsPlayer:   true,
		Permission: 10,
	}

	if err := lrm.memberManager.AddMember(roomID, member); err != nil {
		return err
	}

	// 更新房间状态
	gameRoom.MemberCount++
	gameRoom.LastActivity = time.Now()
	gameRoom.lastStateUpdate = time.Now()

	// 重新计算TTL
	lrm.setRoomTTL(roomID, RoomTypeGame)

	log.Printf("User %d joined game room %d as player (total players: %d)", userID, roomID, gameRoom.MemberCount)
	return nil
}

// 内存成员管理器方法

// AddMember 添加成员（内存优先）
func (mm *MemoryMemberManager) AddMember(roomID uint64, member *LiveMember) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	// 获取或创建成员集合
	memberSet, exists := mm.roomMembers[roomID]
	if !exists {
		memberSet = &MemberSet{
			members:    make(map[uint32]*LiveMember),
			count:      0,
			roomType:   RoomTypeChat, // 默认值，后续会更新
			lastUpdate: time.Now(),
			hotMembers: make(map[uint32]bool),
		}
		mm.roomMembers[roomID] = memberSet
	}

	memberSet.mutex.Lock()
	defer memberSet.mutex.Unlock()

	// 检查是否已存在
	if existingMember, exists := memberSet.members[member.UserID]; exists {
		// 更新现有成员信息
		existingMember.LoginID = member.LoginID
		existingMember.ConnID = member.ConnID
		existingMember.LastSeen = member.LastSeen
		existingMember.Online = true
	} else {
		// 添加新成员
		memberSet.members[member.UserID] = member
		memberSet.count++
		atomic.AddInt64(&mm.totalMembers, 1)

		// 更新用户房间映射
		mm.userRooms[member.UserID] = append(mm.userRooms[member.UserID], roomID)
	}

	// 标记为活跃成员
	memberSet.hotMembers[member.UserID] = true
	memberSet.lastUpdate = time.Now()

	// 异步更新Redis（不阻塞）
	if mm.config.EnableMemoryFirst {
		go mm.syncMemberToRedis(roomID, member)
	}

	return nil
}

// RemoveMember 移除成员
func (mm *MemoryMemberManager) RemoveMember(roomID uint64, userID uint32) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	memberSet, exists := mm.roomMembers[roomID]
	if !exists {
		return fmt.Errorf("room %d not found", roomID)
	}

	memberSet.mutex.Lock()
	defer memberSet.mutex.Unlock()

	if _, exists := memberSet.members[userID]; exists {
		delete(memberSet.members, userID)
		delete(memberSet.hotMembers, userID)
		memberSet.count--
		atomic.AddInt64(&mm.totalMembers, -1)

		// 更新用户房间映射
		userRooms := mm.userRooms[userID]
		for i, rid := range userRooms {
			if rid == roomID {
				mm.userRooms[userID] = append(userRooms[:i], userRooms[i+1:]...)
				break
			}
		}

		memberSet.lastUpdate = time.Now()

		// 异步更新Redis
		if mm.config.EnableMemoryFirst {
			go mm.removeMemberFromRedis(roomID, userID)
		}

		return nil
	}

	return fmt.Errorf("user %d not found in room %d", userID, roomID)
}

// GetMemberCount 获取房间成员数
func (mm *MemoryMemberManager) GetMemberCount(roomID uint64) int32 {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if memberSet, exists := mm.roomMembers[roomID]; exists {
		return memberSet.count
	}
	return 0
}

// 辅助方法

// setRoomTTL 设置房间TTL
func (lrm *LiveRoomManager) setRoomTTL(roomID uint64, roomType RoomType) error {
	ttl := lrm.calculateTTL(roomID, roomType)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	roomKey := fmt.Sprintf("room_nodes:%d", roomID)
	return lrm.redis.Expire(ctx, roomKey, ttl).Err()
}

// calculateTTL 计算动态TTL
func (lrm *LiveRoomManager) calculateTTL(roomID uint64, roomType RoomType) time.Duration {
	var lastActivity time.Time
	var threshold time.Duration
	var activeTTL, inactiveTTL time.Duration

	switch roomType {
	case RoomTypeLive:
		if room, exists := lrm.liveRooms[roomID]; exists {
			lastActivity = room.LastActivity
		}
		threshold = lrm.config.LiveActiveThreshold
		activeTTL = lrm.config.LiveRoomActiveTTL
		inactiveTTL = lrm.config.LiveRoomInactiveTTL

	case RoomTypeGame:
		if room, exists := lrm.gameRooms[roomID]; exists {
			lastActivity = room.LastActivity
		}
		threshold = lrm.config.GameActiveThreshold
		activeTTL = lrm.config.GameRoomActiveTTL
		inactiveTTL = lrm.config.GameRoomInactiveTTL

	default:
		return 24 * time.Hour // 普通聊天房间保持原来的TTL
	}

	if time.Since(lastActivity) < threshold {
		return activeTTL
	}
	return inactiveTTL
}

// updateHotLevel 更新直播房间热度等级
func (lrm *LiveRoomManager) updateHotLevel(room *LiveRoom, viewerCount int32) {
	var newLevel int
	switch {
	case viewerCount >= 100000:
		newLevel = 5 // 超级热门
	case viewerCount >= 10000:
		newLevel = 4 // 很热门
	case viewerCount >= 1000:
		newLevel = 3 // 热门
	case viewerCount >= 100:
		newLevel = 2 // 一般
	default:
		newLevel = 1 // 冷门
	}

	if newLevel != room.hotLevel {
		room.hotLevel = newLevel
		log.Printf("Live room %d hot level updated to %d (viewers: %d)", room.ID, newLevel, viewerCount)
	}
}

// syncMemberToRedis 异步同步成员到Redis
func (mm *MemoryMemberManager) syncMemberToRedis(roomID uint64, member *LiveMember) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	memberKey := fmt.Sprintf("room_member:%d:%d", roomID, member.UserID)
	data, err := json.Marshal(member)
	if err != nil {
		return
	}

	// 较短的TTL，主要用于容灾
	mm.redis.Set(ctx, memberKey, data, 1*time.Hour)
}

// removeMemberFromRedis 异步从Redis移除成员
func (mm *MemoryMemberManager) removeMemberFromRedis(roomID uint64, userID uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	memberKey := fmt.Sprintf("room_member:%d:%d", roomID, userID)
	mm.redis.Del(ctx, memberKey)
}

// GetStats 获取直播房间管理器统计信息
func (lrm *LiveRoomManager) GetStats() map[string]interface{} {
	lrm.mutex.RLock()
	defer lrm.mutex.RUnlock()

	// 计算各类房间数量
	var totalViewers int32
	var hotRooms int
	for _, room := range lrm.liveRooms {
		totalViewers += room.ViewerCount
		if room.hotLevel >= 3 {
			hotRooms++
		}
	}

	return map[string]interface{}{
		"live_rooms":    atomic.LoadInt64(&lrm.liveRoomCount),
		"game_rooms":    atomic.LoadInt64(&lrm.gameRoomCount),
		"total_viewers": totalViewers,
		"hot_rooms":     hotRooms,
		"total_members": atomic.LoadInt64(&lrm.memberManager.totalMembers),
		"memory_rooms":  len(lrm.memberManager.roomMembers),
	}
}
