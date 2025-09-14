package message

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"
)

// GlobalIDGenerator 128位全局消息ID生成器
// ID格式: 时间戳[48bit] + 用户ID[32bit] + 房间号[32bit] + 登录ID[4bit] + 序号[6bit] + 自定义[6bit]
type GlobalIDGenerator struct {
	sequence      uint64 // 序列号计数器
	lastTimestamp int64  // 上次生成的时间戳(毫秒)，用于时钟回拨检测
}

const (
	// ID各部分位数
	TimestampBits = 48 // 时间戳位数 (支持到2250年+)
	UserIDBits    = 32 // 用户ID位数 (支持42亿用户)
	RoomIDBits    = 32 // 房间ID位数 (支持42亿房间)
	LoginIDBits   = 4  // 登录ID位数 (支持16个设备)
	SequenceBits  = 6  // 序号位数 (每毫秒64个消息)
	CustomBits    = 6  // 自定义位数

	// 最大值
	MaxUserID    = (1 << UserIDBits) - 1
	MaxRoomID    = (1 << RoomIDBits) - 1
	MaxLoginID   = (1 << LoginIDBits) - 1
	MaxSequence  = (1 << SequenceBits) - 1
	MaxCustom    = (1 << CustomBits) - 1
	MaxTimestamp = (1 << TimestampBits) - 1

	// 起始时间戳 (2024-01-01 00:00:00 UTC) 毫秒
	Epoch = int64(1704067200000)

	// ID总长度
	GlobalIDLength = 16 // 128位 = 16字节
)

// GlobalID 128位全局消息ID
type GlobalID [GlobalIDLength]byte

// NewGlobalIDGenerator 创建全局ID生成器
func NewGlobalIDGenerator() *GlobalIDGenerator {
	return &GlobalIDGenerator{
		sequence: 0,
	}
}

// GenerateGlobalID 生成128位全局消息ID
// 处理两类边界情况：
//  1. 时钟回拨：若当前时间小于上次生成时间，复用上次时间戳，靠序列号递增保证唯一
//  2. 同毫秒序号耗尽：6位序号每毫秒最多64个，耗尽则自旋等待下一毫秒
func (g *GlobalIDGenerator) GenerateGlobalID(userID, roomID uint32, loginID, custom uint8) GlobalID {
	for {
		now := time.Now().UnixMilli()
		last := atomic.LoadInt64(&g.lastTimestamp)

		if now < last {
			// 时钟回拨，复用上次时间戳
			now = last
		}

		// 抢占新的毫秒时间戳
		if now > last {
			if atomic.CompareAndSwapInt64(&g.lastTimestamp, last, now) {
				return g.buildID(now, userID, roomID, loginID, custom)
			}
			// 其他协程已经更新了时间戳，重新循环
			continue
		}

		// 同一毫秒内：序列号递增
		seq := atomic.AddUint64(&g.sequence, 1) & MaxSequence
		if seq == 0 {
			// 序列号耗尽(64个/毫秒)，自旋等待下一毫秒
			time.Sleep(200 * time.Microsecond)
			continue
		}

		return g.buildIDWithSeq(now, userID, roomID, loginID, custom, seq)
	}
}

// buildID 构建全局ID（新毫秒，序列号从头开始）
func (g *GlobalIDGenerator) buildID(now int64, userID, roomID uint32, loginID, custom uint8) GlobalID {
	seq := atomic.AddUint64(&g.sequence, 1) & MaxSequence
	return g.buildIDWithSeq(now, userID, roomID, loginID, custom, seq)
}

// buildIDWithSeq 按给定时间戳和序列号构建全局ID
func (g *GlobalIDGenerator) buildIDWithSeq(now int64, userID, roomID uint32, loginID, custom uint8, seq uint64) GlobalID {
	// 确保时间戳不超过48位
	timestamp := uint64(now - Epoch)
	if timestamp > MaxTimestamp {
		timestamp = MaxTimestamp
	}

	var id GlobalID

	// 时间戳 (48位) - 存储在前6字节
	binary.BigEndian.PutUint64(id[0:8], timestamp<<16) // 左移16位，占用高48位

	// 用户ID (32位) - 存储在第6-10字节
	binary.BigEndian.PutUint32(id[6:10], userID)

	// 房间ID (32位) - 存储在第10-14字节
	binary.BigEndian.PutUint32(id[10:14], roomID)

	// 登录ID (4位) + 序号 (6位) + 自定义 (6位) = 16位 - 存储在最后2字节
	lastPart := uint16(loginID&MaxLoginID)<<12 | uint16(seq&MaxSequence)<<6 | uint16(custom&MaxCustom)
	binary.BigEndian.PutUint16(id[14:16], lastPart)

	return id
}

// GlobalID工具方法
func (id GlobalID) String() string {
	return fmt.Sprintf("%x", id[:])
}

// ToBytes 转换为字节数组
func (id GlobalID) ToBytes() []byte {
	return id[:]
}

// FromBytes 从字节数组创建GlobalID
func FromBytes(data []byte) (GlobalID, error) {
	if len(data) != GlobalIDLength {
		return GlobalID{}, fmt.Errorf("invalid ID length: expected %d, got %d", GlobalIDLength, len(data))
	}
	var id GlobalID
	copy(id[:], data)
	return id, nil
}

// ExtractTimestamp 提取时间戳 (48位)
func (id GlobalID) ExtractTimestamp() int64 {
	// 读取前8字节，右移16位得到48位时间戳
	timestamp := binary.BigEndian.Uint64(id[0:8]) >> 16
	return int64(timestamp) + Epoch
}

// ExtractUserID 提取用户ID (32位)
func (id GlobalID) ExtractUserID() uint32 {
	return binary.BigEndian.Uint32(id[6:10])
}

// ExtractRoomID 提取房间ID (32位)
func (id GlobalID) ExtractRoomID() uint32 {
	return binary.BigEndian.Uint32(id[10:14])
}

// ExtractLoginID 提取登录ID (4位)
func (id GlobalID) ExtractLoginID() uint8 {
	lastPart := binary.BigEndian.Uint16(id[14:16])
	return uint8((lastPart >> 12) & MaxLoginID)
}

// ExtractSequence 提取序号 (6位)
func (id GlobalID) ExtractSequence() uint8 {
	lastPart := binary.BigEndian.Uint16(id[14:16])
	return uint8((lastPart >> 6) & MaxSequence)
}

// ExtractCustom 提取自定义字段 (6位)
func (id GlobalID) ExtractCustom() uint8 {
	lastPart := binary.BigEndian.Uint16(id[14:16])
	return uint8(lastPart & MaxCustom)
}

// IsZero 检查是否为零值
func (id GlobalID) IsZero() bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return true
}

// IsValid 检查全局ID是否有效
func (id GlobalID) IsValid() bool {
	if id.IsZero() {
		return false
	}

	timestamp := id.ExtractTimestamp()
	now := time.Now().UnixMilli()

	// 消息ID不能来自未来（允许1分钟的时钟偏差）
	if timestamp > now+60*1000 {
		return false
	}

	// 消息ID不能太旧（比如超过30天）
	if now-timestamp > 30*24*60*60*1000 {
		return false
	}

	return true
}

// Compare 比较两个全局ID
// 返回值: -1(id < other), 0(id == other), 1(id > other)
func (id GlobalID) Compare(other GlobalID) int {
	// 首先比较时间戳
	ts1 := id.ExtractTimestamp()
	ts2 := other.ExtractTimestamp()

	if ts1 < ts2 {
		return -1
	} else if ts1 > ts2 {
		return 1
	}

	// 时间戳相同，逐字节比较
	for i := 0; i < GlobalIDLength; i++ {
		if id[i] < other[i] {
			return -1
		} else if id[i] > other[i] {
			return 1
		}
	}

	return 0
}

// Equal 检查两个ID是否相等
func (id GlobalID) Equal(other GlobalID) bool {
	return id.Compare(other) == 0
}

// 连接序列号生成器 - 用于连接的幂等性保证
type ConnSeqGenerator struct {
	sequence uint64
}

func NewConnSeqGenerator() *ConnSeqGenerator {
	return &ConnSeqGenerator{
		sequence: 0,
	}
}

func (g *ConnSeqGenerator) Next() uint64 {
	return atomic.AddUint64(&g.sequence, 1)
}

func (g *ConnSeqGenerator) Current() uint64 {
	return atomic.LoadUint64(&g.sequence)
}

func (g *ConnSeqGenerator) Reset() {
	atomic.StoreUint64(&g.sequence, 0)
}

// 工具函数：从字节数组解析GlobalID各部分信息
func ParseGlobalID(data []byte) (timestamp int64, userID, roomID uint32, loginID, sequence, custom uint8, err error) {
	id, err := FromBytes(data)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	return id.ExtractTimestamp(),
		id.ExtractUserID(),
		id.ExtractRoomID(),
		id.ExtractLoginID(),
		id.ExtractSequence(),
		id.ExtractCustom(),
		nil
}
