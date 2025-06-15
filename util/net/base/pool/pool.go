package pool

import (
	"sync"
)

// ObjectPool 通用对象池接口
type ObjectPool interface {
	Get() interface{}
	Put(interface{})
	Clear()
}

// SyncPool sync.Pool的封装
type SyncPool struct {
	pool *sync.Pool
}

// NewSyncPool 创建同步对象池
func NewSyncPool(newFunc func() interface{}) *SyncPool {
	return &SyncPool{
		pool: &sync.Pool{
			New: newFunc,
		},
	}
}

// Get 获取对象
func (sp *SyncPool) Get() interface{} {
	return sp.pool.Get()
}

// Put 归还对象
func (sp *SyncPool) Put(obj interface{}) {
	sp.pool.Put(obj)
}

// Clear 清空池（重新创建）
func (sp *SyncPool) Clear() {
	sp.pool = &sync.Pool{
		New: sp.pool.New,
	}
}

// ==================== 类型化对象池 ====================

// ByteSlicePool 字节切片池
type ByteSlicePool struct {
	pools []*sync.Pool
	sizes []int
}

// NewByteSlicePool 创建字节切片池
func NewByteSlicePool() *ByteSlicePool {
	sizes := []int{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
	}

	pools := make([]*sync.Pool, len(sizes))
	for i, size := range sizes {
		poolSize := size
		pools[i] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, poolSize)
			},
		}
	}

	return &ByteSlicePool{
		pools: pools,
		sizes: sizes,
	}
}

// Get 获取指定大小的字节切片
func (bsp *ByteSlicePool) Get(size int) []byte {
	for i, poolSize := range bsp.sizes {
		if size <= poolSize {
			return bsp.pools[i].Get().([]byte)[:size]
		}
	}
	// 如果超过最大池大小，直接分配
	return make([]byte, size)
}

// Put 归还字节切片
func (bsp *ByteSlicePool) Put(buf []byte) {
	capacity := cap(buf)
	for i, poolSize := range bsp.sizes {
		if capacity == poolSize {
			bsp.pools[i].Put(buf[:poolSize])
			return
		}
	}
	// 不匹配任何池大小，直接丢弃
}

// ==================== 连接池 ====================

// ConnPool 连接池
type ConnPool struct {
	mu    sync.RWMutex
	conns []interface{}

	// 配置
	maxSize      int
	minSize      int
	createFunc   func() (interface{}, error)
	destroyFunc  func(interface{})
	validateFunc func(interface{}) bool

	// 统计
	created   int
	destroyed int
}

// ConnPoolConfig 连接池配置
type ConnPoolConfig struct {
	MaxSize      int
	MinSize      int
	CreateFunc   func() (interface{}, error)
	DestroyFunc  func(interface{})
	ValidateFunc func(interface{}) bool
}

// NewConnPool 创建连接池
func NewConnPool(config *ConnPoolConfig) *ConnPool {
	if config.MaxSize <= 0 {
		config.MaxSize = 10
	}
	if config.MinSize < 0 {
		config.MinSize = 0
	}

	pool := &ConnPool{
		conns:        make([]interface{}, 0, config.MaxSize),
		maxSize:      config.MaxSize,
		minSize:      config.MinSize,
		createFunc:   config.CreateFunc,
		destroyFunc:  config.DestroyFunc,
		validateFunc: config.ValidateFunc,
	}

	// 预创建最小连接数
	pool.prewarmConnections()

	return pool
}

// Get 获取连接
func (cp *ConnPool) Get() (interface{}, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// 尝试从池中获取
	for len(cp.conns) > 0 {
		conn := cp.conns[len(cp.conns)-1]
		cp.conns = cp.conns[:len(cp.conns)-1]

		// 验证连接有效性
		if cp.validateFunc == nil || cp.validateFunc(conn) {
			return conn, nil
		}

		// 连接无效，销毁
		if cp.destroyFunc != nil {
			cp.destroyFunc(conn)
		}
		cp.destroyed++
	}

	// 池中没有可用连接，创建新连接
	if cp.createFunc != nil {
		conn, err := cp.createFunc()
		if err != nil {
			return nil, err
		}
		cp.created++
		return conn, nil
	}

	return nil, nil
}

// Put 归还连接
func (cp *ConnPool) Put(conn interface{}) {
	if conn == nil {
		return
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// 检查池是否已满
	if len(cp.conns) >= cp.maxSize {
		// 池已满，销毁连接
		if cp.destroyFunc != nil {
			cp.destroyFunc(conn)
		}
		cp.destroyed++
		return
	}

	// 验证连接有效性
	if cp.validateFunc != nil && !cp.validateFunc(conn) {
		// 连接无效，销毁
		if cp.destroyFunc != nil {
			cp.destroyFunc(conn)
		}
		cp.destroyed++
		return
	}

	// 归还到池中
	cp.conns = append(cp.conns, conn)
}

// Close 关闭连接池
func (cp *ConnPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// 销毁所有连接
	for _, conn := range cp.conns {
		if cp.destroyFunc != nil {
			cp.destroyFunc(conn)
		}
		cp.destroyed++
	}

	cp.conns = cp.conns[:0]
}

// prewarmConnections 预热连接
func (cp *ConnPool) prewarmConnections() {
	if cp.createFunc == nil {
		return
	}

	for i := 0; i < cp.minSize; i++ {
		conn, err := cp.createFunc()
		if err != nil {
			break
		}
		cp.conns = append(cp.conns, conn)
		cp.created++
	}
}

// Stats 获取连接池统计信息
func (cp *ConnPool) Stats() (active, idle, created, destroyed int) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	idle = len(cp.conns)
	active = cp.created - cp.destroyed - idle
	created = cp.created
	destroyed = cp.destroyed

	return
}

// ==================== 全局对象池 ====================

var (
	globalByteSlicePool *ByteSlicePool
	globalPoolOnce      sync.Once
)

// GetGlobalByteSlicePool 获取全局字节切片池
func GetGlobalByteSlicePool() *ByteSlicePool {
	globalPoolOnce.Do(func() {
		globalByteSlicePool = NewByteSlicePool()
	})
	return globalByteSlicePool
}

// GetBytes 从全局池获取字节切片
func GetBytes(size int) []byte {
	return GetGlobalByteSlicePool().Get(size)
}

// PutBytes 归还字节切片到全局池
func PutBytes(buf []byte) {
	GetGlobalByteSlicePool().Put(buf)
}
