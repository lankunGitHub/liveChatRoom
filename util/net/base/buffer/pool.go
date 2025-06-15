package buffer

import (
	"sync"
)

// BufferPool 高性能缓冲区池实现
type BufferPool struct {
	// 分级缓冲区池 - 按2的幂次分级，减少内存碎片
	pools []*sync.Pool

	// 尺寸级别定义
	sizes []int

	// 配置
	maxSize int
}

// 默认的尺寸级别 - 2的幂次递增
var defaultSizes = []int{
	512,     // 512B
	1024,    // 1KB
	2048,    // 2KB
	4096,    // 4KB
	8192,    // 8KB
	16384,   // 16KB
	32768,   // 32KB
	65536,   // 64KB
	131072,  // 128KB
	262144,  // 256KB
	524288,  // 512KB
	1048576, // 1MB
}

// NewBufferPool 创建缓冲区池
func NewBufferPool() *BufferPool {
	return NewBufferPoolWithSizes(defaultSizes)
}

// NewBufferPoolWithSizes 创建指定尺寸级别的缓冲区池
func NewBufferPoolWithSizes(sizes []int) *BufferPool {
	if len(sizes) == 0 {
		sizes = defaultSizes
	}

	pools := make([]*sync.Pool, len(sizes))

	// 为每个尺寸级别创建对应的sync.Pool
	for i, size := range sizes {
		poolSize := size // 捕获循环变量
		pools[i] = &sync.Pool{
			New: func() interface{} {
				return NewRingBufferWithCapacity(poolSize, poolSize*2)
			},
		}
	}

	return &BufferPool{
		pools:   pools,
		sizes:   sizes,
		maxSize: sizes[len(sizes)-1],
	}
}

// Get 获取缓冲区
func (bp *BufferPool) Get(size int) Buffer {
	if size <= 0 {
		size = defaultSizes[0]
	}

	// 如果请求的大小超过最大池大小，直接创建
	if size > bp.maxSize {
		return NewRingBufferWithCapacity(size, size*2)
	}

	// 找到合适的尺寸级别
	poolIndex := bp.findPoolIndex(size)
	if poolIndex == -1 {
		// 没有找到合适的池，创建新的缓冲区
		return NewRingBufferWithCapacity(size, size*2)
	}

	// 从池中获取缓冲区
	buffer := bp.pools[poolIndex].Get().(Buffer)
	buffer.Reset()

	return buffer
}

// Put 归还缓冲区到池中
func (bp *BufferPool) Put(buffer Buffer) {
	if buffer == nil {
		return
	}

	bufferCap := buffer.Cap()

	// 找到对应的池
	poolIndex := bp.findExactPoolIndex(bufferCap)
	if poolIndex != -1 {
		// 重置缓冲区
		buffer.Reset()

		// 尝试收缩缓冲区以释放多余内存
		buffer.Shrink()

		// 归还到对应的池
		bp.pools[poolIndex].Put(buffer)
	}
	// 如果没有找到对应的池，直接丢弃（让GC回收）
}

// Clear 清空所有缓冲区池
func (bp *BufferPool) Clear() {
	for _, pool := range bp.pools {
		// 创建新的池来替换旧的池，让GC回收旧的缓冲区
		*pool = sync.Pool{
			New: pool.New,
		}
	}
}

// ==================== 内部方法 ====================

// findPoolIndex 找到适合指定大小的池索引
func (bp *BufferPool) findPoolIndex(size int) int {
	for i, poolSize := range bp.sizes {
		if size <= poolSize {
			return i
		}
	}
	return -1
}

// findExactPoolIndex 找到与指定容量完全匹配的池索引
func (bp *BufferPool) findExactPoolIndex(capacity int) int {
	for i, poolSize := range bp.sizes {
		if capacity == poolSize || capacity == poolSize*2 {
			return i
		}
	}
	return -1
}

// ==================== 全局缓冲区池 ====================

var (
	globalPool     *BufferPool
	globalPoolOnce sync.Once
)

// GetGlobalPool 获取全局缓冲区池单例
func GetGlobalPool() *BufferPool {
	globalPoolOnce.Do(func() {
		globalPool = NewBufferPool()
	})
	return globalPool
}

// Get 从全局池获取缓冲区
func Get(size int) Buffer {
	return GetGlobalPool().Get(size)
}

// Put 归还缓冲区到全局池
func Put(buffer Buffer) {
	GetGlobalPool().Put(buffer)
}
