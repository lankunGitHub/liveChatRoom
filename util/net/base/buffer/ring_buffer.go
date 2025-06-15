package buffer

import (
	"sync"
	"syscall"
	"unsafe"
)

// RingBuffer 高性能环形缓冲区实现
type RingBuffer struct {
	mu sync.RWMutex

	// 缓冲区数据
	buf  []byte
	head int
	tail int
	size int

	// 零拷贝支持
	iovecs []syscall.Iovec

	// 容量管理
	initialCapacity int
	maxCapacity     int
}

// NewRingBuffer 创建环形缓冲区
func NewRingBuffer(initialCapacity int) *RingBuffer {
	if initialCapacity <= 0 {
		initialCapacity = 4096
	}

	return &RingBuffer{
		buf:             make([]byte, initialCapacity),
		head:            0,
		tail:            0,
		size:            0,
		iovecs:          make([]syscall.Iovec, 0, 2),
		initialCapacity: initialCapacity,
		maxCapacity:     1024 * 1024, // 1MB
	}
}

// NewRingBufferWithCapacity 创建指定容量的环形缓冲区
func NewRingBufferWithCapacity(initialCapacity, maxCapacity int) *RingBuffer {
	if initialCapacity <= 0 {
		initialCapacity = 4096
	}
	if maxCapacity <= 0 {
		maxCapacity = 1024 * 1024
	}

	return &RingBuffer{
		buf:             make([]byte, initialCapacity),
		head:            0,
		tail:            0,
		size:            0,
		iovecs:          make([]syscall.Iovec, 0, 2),
		initialCapacity: initialCapacity,
		maxCapacity:     maxCapacity,
	}
}

// ==================== 基础操作 ====================

// Size 返回当前数据大小
func (rb *RingBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// Cap 返回缓冲区容量
func (rb *RingBuffer) Cap() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return cap(rb.buf)
}

// Available 返回可用空间
func (rb *RingBuffer) Available() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return cap(rb.buf) - rb.size
}

// Reset 重置缓冲区
func (rb *RingBuffer) Reset() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.head = 0
	rb.tail = 0
	rb.size = 0
}

// ==================== 读写操作 ====================

// Read 读取数据
func (rb *RingBuffer) Read(data []byte) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 || len(data) == 0 {
		return 0, nil
	}

	toRead := len(data)
	if toRead > rb.size {
		toRead = rb.size
	}

	copied := rb.readToSlice(data, toRead)
	rb.size -= copied

	return copied, nil
}

// Write 写入数据
func (rb *RingBuffer) Write(data []byte) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(data) == 0 {
		return 0, nil
	}

	// 确保有足够空间
	if rb.size+len(data) > cap(rb.buf) {
		if err := rb.grow(len(data)); err != nil {
			return 0, err
		}
	}

	written := rb.writeFromSlice(data)
	rb.size += written

	return written, nil
}

// ReadAll 读取所有数据
func (rb *RingBuffer) ReadAll() []byte {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 {
		return nil
	}

	data := make([]byte, rb.size)
	rb.readToSlice(data, rb.size)
	rb.size = 0

	return data
}

// Peek 预览数据（不移动读指针）
func (rb *RingBuffer) Peek(n int) ([]byte, error) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 || n <= 0 {
		return nil, nil
	}

	toPeek := n
	if toPeek > rb.size {
		toPeek = rb.size
	}

	data := make([]byte, toPeek)
	rb.peekToSlice(data, toPeek)

	return data, nil
}

// Discard 丢弃数据
func (rb *RingBuffer) Discard(n int) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 || n <= 0 {
		return nil
	}

	toDiscard := n
	if toDiscard > rb.size {
		toDiscard = rb.size
	}

	rb.head = (rb.head + toDiscard) % cap(rb.buf)
	rb.size -= toDiscard

	return nil
}

// ==================== 零拷贝操作 ====================

// PrepareIovecs 准备向量I/O结构
func (rb *RingBuffer) PrepareIovecs() []syscall.Iovec {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return nil
	}

	// 清空之前的iovecs
	rb.iovecs = rb.iovecs[:0]

	// 根据数据分布创建iovecs
	if rb.head < rb.tail {
		// 数据连续
		rb.iovecs = append(rb.iovecs, syscall.Iovec{
			Base: &rb.buf[rb.head],
			Len:  uint64(rb.tail - rb.head),
		})
	} else if rb.size > 0 {
		// 数据跨越缓冲区边界
		if rb.head < len(rb.buf) {
			rb.iovecs = append(rb.iovecs, syscall.Iovec{
				Base: &rb.buf[rb.head],
				Len:  uint64(len(rb.buf) - rb.head),
			})
		}
		if rb.tail > 0 {
			rb.iovecs = append(rb.iovecs, syscall.Iovec{
				Base: &rb.buf[0],
				Len:  uint64(rb.tail),
			})
		}
	}

	return rb.iovecs
}

// Readv 向量读取（零拷贝）
func (rb *RingBuffer) Readv(iovecs []syscall.Iovec) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 {
		return 0, nil
	}

	totalRead := 0
	remaining := rb.size

	for _, iovec := range iovecs {
		if remaining <= 0 {
			break
		}

		toRead := int(iovec.Len)
		if toRead > remaining {
			toRead = remaining
		}

		// 从缓冲区读取数据到iovec
		baseSlice := (*[1 << 30]byte)(unsafe.Pointer(iovec.Base))[:iovec.Len:iovec.Len]
		read := rb.readToSlice(baseSlice, toRead)
		totalRead += read
		remaining -= read

		if read < toRead {
			break
		}
	}

	rb.size -= totalRead
	return totalRead, nil
}

// Writev 向量写入（零拷贝）
func (rb *RingBuffer) Writev(iovecs []syscall.Iovec) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	totalWritten := 0

	for _, iovec := range iovecs {
		if iovec.Len == 0 {
			continue
		}

		// 确保有足够空间
		if rb.size+int(iovec.Len) > cap(rb.buf) {
			if err := rb.grow(int(iovec.Len)); err != nil {
				return totalWritten, err
			}
		}

		// 写入数据
		baseSlice := (*[1 << 30]byte)(unsafe.Pointer(iovec.Base))[:iovec.Len:iovec.Len]
		written := rb.writeFromSlice(baseSlice)
		totalWritten += written

		if written < int(iovec.Len) {
			break
		}
	}

	rb.size += totalWritten
	return totalWritten, nil
}

// ==================== 流式协议支持 ====================

// ReadFrame 读取指定长度的帧
func (rb *RingBuffer) ReadFrame(frameLen int) ([]byte, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size < frameLen || frameLen <= 0 {
		return nil, false
	}

	frame := make([]byte, frameLen)
	read := rb.readToSlice(frame, frameLen)

	if read == frameLen {
		rb.size -= read
		return frame, true
	}

	return nil, false
}

// PeekFrame 预览指定长度的帧（不移动读指针）
func (rb *RingBuffer) PeekFrame(frameLen int) ([]byte, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size < frameLen || frameLen <= 0 {
		return nil, false
	}

	frame := make([]byte, frameLen)
	rb.peekToSlice(frame, frameLen)

	return frame, true
}

// ==================== 内存管理 ====================

// Grow 扩展缓冲区
func (rb *RingBuffer) Grow(additional int) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.grow(additional)
}

// grow 内部扩展方法（不加锁）
func (rb *RingBuffer) grow(additional int) error {
	currentCap := cap(rb.buf)
	newCap := currentCap * 2

	// 确保新容量足够
	if newCap < currentCap+rb.size+additional {
		newCap = currentCap + rb.size + additional
	}

	// 限制最大容量
	if newCap > rb.maxCapacity {
		newCap = rb.maxCapacity
	}

	if newCap <= currentCap {
		return nil
	}

	// 创建新缓冲区
	newBuf := make([]byte, newCap)

	// 复制现有数据
	if rb.size > 0 {
		if rb.head < rb.tail {
			// 数据连续
			copy(newBuf, rb.buf[rb.head:rb.tail])
		} else {
			// 数据跨越边界
			n := copy(newBuf, rb.buf[rb.head:])
			copy(newBuf[n:], rb.buf[:rb.tail])
		}
	}

	// 更新状态
	rb.buf = newBuf
	rb.head = 0
	rb.tail = rb.size

	return nil
}

// Shrink 收缩缓冲区（当使用率低时释放内存）
func (rb *RingBuffer) Shrink() error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	currentCap := cap(rb.buf)
	// 如果使用率低于25%且大于初始容量，则收缩
	if rb.size < currentCap/4 && currentCap > rb.initialCapacity {
		newCap := currentCap / 2
		if newCap < rb.initialCapacity {
			newCap = rb.initialCapacity
		}

		// 创建新缓冲区
		newBuf := make([]byte, newCap)

		// 复制现有数据
		if rb.size > 0 {
			if rb.head < rb.tail {
				copy(newBuf, rb.buf[rb.head:rb.tail])
			} else {
				n := copy(newBuf, rb.buf[rb.head:])
				copy(newBuf[n:], rb.buf[:rb.tail])
			}
		}

		// 更新状态
		rb.buf = newBuf
		rb.head = 0
		rb.tail = rb.size
	}

	return nil
}

// ==================== 内部辅助方法 ====================

// readToSlice 读取数据到切片（不加锁）
func (rb *RingBuffer) readToSlice(dst []byte, n int) int {
	if rb.size == 0 || n <= 0 {
		return 0
	}

	toRead := n
	if toRead > rb.size {
		toRead = rb.size
	}

	if rb.head < rb.tail {
		// 数据连续
		copied := copy(dst, rb.buf[rb.head:rb.head+toRead])
		rb.head += copied
		return copied
	} else {
		// 数据跨越边界
		firstPart := len(rb.buf) - rb.head
		if toRead <= firstPart {
			copied := copy(dst, rb.buf[rb.head:rb.head+toRead])
			rb.head += copied
			if rb.head == len(rb.buf) {
				rb.head = 0
			}
			return copied
		} else {
			// 读取第一部分
			copied := copy(dst, rb.buf[rb.head:])
			rb.head = 0

			// 读取第二部分
			remaining := toRead - copied
			if remaining > 0 && rb.tail > 0 {
				secondPart := copy(dst[copied:], rb.buf[:remaining])
				rb.head = secondPart
				copied += secondPart
			}

			return copied
		}
	}
}

// writeFromSlice 从切片写入数据（不加锁）
func (rb *RingBuffer) writeFromSlice(src []byte) int {
	if len(src) == 0 {
		return 0
	}

	written := 0
	for written < len(src) {
		available := cap(rb.buf) - rb.size
		if available == 0 {
			break
		}

		toWrite := len(src) - written
		if toWrite > available {
			toWrite = available
		}

		// 计算写入位置
		if rb.tail < rb.head || (rb.tail >= rb.head && rb.size == 0) {
			// 尾部有连续空间或缓冲区为空
			endSpace := len(rb.buf) - rb.tail
			if toWrite <= endSpace {
				copied := copy(rb.buf[rb.tail:], src[written:written+toWrite])
				rb.tail += copied
				if rb.tail == len(rb.buf) {
					rb.tail = 0
				}
				written += copied
			} else {
				// 写入到尾部，然后从头部开始
				copied := copy(rb.buf[rb.tail:], src[written:written+endSpace])
				rb.tail = 0
				written += copied

				remaining := toWrite - copied
				if remaining > 0 && rb.head > 0 {
					copied2 := copy(rb.buf[rb.tail:rb.head], src[written:written+remaining])
					rb.tail += copied2
					written += copied2
				}
			}
		} else {
			// 在head和tail之间写入
			copied := copy(rb.buf[rb.tail:], src[written:written+toWrite])
			rb.tail += copied
			written += copied
		}
	}

	return written
}

// peekToSlice 预览数据到切片（不移动读指针，不加锁）
func (rb *RingBuffer) peekToSlice(dst []byte, n int) int {
	if rb.size == 0 || n <= 0 {
		return 0
	}

	toRead := n
	if toRead > rb.size {
		toRead = rb.size
	}

	if rb.head < rb.tail {
		// 数据连续
		return copy(dst, rb.buf[rb.head:rb.head+toRead])
	} else {
		// 数据跨越边界
		firstPart := len(rb.buf) - rb.head
		if toRead <= firstPart {
			return copy(dst, rb.buf[rb.head:rb.head+toRead])
		} else {
			// 复制第一部分
			copied := copy(dst, rb.buf[rb.head:])

			// 复制第二部分
			remaining := toRead - copied
			if remaining > 0 {
				copied += copy(dst[copied:], rb.buf[:remaining])
			}

			return copied
		}
	}
}
