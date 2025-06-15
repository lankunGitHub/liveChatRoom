package buffer

import "syscall"

// Buffer 统一的缓冲区接口 - 高性能零拷贝支持
type Buffer interface {
	// 基础读写操作
	Read(data []byte) (int, error)
	Write(data []byte) (int, error)
	ReadAll() []byte

	// 状态查询
	Size() int
	Cap() int
	Available() int

	// 高级操作
	Peek(n int) ([]byte, error)
	Discard(n int) error
	Reset()

	// 零拷贝操作 - 支持向量I/O
	Readv(iovecs []syscall.Iovec) (int, error)
	Writev(iovecs []syscall.Iovec) (int, error)
	PrepareIovecs() []syscall.Iovec

	// 流式协议支持
	ReadFrame(frameLen int) ([]byte, bool)
	PeekFrame(frameLen int) ([]byte, bool)

	// 内存管理
	Grow(additional int) error
	Shrink() error
}

// Pool 缓冲区池接口 - 高效的内存复用
type Pool interface {
	Get(size int) Buffer
	Put(buffer Buffer)
	Clear()
}
