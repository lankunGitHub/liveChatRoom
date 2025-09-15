package epoll

import (
	"encoding/binary"
	"syscall"
	"unsafe"
)

// Event epoll事件
type Event struct {
	Events uint32 // 事件类型
	Data   uint64 // 用户数据（通常存储fd）
}

// epollEventSize 内核 struct epoll_event 的大小
// 注意：Linux 内核的 epoll_event 是 __attribute__((packed))，
// 布局为 {u32 events; u64 data} 共 12 字节，data 偏移为 4。
// Go 的 Event 结构体是 16 字节（uint64 按 8 字节对齐），
// 不能直接传给内核，必须手动按 12 字节打包/解包。
const epollEventSize = 12

// Epoll epoll实例封装
type Epoll struct {
	fd        int
	events    []Event
	rawEvents []byte // 内核格式的原始事件缓冲区 (12字节/事件)
}

// 事件类型常量
const (
	EPOLLIN  = 0x1        // 可读
	EPOLLOUT = 0x4        // 可写
	EPOLLERR = 0x8        // 错误
	EPOLLHUP = 0x10       // 挂起
	EPOLLET  = 0x80000000 // 边缘触发模式
	EPOLLLT  = 0x0        // 水平触发模式（默认）
)

// 操作类型常量
const (
	EPOLL_CTL_ADD = 1 // 添加
	EPOLL_CTL_DEL = 2 // 删除
	EPOLL_CTL_MOD = 3 // 修改
)

// New 创建新的epoll实例
func New() (*Epoll, error) {
	fd, err := epollCreate1(0)
	if err != nil {
		return nil, err
	}

	return &Epoll{
		fd:        fd,
		events:    make([]Event, 64), // 默认64个事件
		rawEvents: make([]byte, 64*epollEventSize),
	}, nil
}

// NewWithSize 创建指定事件数量的epoll实例
func NewWithSize(size int) (*Epoll, error) {
	if size <= 0 {
		size = 64
	}

	fd, err := epollCreate1(0)
	if err != nil {
		return nil, err
	}

	return &Epoll{
		fd:        fd,
		events:    make([]Event, size),
		rawEvents: make([]byte, size*epollEventSize),
	}, nil
}

// Close 关闭epoll实例
func (e *Epoll) Close() error {
	if e.fd < 0 {
		return nil
	}
	err := syscall.Close(e.fd)
	e.fd = -1
	return err
}

// ==================== 文件描述符管理 ====================

// Add 添加文件描述符到epoll
func (e *Epoll) Add(fd int, events uint32) error {
	return e.Ctl(EPOLL_CTL_ADD, fd, events)
}

// Del 从epoll删除文件描述符
func (e *Epoll) Del(fd int) error {
	return e.Ctl(EPOLL_CTL_DEL, fd, 0)
}

// Mod 修改文件描述符的事件
func (e *Epoll) Mod(fd int, events uint32) error {
	return e.Ctl(EPOLL_CTL_MOD, fd, events)
}

// Ctl epoll控制操作
func (e *Epoll) Ctl(op int, fd int, events uint32) error {
	event := Event{
		Events: events,
		Data:   uint64(fd),
	}
	return epollCtl(e.fd, op, fd, &event)
}

// ==================== 事件等待 ====================

// Wait 等待事件，返回就绪的事件数量
func (e *Epoll) Wait(timeout int) (int, error) {
	n, err := epollWait(e.fd, e.rawEvents, timeout)
	if err != nil || n <= 0 {
		return n, err
	}

	// 按内核 packed 布局(12字节/条)逐条解码
	for i := 0; i < n; i++ {
		rec := e.rawEvents[i*epollEventSize : (i+1)*epollEventSize]
		e.events[i].Events = binary.NativeEndian.Uint32(rec[0:4])
		e.events[i].Data = binary.NativeEndian.Uint64(rec[4:12])
	}
	return n, nil
}

// WaitWithTimeout 等待事件（毫秒超时）
func (e *Epoll) WaitWithTimeout(timeoutMs int) (int, error) {
	return e.Wait(timeoutMs)
}

// WaitForever 永久等待事件
func (e *Epoll) WaitForever() (int, error) {
	return e.Wait(-1)
}

// GetEvents 获取事件数组
func (e *Epoll) GetEvents() []Event {
	return e.events
}

// GetEvent 获取指定索引的事件
func (e *Epoll) GetEvent(index int) Event {
	if index < 0 || index >= len(e.events) {
		return Event{}
	}
	return e.events[index]
}

// ==================== 事件循环支持 ====================

// EventHandler 事件处理器接口
type EventHandler interface {
	OnRead(fd int)
	OnWrite(fd int)
	OnError(fd int)
	OnHup(fd int)
}

// Loop 事件循环
func (e *Epoll) Loop(handler EventHandler) error {
	for {
		n, err := e.WaitForever()
		if err != nil {
			return err
		}

		for i := 0; i < n; i++ {
			event := e.events[i]
			fd := int(event.Data)

			// 处理错误事件
			if event.Events&EPOLLERR != 0 {
				handler.OnError(fd)
				continue
			}

			// 处理挂起事件
			if event.Events&EPOLLHUP != 0 {
				handler.OnHup(fd)
				continue
			}

			// 处理读事件
			if event.Events&EPOLLIN != 0 {
				handler.OnRead(fd)
			}

			// 处理写事件
			if event.Events&EPOLLOUT != 0 {
				handler.OnWrite(fd)
			}
		}
	}
}

// ==================== 批量操作 ====================

// AddMultiple 批量添加文件描述符
func (e *Epoll) AddMultiple(fds []int, events uint32) error {
	for _, fd := range fds {
		if err := e.Add(fd, events); err != nil {
			return err
		}
	}
	return nil
}

// DelMultiple 批量删除文件描述符
func (e *Epoll) DelMultiple(fds []int) error {
	for _, fd := range fds {
		if err := e.Del(fd); err != nil {
			return err
		}
	}
	return nil
}

// ==================== 工具方法 ====================

// IsReadable 检查事件是否可读
func (e Event) IsReadable() bool {
	return e.Events&EPOLLIN != 0
}

// IsWritable 检查事件是否可写
func (e Event) IsWritable() bool {
	return e.Events&EPOLLOUT != 0
}

// IsError 检查是否有错误
func (e Event) IsError() bool {
	return e.Events&EPOLLERR != 0
}

// IsHup 检查是否挂起
func (e Event) IsHup() bool {
	return e.Events&EPOLLHUP != 0
}

// GetFD 获取文件描述符
func (e Event) GetFD() int {
	return int(e.Data)
}

// ==================== 系统调用封装 ====================

// epollCreate1 创建epoll实例
func epollCreate1(flags int) (int, error) {
	fd, _, errno := syscall.Syscall(syscall.SYS_EPOLL_CREATE1, uintptr(flags), 0, 0)
	if errno != 0 {
		return -1, errno
	}
	return int(fd), nil
}

// epollCtl 控制epoll实例
// event 按内核 packed 布局(12字节)手动打包后传入
func epollCtl(epfd int, op int, fd int, event *Event) error {
	var buf [epollEventSize]byte
	binary.NativeEndian.PutUint32(buf[0:4], event.Events)
	binary.NativeEndian.PutUint64(buf[4:12], event.Data)

	_, _, errno := syscall.Syscall6(syscall.SYS_EPOLL_CTL,
		uintptr(epfd),
		uintptr(op),
		uintptr(fd),
		uintptr(unsafe.Pointer(&buf[0])),
		0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// epollWait 等待epoll事件
// rawEvents 为内核 packed 布局的原始缓冲区（12字节/事件），
// 由调用方(Wait方法)解码到 Event 数组
func epollWait(epfd int, rawEvents []byte, timeout int) (int, error) {
	n, _, errno := syscall.Syscall6(syscall.SYS_EPOLL_WAIT,
		uintptr(epfd),
		uintptr(unsafe.Pointer(&rawEvents[0])),
		uintptr(len(rawEvents)/epollEventSize),
		uintptr(timeout),
		0, 0)
	if errno != 0 {
		return -1, errno
	}
	return int(n), nil
}
