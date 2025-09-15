package socket

import (
	"net"
	"strconv"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// Socket 高性能socket封装
type Socket struct {
	fd     int64 // 原子访问（Close与并发IO竞争）
	family int
	sotype int
}

// NewSocket 创建socket
func NewSocket(family, sotype, proto int) (*Socket, error) {
	fd, err := syscall.Socket(family, sotype, proto)
	if err != nil {
		return nil, err
	}

	s := &Socket{
		fd:     int64(fd),
		family: family,
		sotype: sotype,
	}

	// 设置为非阻塞模式
	if err := s.SetNonblock(); err != nil {
		syscall.Close(fd)
		return nil, err
	}

	return s, nil
}

// NewTCPSocket 创建TCP socket
func NewTCPSocket() (*Socket, error) {
	return NewSocket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
}

// NewUDPSocket 创建UDP socket
func NewUDPSocket() (*Socket, error) {
	return NewSocket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
}

// FromFD 从文件描述符创建Socket
func FromFD(fd int) *Socket {
	return &Socket{
		fd:     int64(fd),
		family: syscall.AF_INET,
		sotype: syscall.SOCK_STREAM,
	}
}

// ==================== 基础操作 ====================

// FD 获取文件描述符
func (s *Socket) FD() int {
	return s.fdVal()
}

// fdVal 原子读取文件描述符
func (s *Socket) fdVal() int {
	return int(atomic.LoadInt64(&s.fd))
}

// Close 关闭socket
// 用原子交换保证幂等，并与并发读fd的IO操作无数据竞争
func (s *Socket) Close() error {
	fd := atomic.SwapInt64(&s.fd, -1)
	if fd < 0 {
		return nil
	}
	return syscall.Close(int(fd))
}

// ==================== 非阻塞I/O ====================

// SetNonblock 设置非阻塞模式
func (s *Socket) SetNonblock() error {
	return setNonblock(s.fdVal())
}

// Read 非阻塞读取
func (s *Socket) Read(buf []byte) (int, error) {
	n, err := syscall.Read(s.fdVal(), buf)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Write 非阻塞写入
func (s *Socket) Write(buf []byte) (int, error) {
	n, err := syscall.Write(s.fdVal(), buf)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Readv 向量读取（零拷贝）
func (s *Socket) Readv(iovecs []syscall.Iovec) (int, error) {
	n, _, errno := syscall.Syscall(syscall.SYS_READV,
		uintptr(s.fdVal()),
		uintptr(unsafe.Pointer(&iovecs[0])),
		uintptr(len(iovecs)))
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

// Writev 向量写入（零拷贝）
func (s *Socket) Writev(iovecs []syscall.Iovec) (int, error) {
	n, _, errno := syscall.Syscall(syscall.SYS_WRITEV,
		uintptr(s.fdVal()),
		uintptr(unsafe.Pointer(&iovecs[0])),
		uintptr(len(iovecs)))
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

// ==================== TCP服务器操作 ====================

// Bind 绑定地址
func (s *Socket) Bind(addr string) error {
	sockAddr, err := parseAddr(addr)
	if err != nil {
		return err
	}

	return syscall.Bind(s.fdVal(), sockAddr)
}

// Listen 开始监听
func (s *Socket) Listen(backlog int) error {
	return syscall.Listen(s.fdVal(), backlog)
}

// Accept 接受连接
func (s *Socket) Accept() (*Socket, string, error) {
	fd, sockAddr, err := syscall.Accept(s.fdVal())
	if err != nil {
		return nil, "", err
	}

	// 设置新连接为非阻塞模式
	if err := setNonblock(fd); err != nil {
		syscall.Close(fd)
		return nil, "", err
	}

	conn := &Socket{
		fd:     int64(fd),
		family: s.family,
		sotype: s.sotype,
	}

	addr := sockAddrToString(sockAddr)
	return conn, addr, nil
}

// ==================== TCP客户端操作 ====================

// Connect 连接到远程地址
func (s *Socket) Connect(addr string) error {
	sockAddr, err := parseAddr(addr)
	if err != nil {
		return err
	}

	err = syscall.Connect(s.fdVal(), sockAddr)
	if err != nil && err != syscall.EINPROGRESS {
		return err
	}
	return nil
}

// ==================== Socket选项设置 ====================

// SetReuseAddr 设置地址重用
func (s *Socket) SetReuseAddr(reuse bool) error {
	return s.setSockoptInt(syscall.SOL_SOCKET, syscall.SO_REUSEADDR, boolToInt(reuse))
}

// SetReusePort 设置端口重用
func (s *Socket) SetReusePort(reuse bool) error {
	return s.setSockoptInt(syscall.SOL_SOCKET, 0xf, boolToInt(reuse)) // SO_REUSEPORT = 0xf
}

// SetKeepAlive 设置保活
func (s *Socket) SetKeepAlive(keepalive bool) error {
	return s.setSockoptInt(syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, boolToInt(keepalive))
}

// SetTCPNoDelay 设置TCP_NODELAY
func (s *Socket) SetTCPNoDelay(nodelay bool) error {
	return s.setSockoptInt(syscall.IPPROTO_TCP, syscall.TCP_NODELAY, boolToInt(nodelay))
}

// SetSendBuffer 设置发送缓冲区大小
func (s *Socket) SetSendBuffer(size int) error {
	return s.setSockoptInt(syscall.SOL_SOCKET, syscall.SO_SNDBUF, size)
}

// SetRecvBuffer 设置接收缓冲区大小
func (s *Socket) SetRecvBuffer(size int) error {
	return s.setSockoptInt(syscall.SOL_SOCKET, syscall.SO_RCVBUF, size)
}

// SetLinger 设置Linger选项
func (s *Socket) SetLinger(onoff bool, timeout int) error {
	linger := syscall.Linger{
		Onoff:  int32(boolToInt(onoff)),
		Linger: int32(timeout),
	}
	return s.setSockoptLinger(syscall.SOL_SOCKET, syscall.SO_LINGER, &linger)
}

// ==================== 内部辅助方法 ====================

// setSockoptInt 设置int类型socket选项
func (s *Socket) setSockoptInt(level, opt, value int) error {
	return syscall.SetsockoptInt(s.fdVal(), level, opt, value)
}

// setSockoptLinger 设置Linger选项
func (s *Socket) setSockoptLinger(level, opt int, linger *syscall.Linger) error {
	return syscall.SetsockoptLinger(s.fdVal(), level, opt, linger)
}

// ==================== 工具函数 ====================

// setNonblock 设置文件描述符为非阻塞模式
func setNonblock(fd int) error {
	flags, err := fcntl(fd, syscall.F_GETFL, 0)
	if err != nil {
		return err
	}
	_, err = fcntl(fd, syscall.F_SETFL, flags|syscall.O_NONBLOCK)
	return err
}

// fcntl 文件描述符控制
func fcntl(fd int, cmd int, arg int) (int, error) {
	ret, _, errno := syscall.Syscall(syscall.SYS_FCNTL,
		uintptr(fd), uintptr(cmd), uintptr(arg))
	if errno != 0 {
		return -1, errno
	}
	return int(ret), nil
}

// parseAddr 解析地址字符串
func parseAddr(addr string) (syscall.Sockaddr, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	ip4 := tcpAddr.IP.To4()
	if ip4 == nil {
		// 暂时只支持IPv4
		return nil, syscall.EAFNOSUPPORT
	}

	sockAddr := &syscall.SockaddrInet4{
		Port: tcpAddr.Port,
	}
	copy(sockAddr.Addr[:], ip4)

	return sockAddr, nil
}

// sockAddrToString 将sockaddr转换为字符串
func sockAddrToString(sockAddr syscall.Sockaddr) string {
	switch addr := sockAddr.(type) {
	case *syscall.SockaddrInet4:
		ip := net.IPv4(addr.Addr[0], addr.Addr[1], addr.Addr[2], addr.Addr[3])
		return net.JoinHostPort(ip.String(), strconv.Itoa(addr.Port))
	case *syscall.SockaddrInet6:
		ip := net.IP(addr.Addr[:])
		return net.JoinHostPort(ip.String(), strconv.Itoa(addr.Port))
	default:
		return "unknown"
	}
}

// boolToInt 布尔值转整数
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
