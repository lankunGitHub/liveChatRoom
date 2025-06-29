package connection

import (
	"fmt"
	"liveChatroom/util/net/base/buffer"
	"liveChatroom/util/net/base/io"
	"liveChatroom/util/net/base/socket"
	"liveChatroom/util/net/base/timer"
	"liveChatroom/util/net/protocol"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// State 连接状态
type State int

const (
	StateConnecting State = iota
	StateConnected
	StateClosing
	StateClosed
)

// Connection 高性能网络连接
type Connection struct {
	mu sync.RWMutex

	// 基础信息
	id         string
	socket     *socket.Socket
	localAddr  string
	remoteAddr string

	// 状态管理
	state      State
	closed     int32
	createdAt  time.Time
	lastActive time.Time

	// I/O组件
	reader      *io.Reader
	writer      *io.Writer
	readStream  *io.ReadStream
	writeStream *io.WriteStream

	// 缓冲区
	readBuffer  buffer.Buffer
	writeBuffer buffer.Buffer

	// 协议处理
	protocolType    protocol.ProtocolType
	protocolHandler protocol.Protocol

	// 事件处理
	onRead    func(*Connection, []byte)
	onWrite   func(*Connection, []byte)
	onClose   func(*Connection)
	onError   func(*Connection, error)
	onMessage func(*Connection, protocol.Message)
	onUpgrade func(*Connection, protocol.Protocol)

	// 定时器
	readTimeout  time.Duration
	writeTimeout time.Duration
	idleTimeout  time.Duration
	timeoutTimer uint64

	// 连接选项
	keepAlive  bool
	tcpNoDelay bool

	// 内部状态
	upgrading bool
}

// NewConnection 创建新连接
func NewConnection(sock *socket.Socket, localAddr, remoteAddr string) *Connection {
	conn := &Connection{
		id:         generateConnectionID(),
		socket:     sock,
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
		state:      StateConnected,
		createdAt:  time.Now(),
		lastActive: time.Now(),

		// 创建I/O组件
		reader:      io.NewReader(sock.FD()),
		writer:      io.NewWriter(sock.FD()),
		readStream:  io.NewReadStream(sock.FD(), 8192),
		writeStream: io.NewWriteStream(sock.FD(), 8192),

		// 创建缓冲区
		readBuffer:  buffer.Get(8192),
		writeBuffer: buffer.Get(8192),

		// 默认超时设置
		readTimeout:  30 * time.Second,
		writeTimeout: 30 * time.Second,
		idleTimeout:  5 * time.Minute,

		// 默认选项
		keepAlive:  true,
		tcpNoDelay: true,
	}

	// 配置socket选项
	conn.configureSocket()

	// 启动超时检测
	conn.startIdleTimer()

	return conn
}

// ==================== 基础信息 ====================

// ID 获取连接ID
func (c *Connection) ID() string {
	return c.id
}

// LocalAddr 获取本地地址
func (c *Connection) LocalAddr() string {
	return c.localAddr
}

// RemoteAddr 获取远程地址
func (c *Connection) RemoteAddr() string {
	return c.remoteAddr
}

// FD 获取文件描述符
func (c *Connection) FD() int {
	return c.socket.FD()
}

// ==================== 状态管理 ====================

// State 获取连接状态
func (c *Connection) State() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// IsClosed 检查连接是否已关闭
func (c *Connection) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// GetProtocolType 获取当前协议类型
func (c *Connection) GetProtocolType() protocol.ProtocolType {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.protocolType
}

// ==================== I/O操作 ====================

// Read 读取数据 - 自动协议检测和解析
func (c *Connection) Read() error {
	if c.IsClosed() {
		return fmt.Errorf("connection closed")
	}

	// 读取数据到缓冲区
	n, err := c.reader.ReadToBuffer(c.readBuffer)
	if err != nil {
		c.handleError(err)
		return err
	}

	if n > 0 {
		c.updateActivity()

		// 触发读取事件
		if c.onRead != nil {
			data, _ := c.readBuffer.Peek(n)
			c.onRead(c, data)
		}

		// 处理协议数据
		return c.processData()
	}

	return nil
}

// Write 写入数据
func (c *Connection) Write(data []byte) (int, error) {
	if c.IsClosed() {
		return 0, fmt.Errorf("connection closed")
	}

	if len(data) == 0 {
		return 0, nil
	}

	// 写入数据
	n, err := c.writer.Write(data)
	if err != nil {
		c.handleError(err)
		return 0, err
	}

	if n > 0 {
		c.updateActivity()

		// 触发写入事件
		if c.onWrite != nil {
			c.onWrite(c, data[:n])
		}
	}

	return n, nil
}

// WriteBuffered 写入数据到缓冲区
func (c *Connection) WriteBuffered(data []byte) error {
	if c.IsClosed() {
		return fmt.Errorf("connection closed")
	}

	_, err := c.writeBuffer.Write(data)
	return err
}

// Flush 刷新写入缓冲区
func (c *Connection) Flush() error {
	if c.IsClosed() {
		return fmt.Errorf("connection closed")
	}

	data := c.writeBuffer.ReadAll()
	if len(data) == 0 {
		return nil
	}

	_, err := c.Write(data)
	return err
}

// ==================== 高级I/O操作 ====================

// Writev 向量写入 - 零拷贝
func (c *Connection) Writev(data [][]byte) (int, error) {
	if c.IsClosed() {
		return 0, fmt.Errorf("connection closed")
	}

	n, err := c.writer.WriteBatch(data)
	if err != nil {
		c.handleError(err)
		return 0, err
	}

	if n > 0 {
		c.updateActivity()
	}

	return n, nil
}

// SendFile 发送文件 - 零拷贝
func (c *Connection) SendFile(fileFD int, offset int64, count int) (int, error) {
	if c.IsClosed() {
		return 0, fmt.Errorf("connection closed")
	}

	n, err := io.Sendfile(c.socket.FD(), fileFD, &offset, count)
	if err != nil {
		c.handleError(err)
		return 0, err
	}

	if n > 0 {
		c.updateActivity()
	}

	return n, nil
}

// ==================== HTTP协议操作 ====================

// ReadMessage 读取协议消息
func (c *Connection) ReadMessage() ([]protocol.Message, error) {
	// 自动检测协议类型
	if err := c.detectAndSetupProtocol(); err != nil {
		return nil, err
	}

	if c.protocolHandler == nil {
		return nil, fmt.Errorf("no protocol handler available")
	}

	// 使用协议解析器解析消息
	messages, err := c.protocolHandler.GetParser().Parse(c.readBuffer)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

// detectAndSetupProtocol 检测并设置协议处理器
func (c *Connection) detectAndSetupProtocol() error {
	if c.protocolType != protocol.ProtocolUnknown && c.protocolHandler != nil {
		return nil // 已经设置
	}

	// 获取数据进行协议检测
	data, err := c.readBuffer.Peek(1024)
	if err != nil || len(data) == 0 {
		return fmt.Errorf("insufficient data for protocol detection")
	}

	// 检测协议类型
	detectedType := protocol.DetectProtocol(data)
	if detectedType == protocol.ProtocolUnknown {
		return fmt.Errorf("unknown protocol")
	}

	// 获取协议处理器
	handler, err := protocol.Get(detectedType)
	if err != nil {
		return fmt.Errorf("failed to get protocol handler: %v", err)
	}

	// 设置协议
	c.protocolType = detectedType
	c.protocolHandler = handler

	return nil
}

// WriteResponse 写入响应消息
func (c *Connection) WriteResponse(statusCode int, contentType string, body []byte) error {
	if c.protocolHandler == nil {
		return fmt.Errorf("no protocol handler available")
	}

	// 获取状态文本
	statusText := getStatusText(statusCode)

	// 构建头部
	headers := make(map[string]string)
	if contentType != "" {
		headers["Content-Type"] = contentType
	}

	// 构建响应消息
	data, err := c.protocolHandler.GetBuilder().BuildResponse(statusCode, statusText, headers, body)
	if err != nil {
		return err
	}

	_, err = c.Write(data)
	return err
}

// getStatusText 获取状态码对应的文本
func getStatusText(code int) string {
	switch code {
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 204:
		return "No Content"
	case 301:
		return "Moved Permanently"
	case 302:
		return "Found"
	case 304:
		return "Not Modified"
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 405:
		return "Method Not Allowed"
	case 500:
		return "Internal Server Error"
	case 501:
		return "Not Implemented"
	case 502:
		return "Bad Gateway"
	case 503:
		return "Service Unavailable"
	default:
		return "Unknown"
	}
}

// ==================== 协议操作 ====================

// UpgradeProtocol 协议升级
func (c *Connection) UpgradeProtocol(request protocol.Request) (protocol.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != StateConnected {
		return nil, fmt.Errorf("connection not in connected state")
	}

	if c.protocolHandler == nil {
		return nil, fmt.Errorf("no protocol handler available")
	}

	// 处理协议升级
	newProtocol, response, err := c.protocolHandler.HandleUpgrade(request)
	if err != nil {
		return nil, fmt.Errorf("protocol upgrade failed: %v", err)
	}

	// 更新协议处理器
	oldProtocol := c.protocolHandler
	c.protocolHandler = newProtocol
	c.protocolType = newProtocol.GetType()
	c.upgrading = false

	// 关闭旧协议处理器
	if oldProtocol != nil {
		oldProtocol.Close()
	}

	// 触发升级事件
	if c.onUpgrade != nil {
		c.onUpgrade(c, newProtocol)
	}

	return response, nil
}

// WriteMessage 写入协议消息
func (c *Connection) WriteMessage(msg protocol.Message) error {
	if c.protocolHandler == nil {
		return fmt.Errorf("no protocol handler available")
	}

	data, err := c.protocolHandler.GetBuilder().BuildMessage(msg)
	if err != nil {
		return err
	}

	_, err = c.Write(data)
	return err
}

// WriteError 写入错误消息
func (c *Connection) WriteError(err error) error {
	if c.protocolHandler == nil {
		return fmt.Errorf("no protocol handler available")
	}

	data, buildErr := c.protocolHandler.GetBuilder().BuildError(500, err.Error())
	if buildErr != nil {
		return buildErr
	}

	_, writeErr := c.Write(data)
	return writeErr
}

// ==================== 协议数据处理 ====================

// processData 处理协议数据
func (c *Connection) processData() error {
	// 自动检测并设置协议
	if err := c.detectAndSetupProtocol(); err != nil {
		return err
	}

	// 解析消息
	messages, err := c.ReadMessage()
	if err != nil {
		return err
	}

	// 处理每个消息
	for _, msg := range messages {
		if c.onMessage != nil {
			c.onMessage(c, msg)
		}
	}

	return nil
}

// ==================== 内部方法 ====================

// configureSocket 配置socket选项
func (c *Connection) configureSocket() {
	if c.keepAlive {
		c.socket.SetKeepAlive(true)
	}
	if c.tcpNoDelay {
		c.socket.SetTCPNoDelay(true)
	}
}

// updateActivity 更新活动时间
func (c *Connection) updateActivity() {
	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()

	// 重置空闲定时器
	c.resetIdleTimer()
}

// startIdleTimer 启动空闲定时器
func (c *Connection) startIdleTimer() {
	if c.idleTimeout > 0 {
		c.timeoutTimer = timer.After(c.idleTimeout, func() {
			c.handleIdleTimeout()
		})
	}
}

// resetIdleTimer 重置空闲定时器
func (c *Connection) resetIdleTimer() {
	if c.timeoutTimer != 0 {
		timer.Cancel(c.timeoutTimer)
	}
	c.startIdleTimer()
}

// handleIdleTimeout 处理空闲超时
func (c *Connection) handleIdleTimeout() {
	if c.IsClosed() {
		return
	}

	c.mu.RLock()
	lastActive := c.lastActive
	c.mu.RUnlock()

	// 检查是否真的超时
	if time.Since(lastActive) >= c.idleTimeout {
		c.handleError(fmt.Errorf("connection idle timeout"))
		c.Close()
	} else {
		// 重新设置定时器
		c.resetIdleTimer()
	}
}

// handleError 处理错误
func (c *Connection) handleError(err error) {
	if c.onError != nil {
		c.onError(c, err)
	}
}

// Close 关闭连接
func (c *Connection) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil // 已经关闭
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新状态
	c.state = StateClosed

	// 取消定时器
	if c.timeoutTimer != 0 {
		timer.Cancel(c.timeoutTimer)
	}

	// 关闭socket
	err := c.socket.Close()

	// 关闭流
	if c.readStream != nil {
		c.readStream.Close()
	}
	if c.writeStream != nil {
		c.writeStream.Close()
	}

	// 归还缓冲区
	if c.readBuffer != nil {
		buffer.Put(c.readBuffer)
	}
	if c.writeBuffer != nil {
		buffer.Put(c.writeBuffer)
	}

	// 关闭协议处理器
	if c.protocolHandler != nil {
		c.protocolHandler.Close()
		c.protocolHandler = nil
	}

	// 触发关闭事件
	if c.onClose != nil {
		c.onClose(c)
	}

	return err
}

// ==================== 事件处理器设置 ====================

// OnRead 设置读取事件处理器
func (c *Connection) OnRead(handler func(*Connection, []byte)) {
	c.onRead = handler
}

// OnWrite 设置写入事件处理器
func (c *Connection) OnWrite(handler func(*Connection, []byte)) {
	c.onWrite = handler
}

// OnClose 设置关闭事件处理器
func (c *Connection) OnClose(handler func(*Connection)) {
	c.onClose = handler
}

// OnError 设置错误事件处理器
func (c *Connection) OnError(handler func(*Connection, error)) {
	c.onError = handler
}

// OnMessage 设置消息事件处理器
func (c *Connection) OnMessage(handler func(*Connection, protocol.Message)) {
	c.onMessage = handler
}

// OnUpgrade 设置协议升级事件处理器
func (c *Connection) OnUpgrade(handler func(*Connection, protocol.Protocol)) {
	c.onUpgrade = handler
}

// ==================== 配置选项 ====================

// SetReadTimeout 设置读取超时
func (c *Connection) SetReadTimeout(timeout time.Duration) {
	c.readTimeout = timeout
}

// SetWriteTimeout 设置写入超时
func (c *Connection) SetWriteTimeout(timeout time.Duration) {
	c.writeTimeout = timeout
}

// SetIdleTimeout 设置空闲超时
func (c *Connection) SetIdleTimeout(timeout time.Duration) {
	c.idleTimeout = timeout
	c.resetIdleTimer()
}

// SetKeepAlive 设置KeepAlive
func (c *Connection) SetKeepAlive(keepAlive bool) {
	c.keepAlive = keepAlive
	c.socket.SetKeepAlive(keepAlive)
}

// SetTCPNoDelay 设置TCP_NODELAY
func (c *Connection) SetTCPNoDelay(noDelay bool) {
	c.tcpNoDelay = noDelay
	c.socket.SetTCPNoDelay(noDelay)
}

// ==================== 工具函数 ====================

// generateConnectionID 生成连接ID
func generateConnectionID() string {
	return fmt.Sprintf("conn_%d_%d", time.Now().UnixNano(), rand.Int63())
}
