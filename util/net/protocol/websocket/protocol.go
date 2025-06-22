package websocket

import (
	"liveChatroom/util/net/protocol"
	"sync"
)

// WebSocketProtocol WebSocket协议实现
type WebSocketProtocol struct {
	mu       sync.RWMutex
	parser   *WebSocketParser
	builder  *WebSocketBuilder
	isServer bool
	closed   bool
}

// NewWebSocketProtocol 创建WebSocket协议处理器
func NewWebSocketProtocol(isServer bool) protocol.Protocol {
	return &WebSocketProtocol{
		parser:   NewWebSocketParser(isServer),
		builder:  NewWebSocketBuilder(isServer),
		isServer: isServer,
	}
}

// GetType 实现Protocol接口
func (p *WebSocketProtocol) GetType() protocol.ProtocolType {
	return protocol.ProtocolWebSocket
}

// GetParser 实现Protocol接口
func (p *WebSocketProtocol) GetParser() protocol.Parser {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.parser
}

// GetBuilder 实现Protocol接口
func (p *WebSocketProtocol) GetBuilder() protocol.Builder {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.builder
}

// CanHandle 实现Protocol接口
func (p *WebSocketProtocol) CanHandle(data []byte) bool {
	// WebSocket协议通过HTTP升级建立，不直接检测原始数据
	// 这个方法主要用于协议升级后的数据检测
	if len(data) < 2 {
		return false
	}

	// 简单检查：第一个字节应该包含有效的opcode
	firstByte := data[0]
	opCode := firstByte & 0x0F

	return isValidOpCode(opCode)
}

// HandleUpgrade 实现Protocol接口
func (p *WebSocketProtocol) HandleUpgrade(request protocol.Request) (protocol.Protocol, protocol.Response, error) {
	// WebSocket协议本身不支持进一步升级
	return nil, nil, &protocol.ProtocolError{
		Type:    protocol.ProtocolWebSocket,
		Code:    protocol.ErrCodeNotSupported,
		Message: "WebSocket protocol does not support further upgrades",
	}
}

// IsUpgradeRequest 实现Protocol接口
func (p *WebSocketProtocol) IsUpgradeRequest(request protocol.Request) bool {
	// WebSocket协议没有升级请求
	return false
}

// GetUpgradeTarget 实现Protocol接口
func (p *WebSocketProtocol) GetUpgradeTarget(request protocol.Request) protocol.ProtocolType {
	return protocol.ProtocolUnknown
}

// Close 实现Protocol接口
func (p *WebSocketProtocol) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	var err error
	if p.parser != nil {
		err = p.parser.Close()
		p.parser = nil
	}

	if p.builder != nil {
		builderErr := p.builder.Close()
		if err == nil {
			err = builderErr
		}
		p.builder = nil
	}

	p.closed = true
	return err
}

// Clone 实现Protocol接口
func (p *WebSocketProtocol) Clone() protocol.Protocol {
	return NewWebSocketProtocol(p.isServer)
}

// =================  WebSocket特定方法 =================

// IsServer 检查是否为服务端模式
func (p *WebSocketProtocol) IsServer() bool {
	return p.isServer
}

// GetParser 获取WebSocket解析器（类型安全）
func (p *WebSocketProtocol) GetWebSocketParser() *WebSocketParser {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.parser
}

// GetBuilder 获取WebSocket构建器（类型安全）
func (p *WebSocketProtocol) GetWebSocketBuilder() *WebSocketBuilder {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.builder
}

// =================  注册WebSocket协议 =================

func init() {
	// 注册WebSocket协议到全局注册表（服务端模式）
	protocol.Register(protocol.ProtocolWebSocket, func() protocol.Protocol {
		return NewWebSocketProtocol(true)
	})
}

// =================  便利函数 =================

// NewServerWebSocketProtocol 创建服务端WebSocket协议处理器
func NewServerWebSocketProtocol() protocol.Protocol {
	return NewWebSocketProtocol(true)
}

// NewClientWebSocketProtocol 创建客户端WebSocket协议处理器
func NewClientWebSocketProtocol() protocol.Protocol {
	return NewWebSocketProtocol(false)
}
