package websocket

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"liveChatroom/util/net/base/buffer"
	"liveChatroom/util/net/protocol"
	"sync"
	"unicode/utf8"
)

// WebSocket操作码
const (
	OpCodeContinuation = 0x0
	OpCodeText         = 0x1
	OpCodeBinary       = 0x2
	OpCodeClose        = 0x8
	OpCodePing         = 0x9
	OpCodePong         = 0xa
)

// WebSocket关闭代码
const (
	CloseNormalClosure    = 1000
	CloseGoingAway        = 1001
	CloseProtocolError    = 1002
	CloseUnsupportedData  = 1003
	CloseNoStatusRcvd     = 1005
	CloseAbnormalClosure  = 1006
	CloseInvalidFrameData = 1007
	ClosePolicyViolation  = 1008
	CloseMessageTooBig    = 1009
	CloseMandatoryExt     = 1010
	CloseInternalError    = 1011
	CloseTLSHandshake     = 1015
)

// =================  消息实现 =================

// WebSocketMessage WebSocket消息
type WebSocketMessage struct {
	protocol.BaseMessage
	OpCode      uint8  `json:"opcode"`
	Masked      bool   `json:"masked"`
	MaskKey     []byte `json:"mask_key,omitempty"`
	Final       bool   `json:"final"`
	CloseCode   uint16 `json:"close_code,omitempty"`
	CloseReason string `json:"close_reason,omitempty"`
}

// GetOpCode 获取操作码
func (m *WebSocketMessage) GetOpCode() uint8 {
	return m.OpCode
}

// IsControlFrame 是否为控制帧
func (m *WebSocketMessage) IsControlFrame() bool {
	return m.OpCode >= 0x8
}

// IsDataFrame 是否为数据帧
func (m *WebSocketMessage) IsDataFrame() bool {
	return m.OpCode <= 0x2
}

// Clone 实现Message接口
func (m *WebSocketMessage) Clone() protocol.Message {
	clone := &WebSocketMessage{
		OpCode:      m.OpCode,
		Masked:      m.Masked,
		Final:       m.Final,
		CloseCode:   m.CloseCode,
		CloseReason: m.CloseReason,
	}

	if m.MaskKey != nil {
		clone.MaskKey = make([]byte, len(m.MaskKey))
		copy(clone.MaskKey, m.MaskKey)
	}

	// 复制基础消息字段
	clone.BaseMessage = *(m.BaseMessage.Clone().(*protocol.BaseMessage))

	return clone
}

// =================  解析器实现 =================

// WebSocketParser WebSocket协议解析器
type WebSocketParser struct {
	mu           sync.Mutex
	buffer       []byte
	isServer     bool
	maxFrameSize int64
	maxMsgSize   int64

	// 分片消息状态
	fragmentBuffer []byte
	fragmentOpCode uint8
}

// NewWebSocketParser 创建WebSocket解析器
func NewWebSocketParser(isServer bool) *WebSocketParser {
	return &WebSocketParser{
		isServer:     isServer,
		maxFrameSize: 65536,   // 64KB
		maxMsgSize:   1048576, // 1MB
	}
}

// GetProtocolType 实现Parser接口
func (p *WebSocketParser) GetProtocolType() protocol.ProtocolType {
	return protocol.ProtocolWebSocket
}

// Feed 实现Parser接口
func (p *WebSocketParser) Feed(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = append(p.buffer, data...)

	// 检查缓冲区大小
	if int64(len(p.buffer)) > p.maxFrameSize {
		return &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeParseError,
			Message: "frame too large",
		}
	}

	return nil
}

// Parse 实现Parser接口
func (p *WebSocketParser) Parse(buf buffer.Buffer) ([]protocol.Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 从缓冲区读取数据
	data := buf.ReadAll()
	if len(data) > 0 {
		p.buffer = append(p.buffer, data...)
	}

	var messages []protocol.Message

	for len(p.buffer) >= 2 {
		msg, consumed, err := p.parseFrame()
		if err != nil {
			return nil, err
		}

		if consumed == 0 {
			break // 需要更多数据
		}

		if msg != nil {
			messages = append(messages, msg)
		}

		// 移除已解析的数据
		p.buffer = p.buffer[consumed:]
	}

	return messages, nil
}

// parseFrame 解析WebSocket帧
func (p *WebSocketParser) parseFrame() (protocol.Message, int, error) {
	if len(p.buffer) < 2 {
		return nil, 0, nil
	}

	// 第一个字节：FIN + RSV + OpCode
	firstByte := p.buffer[0]
	fin := (firstByte & 0x80) != 0
	opCode := firstByte & 0x0F

	// 第二个字节：MASK + Payload Length
	secondByte := p.buffer[1]
	masked := (secondByte & 0x80) != 0
	payloadLen := int64(secondByte & 0x7F)

	headerLen := 2

	// 验证掩码
	if p.isServer && !masked {
		return nil, 0, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeProtocolError,
			Message: "client frames must be masked",
		}
	}
	if !p.isServer && masked {
		return nil, 0, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeProtocolError,
			Message: "server frames must not be masked",
		}
	}

	// 扩展载荷长度
	if payloadLen == 126 {
		if len(p.buffer) < 4 {
			return nil, 0, nil
		}
		payloadLen = int64(binary.BigEndian.Uint16(p.buffer[2:4]))
		headerLen = 4
	} else if payloadLen == 127 {
		if len(p.buffer) < 10 {
			return nil, 0, nil
		}
		rawLen := binary.BigEndian.Uint64(p.buffer[2:10])
		// RFC 6455: 64位长度的最高位必须为0。
		// 若最高位为1，转成int64后为负数，后续 make([]byte, 负数) 会直接 panic
		if rawLen&(1<<63) != 0 {
			return nil, 0, &protocol.ProtocolError{
				Type:    protocol.ProtocolWebSocket,
				Code:    protocol.ErrCodeInvalidFrameData,
				Message: "invalid 64-bit payload length: MSB must be 0",
			}
		}
		payloadLen = int64(rawLen)
		headerLen = 10
	}

	// 检查载荷长度
	if payloadLen > p.maxFrameSize {
		return nil, 0, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeParseError,
			Message: fmt.Sprintf("frame payload too large: %d", payloadLen),
		}
	}

	// 掩码密钥
	var maskKey []byte
	if masked {
		if len(p.buffer) < headerLen+4 {
			return nil, 0, nil
		}
		maskKey = make([]byte, 4)
		copy(maskKey, p.buffer[headerLen:headerLen+4])
		headerLen += 4
	}

	// 检查是否有足够的数据
	totalLen := headerLen + int(payloadLen)
	if len(p.buffer) < totalLen {
		return nil, 0, nil
	}

	// 提取载荷
	payload := make([]byte, payloadLen)
	copy(payload, p.buffer[headerLen:totalLen])

	// 解除掩码
	if masked && maskKey != nil {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}

	// 验证帧
	if err := p.validateFrame(opCode, payload, fin); err != nil {
		return nil, 0, err
	}

	// 处理分片
	if opCode == OpCodeContinuation || (!fin && (opCode == OpCodeText || opCode == OpCodeBinary)) {
		return p.handleFragmentation(opCode, payload, fin, maskKey, totalLen)
	}

	// 创建消息
	msg := &WebSocketMessage{
		OpCode:  opCode,
		Masked:  masked,
		MaskKey: maskKey,
		Final:   fin,
	}

	msg.Type = p.getMessageType(opCode)
	msg.Payload = payload

	// 处理关闭帧
	if opCode == OpCodeClose {
		p.parseCloseFrame(msg, payload)
	}

	return msg, totalLen, nil
}

// validateFrame 验证帧
func (p *WebSocketParser) validateFrame(opCode uint8, payload []byte, fin bool) error {
	// 验证操作码
	if !isValidOpCode(opCode) {
		return &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeProtocolError,
			Message: fmt.Sprintf("invalid opcode: %d", opCode),
		}
	}

	// 验证控制帧
	if isControlFrame(opCode) {
		if !fin {
			return &protocol.ProtocolError{
				Type:    protocol.ProtocolWebSocket,
				Code:    protocol.ErrCodeProtocolError,
				Message: "control frames must not be fragmented",
			}
		}
		if len(payload) > 125 {
			return &protocol.ProtocolError{
				Type:    protocol.ProtocolWebSocket,
				Code:    protocol.ErrCodeProtocolError,
				Message: "control frame payload too large",
			}
		}
	}

	// 验证文本帧UTF-8编码
	if opCode == OpCodeText && !utf8.Valid(payload) {
		return &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeInvalidFrameData,
			Message: "text frame contains invalid UTF-8",
		}
	}

	return nil
}

// handleFragmentation 处理分片消息
func (p *WebSocketParser) handleFragmentation(opCode uint8, payload []byte, fin bool, maskKey []byte, totalLen int) (protocol.Message, int, error) {
	if opCode != OpCodeContinuation {
		// 开始新的分片消息
		if len(p.fragmentBuffer) > 0 {
			return nil, 0, &protocol.ProtocolError{
				Type:    protocol.ProtocolWebSocket,
				Code:    protocol.ErrCodeProtocolError,
				Message: "unexpected fragmented frame start",
			}
		}
		p.fragmentOpCode = opCode
		p.fragmentBuffer = make([]byte, 0, len(payload)*2)
	}

	// 添加到分片缓冲区
	p.fragmentBuffer = append(p.fragmentBuffer, payload...)

	// 检查分片消息大小
	if int64(len(p.fragmentBuffer)) > p.maxMsgSize {
		p.fragmentBuffer = p.fragmentBuffer[:0]
		return nil, 0, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeParseError,
			Message: "fragmented message too large",
		}
	}

	if fin {
		// 分片结束，创建完整消息
		msg := &WebSocketMessage{
			OpCode:  p.fragmentOpCode,
			Masked:  len(maskKey) > 0,
			MaskKey: maskKey,
			Final:   true,
		}

		msg.Type = p.getMessageType(p.fragmentOpCode)
		msg.Payload = make([]byte, len(p.fragmentBuffer))
		copy(msg.Payload, p.fragmentBuffer)

		// 验证完整消息
		if p.fragmentOpCode == OpCodeText && !utf8.Valid(msg.Payload) {
			p.fragmentBuffer = p.fragmentBuffer[:0]
			return nil, 0, &protocol.ProtocolError{
				Type:    protocol.ProtocolWebSocket,
				Code:    protocol.ErrCodeInvalidFrameData,
				Message: "fragmented text message contains invalid UTF-8",
			}
		}

		// 重置分片状态
		p.fragmentBuffer = p.fragmentBuffer[:0]
		p.fragmentOpCode = 0

		return msg, totalLen, nil
	}

	return nil, totalLen, nil
}

// parseCloseFrame 解析关闭帧
func (p *WebSocketParser) parseCloseFrame(msg *WebSocketMessage, payload []byte) {
	if len(payload) >= 2 {
		msg.CloseCode = binary.BigEndian.Uint16(payload[:2])
		if len(payload) > 2 {
			msg.CloseReason = string(payload[2:])
		}
	} else {
		msg.CloseCode = CloseNoStatusRcvd
	}
}

// getMessageType 获取消息类型
func (p *WebSocketParser) getMessageType(opCode uint8) string {
	switch opCode {
	case OpCodeText:
		return "websocket_text"
	case OpCodeBinary:
		return "websocket_binary"
	case OpCodeClose:
		return "websocket_close"
	case OpCodePing:
		return "websocket_ping"
	case OpCodePong:
		return "websocket_pong"
	default:
		return "websocket_unknown"
	}
}

// HasPendingMessage 实现Parser接口
func (p *WebSocketParser) HasPendingMessage() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buffer) >= 2
}

// Reset 实现Parser接口
func (p *WebSocketParser) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = p.buffer[:0]
	p.fragmentBuffer = p.fragmentBuffer[:0]
	p.fragmentOpCode = 0
}

// Close 实现Parser接口
func (p *WebSocketParser) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = nil
	p.fragmentBuffer = nil
	return nil
}

// GetState 实现Parser接口
func (p *WebSocketParser) GetState() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	return map[string]interface{}{
		"buffer_length":   len(p.buffer),
		"fragment_length": len(p.fragmentBuffer),
		"fragment_opcode": p.fragmentOpCode,
		"is_server":       p.isServer,
		"max_frame_size":  p.maxFrameSize,
		"max_msg_size":    p.maxMsgSize,
	}
}

// =================  工具函数 =================

// isValidOpCode 验证操作码
func isValidOpCode(opCode uint8) bool {
	switch opCode {
	case OpCodeContinuation, OpCodeText, OpCodeBinary, OpCodeClose, OpCodePing, OpCodePong:
		return true
	}
	return false
}

// isControlFrame 是否为控制帧
func isControlFrame(opCode uint8) bool {
	return opCode >= 0x8
}

// generateMaskKey 生成掩码密钥
func generateMaskKey() []byte {
	maskKey := make([]byte, 4)
	rand.Read(maskKey)
	return maskKey
}
