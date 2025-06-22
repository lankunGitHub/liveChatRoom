package websocket

import (
	"encoding/binary"
	"fmt"
	"liveChatroom/util/net/protocol"
	"sync"
)

// WebSocketBuilder WebSocket协议构建器
type WebSocketBuilder struct {
	mu       sync.Mutex
	isServer bool
}

// NewWebSocketBuilder 创建WebSocket构建器
func NewWebSocketBuilder(isServer bool) *WebSocketBuilder {
	return &WebSocketBuilder{
		isServer: isServer,
	}
}

// GetProtocolType 实现Builder接口
func (b *WebSocketBuilder) GetProtocolType() protocol.ProtocolType {
	return protocol.ProtocolWebSocket
}

// BuildMessage 实现Builder接口
func (b *WebSocketBuilder) BuildMessage(msg protocol.Message) ([]byte, error) {
	wsMsg, ok := msg.(*WebSocketMessage)
	if !ok {
		return nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeInvalidMessage,
			Message: "message is not a WebSocket message",
		}
	}

	return b.buildFrame(wsMsg.OpCode, wsMsg.Payload, wsMsg.Final)
}

// BuildRequest 实现Builder接口（WebSocket没有请求概念）
func (b *WebSocketBuilder) BuildRequest(method, path string, headers map[string]string, body []byte) ([]byte, error) {
	return nil, &protocol.ProtocolError{
		Type:    protocol.ProtocolWebSocket,
		Code:    protocol.ErrCodeNotSupported,
		Message: "WebSocket does not support request building",
	}
}

// BuildResponse 实现Builder接口（WebSocket没有响应概念）
func (b *WebSocketBuilder) BuildResponse(statusCode int, statusText string, headers map[string]string, body []byte) ([]byte, error) {
	return nil, &protocol.ProtocolError{
		Type:    protocol.ProtocolWebSocket,
		Code:    protocol.ErrCodeNotSupported,
		Message: "WebSocket does not support response building",
	}
}

// BuildError 实现Builder接口
func (b *WebSocketBuilder) BuildError(statusCode int, message string) ([]byte, error) {
	closeCode := uint16(CloseInternalError)
	if statusCode == 400 {
		closeCode = uint16(CloseProtocolError)
	} else if statusCode == 413 {
		closeCode = uint16(CloseMessageTooBig)
	}

	return b.BuildCloseFrame(closeCode, message)
}

// Reset 实现Builder接口
func (b *WebSocketBuilder) Reset() {
	// WebSocket构建器无状态，无需重置
}

// Close 实现Builder接口
func (b *WebSocketBuilder) Close() error {
	return nil
}

// =================  WebSocket特定构建方法 =================

// buildFrame 构建WebSocket帧
func (b *WebSocketBuilder) buildFrame(opCode uint8, payload []byte, fin bool) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !isValidOpCode(opCode) {
		return nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeBuildError,
			Message: fmt.Sprintf("invalid opcode: %d", opCode),
		}
	}

	// 验证控制帧
	if isControlFrame(opCode) {
		if len(payload) > 125 {
			return nil, &protocol.ProtocolError{
				Type:    protocol.ProtocolWebSocket,
				Code:    protocol.ErrCodeBuildError,
				Message: "control frame payload too large",
			}
		}
	}

	payloadLen := len(payload)
	headerLen := 2

	// 计算头部长度
	if payloadLen >= 65536 {
		headerLen += 8
	} else if payloadLen >= 126 {
		headerLen += 2
	}

	// 客户端需要掩码
	var maskKey []byte
	if !b.isServer {
		maskKey = generateMaskKey()
		headerLen += 4
	}

	// 构建帧
	frame := make([]byte, headerLen+payloadLen)

	// 第一个字节：FIN + RSV + OpCode
	frame[0] = opCode
	if fin {
		frame[0] |= 0x80
	}

	// 第二个字节：MASK + Payload Length
	if !b.isServer {
		frame[1] = 0x80 // 设置掩码位
	}

	offset := 2

	if payloadLen >= 65536 {
		frame[1] |= 127
		binary.BigEndian.PutUint64(frame[offset:], uint64(payloadLen))
		offset += 8
	} else if payloadLen >= 126 {
		frame[1] |= 126
		binary.BigEndian.PutUint16(frame[offset:], uint16(payloadLen))
		offset += 2
	} else {
		frame[1] |= uint8(payloadLen)
	}

	// 掩码密钥
	if maskKey != nil {
		copy(frame[offset:], maskKey)
		offset += 4
	}

	// 载荷数据
	if payloadLen > 0 {
		copy(frame[offset:], payload)

		// 客户端掩码处理
		if maskKey != nil {
			for i := 0; i < payloadLen; i++ {
				frame[offset+i] ^= maskKey[i%4]
			}
		}
	}

	return frame, nil
}

// BuildTextFrame 构建文本帧
func (b *WebSocketBuilder) BuildTextFrame(data []byte) ([]byte, error) {
	return b.buildFrame(OpCodeText, data, true)
}

// BuildBinaryFrame 构建二进制帧
func (b *WebSocketBuilder) BuildBinaryFrame(data []byte) ([]byte, error) {
	return b.buildFrame(OpCodeBinary, data, true)
}

// BuildCloseFrame 构建关闭帧
func (b *WebSocketBuilder) BuildCloseFrame(code uint16, reason string) ([]byte, error) {
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload[:2], code)
	copy(payload[2:], reason)

	return b.buildFrame(OpCodeClose, payload, true)
}

// BuildPingFrame 构建Ping帧
func (b *WebSocketBuilder) BuildPingFrame(data []byte) ([]byte, error) {
	if len(data) > 125 {
		return nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeBuildError,
			Message: "ping frame payload too large",
		}
	}

	return b.buildFrame(OpCodePing, data, true)
}

// BuildPongFrame 构建Pong帧
func (b *WebSocketBuilder) BuildPongFrame(data []byte) ([]byte, error) {
	if len(data) > 125 {
		return nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeBuildError,
			Message: "pong frame payload too large",
		}
	}

	return b.buildFrame(OpCodePong, data, true)
}

// BuildFragmentedFrames 构建分片帧
func (b *WebSocketBuilder) BuildFragmentedFrames(opCode uint8, data []byte, maxFrameSize int) ([][]byte, error) {
	if maxFrameSize <= 0 {
		maxFrameSize = 32768 // 32KB默认
	}

	if !isValidOpCode(opCode) || isControlFrame(opCode) {
		return nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolWebSocket,
			Code:    protocol.ErrCodeBuildError,
			Message: "invalid opcode for fragmentation",
		}
	}

	var frames [][]byte
	dataLen := len(data)

	for i := 0; i < dataLen; i += maxFrameSize {
		end := i + maxFrameSize
		if end > dataLen {
			end = dataLen
		}

		chunk := data[i:end]
		frameOpCode := uint8(OpCodeContinuation)
		fin := false

		if i == 0 {
			// 第一帧
			frameOpCode = opCode
		}
		if end == dataLen {
			// 最后一帧
			fin = true
		}

		frame, err := b.buildFrame(frameOpCode, chunk, fin)
		if err != nil {
			return nil, err
		}

		frames = append(frames, frame)
	}

	return frames, nil
}

// =================  便利方法 =================

// BuildTextMessage 构建文本消息
func (b *WebSocketBuilder) BuildTextMessage(text string) ([]byte, error) {
	return b.BuildTextFrame([]byte(text))
}

// BuildJSONMessage 构建JSON消息
func (b *WebSocketBuilder) BuildJSONMessage(data []byte) ([]byte, error) {
	return b.BuildTextFrame(data)
}

// BuildPing 构建Ping消息
func (b *WebSocketBuilder) BuildPing() ([]byte, error) {
	return b.BuildPingFrame(nil)
}

// BuildPong 构建Pong消息
func (b *WebSocketBuilder) BuildPong() ([]byte, error) {
	return b.BuildPongFrame(nil)
}

// BuildClose 构建标准关闭消息
func (b *WebSocketBuilder) BuildClose() ([]byte, error) {
	return b.BuildCloseFrame(CloseNormalClosure, "")
}

// BuildCloseGoingAway 构建离开关闭消息
func (b *WebSocketBuilder) BuildCloseGoingAway(reason string) ([]byte, error) {
	return b.BuildCloseFrame(CloseGoingAway, reason)
}
