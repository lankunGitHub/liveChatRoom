package http

import (
	"liveChatroom/util/net/protocol"
	"strings"
	"sync"
)

// HTTPProtocol HTTP协议实现
type HTTPProtocol struct {
	mu      sync.RWMutex
	parser  *HTTPParser
	builder *HTTPBuilder
	closed  bool
}

// NewHTTPProtocol 创建HTTP协议处理器
func NewHTTPProtocol() protocol.Protocol {
	return &HTTPProtocol{
		parser:  NewHTTPParser(),
		builder: NewHTTPBuilder(),
	}
}

// GetType 实现Protocol接口
func (p *HTTPProtocol) GetType() protocol.ProtocolType {
	return protocol.ProtocolHTTP
}

// GetParser 实现Protocol接口
func (p *HTTPProtocol) GetParser() protocol.Parser {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.parser
}

// GetBuilder 实现Protocol接口
func (p *HTTPProtocol) GetBuilder() protocol.Builder {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.builder
}

// CanHandle 实现Protocol接口
func (p *HTTPProtocol) CanHandle(data []byte) bool {
	if len(data) < 4 {
		return false
	}

	dataStr := string(data[:min(len(data), 16)])

	// 检查HTTP方法
	httpMethods := []string{"GET ", "POST", "PUT ", "DELE", "HEAD", "OPTI", "TRAC", "CONN", "PATC"}
	for _, method := range httpMethods {
		if strings.HasPrefix(dataStr, method) {
			return true
		}
	}

	// 检查HTTP响应
	if strings.HasPrefix(dataStr, "HTTP/") {
		return true
	}

	return false
}

// HandleUpgrade 实现Protocol接口
func (p *HTTPProtocol) HandleUpgrade(request protocol.Request) (protocol.Protocol, protocol.Response, error) {
	httpReq, ok := request.(*HTTPRequest)
	if !ok {
		return nil, nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeProtocolMismatch,
			Message: "request is not HTTP request",
		}
	}

	if !httpReq.IsWebSocketUpgrade() {
		return nil, nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeUpgradeError,
			Message: "not a WebSocket upgrade request",
		}
	}

	// 生成WebSocket升级响应
	responseData, err := p.builder.BuildWebSocketUpgradeResponse(httpReq.WebSocketKey, httpReq.WebSocketProtocol)
	if err != nil {
		return nil, nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeUpgradeError,
			Message: "failed to build upgrade response",
			Cause:   err,
		}
	}

	// 创建响应消息
	response := &HTTPResponse{
		StatusCode:        101,
		StatusText:        "Switching Protocols",
		Version:           "HTTP/1.1",
		Connection:        "Upgrade",
		Upgrade:           "websocket",
		WebSocketAccept:   extractWebSocketAccept(responseData),
		WebSocketProtocol: httpReq.WebSocketProtocol,
	}
	response.Type = "http_response"
	response.Headers = map[string]string{
		"connection":           "Upgrade",
		"upgrade":              "websocket",
		"sec-websocket-accept": response.WebSocketAccept,
	}
	if response.WebSocketProtocol != "" {
		response.Headers["sec-websocket-protocol"] = response.WebSocketProtocol
	}

	// 获取WebSocket协议处理器
	wsProtocol, err := protocol.Get(protocol.ProtocolWebSocket)
	if err != nil {
		return nil, nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeUpgradeError,
			Message: "WebSocket protocol not available",
			Cause:   err,
		}
	}

	return wsProtocol, response, nil
}

// IsUpgradeRequest 实现Protocol接口
func (p *HTTPProtocol) IsUpgradeRequest(request protocol.Request) bool {
	httpReq, ok := request.(*HTTPRequest)
	if !ok {
		return false
	}

	return httpReq.IsWebSocketUpgrade()
}

// GetUpgradeTarget 实现Protocol接口
func (p *HTTPProtocol) GetUpgradeTarget(request protocol.Request) protocol.ProtocolType {
	if !p.IsUpgradeRequest(request) {
		return protocol.ProtocolUnknown
	}

	httpReq, ok := request.(*HTTPRequest)
	if !ok {
		return protocol.ProtocolUnknown
	}

	if httpReq.IsWebSocketUpgrade() {
		return protocol.ProtocolWebSocket
	}

	return protocol.ProtocolUnknown
}

// Close 实现Protocol接口
func (p *HTTPProtocol) Close() error {
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
func (p *HTTPProtocol) Clone() protocol.Protocol {
	return NewHTTPProtocol()
}

// =================  工具方法 =================

// extractWebSocketAccept 从响应数据中提取WebSocket Accept
func extractWebSocketAccept(responseData []byte) string {
	responseStr := string(responseData)
	lines := strings.Split(responseStr, "\r\n")

	for _, line := range lines {
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-accept:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}

	return ""
}

// min 返回两个整数的最小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// =================  注册HTTP协议 =================

func init() {
	// 自动注册HTTP协议到全局注册表
	protocol.Register(protocol.ProtocolHTTP, func() protocol.Protocol {
		return NewHTTPProtocol()
	})
}
