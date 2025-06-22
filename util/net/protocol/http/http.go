package http

import (
	"bufio"
	"bytes"
	"fmt"
	"liveChatroom/util/net/base/buffer"
	"liveChatroom/util/net/protocol"
	"strconv"
	"strings"
	"sync"
)

// =================  消息实现 =================

// HTTPRequest HTTP请求消息
type HTTPRequest struct {
	protocol.BaseMessage
	Method   string            `json:"method"`
	Path     string            `json:"path"`
	Version  string            `json:"version"`
	URI      string            `json:"uri"`
	RawPath  string            `json:"raw_path"`
	Query    map[string]string `json:"query"`
	Fragment string            `json:"fragment"`
	Body     []byte            `json:"body"`

	// 连接相关
	Host          string `json:"host"`
	Connection    string `json:"connection"`
	ContentType   string `json:"content_type"`
	ContentLength int64  `json:"content_length"`

	// WebSocket升级相关
	Upgrade           string `json:"upgrade"`
	WebSocketKey      string `json:"websocket_key"`
	WebSocketVersion  string `json:"websocket_version"`
	WebSocketProtocol string `json:"websocket_protocol"`
}

// GetMethod 实现Request接口
func (r *HTTPRequest) GetMethod() string {
	return r.Method
}

// GetPath 实现Request接口
func (r *HTTPRequest) GetPath() string {
	return r.Path
}

// GetVersion 实现Request接口
func (r *HTTPRequest) GetVersion() string {
	return r.Version
}

// IsWebSocketUpgrade 检查是否为WebSocket升级请求
func (r *HTTPRequest) IsWebSocketUpgrade() bool {
	return strings.ToLower(r.Connection) == "upgrade" &&
		strings.ToLower(r.Upgrade) == "websocket" &&
		r.WebSocketKey != ""
}

// Clone 实现Message接口
func (r *HTTPRequest) Clone() protocol.Message {
	clone := &HTTPRequest{
		Method:            r.Method,
		Path:              r.Path,
		Version:           r.Version,
		URI:               r.URI,
		RawPath:           r.RawPath,
		Fragment:          r.Fragment,
		Body:              make([]byte, len(r.Body)),
		Host:              r.Host,
		Connection:        r.Connection,
		ContentType:       r.ContentType,
		ContentLength:     r.ContentLength,
		Upgrade:           r.Upgrade,
		WebSocketKey:      r.WebSocketKey,
		WebSocketVersion:  r.WebSocketVersion,
		WebSocketProtocol: r.WebSocketProtocol,
	}

	copy(clone.Body, r.Body)

	if r.Query != nil {
		clone.Query = make(map[string]string, len(r.Query))
		for k, v := range r.Query {
			clone.Query[k] = v
		}
	}

	// 复制基础消息字段
	clone.BaseMessage = *(r.BaseMessage.Clone().(*protocol.BaseMessage))

	return clone
}

// HTTPResponse HTTP响应消息
type HTTPResponse struct {
	protocol.BaseMessage
	StatusCode int    `json:"status_code"`
	StatusText string `json:"status_text"`
	Version    string `json:"version"`
	Body       []byte `json:"body"`

	// 连接相关
	Connection    string `json:"connection"`
	ContentType   string `json:"content_type"`
	ContentLength int64  `json:"content_length"`

	// WebSocket升级相关
	Upgrade           string `json:"upgrade"`
	WebSocketAccept   string `json:"websocket_accept"`
	WebSocketProtocol string `json:"websocket_protocol"`
}

// GetStatusCode 实现Response接口
func (r *HTTPResponse) GetStatusCode() int {
	return r.StatusCode
}

// GetStatusText 实现Response接口
func (r *HTTPResponse) GetStatusText() string {
	return r.StatusText
}

// GetVersion 实现Response接口
func (r *HTTPResponse) GetVersion() string {
	return r.Version
}

// Clone 实现Message接口
func (r *HTTPResponse) Clone() protocol.Message {
	clone := &HTTPResponse{
		StatusCode:        r.StatusCode,
		StatusText:        r.StatusText,
		Version:           r.Version,
		Body:              make([]byte, len(r.Body)),
		Connection:        r.Connection,
		ContentType:       r.ContentType,
		ContentLength:     r.ContentLength,
		Upgrade:           r.Upgrade,
		WebSocketAccept:   r.WebSocketAccept,
		WebSocketProtocol: r.WebSocketProtocol,
	}

	copy(clone.Body, r.Body)

	// 复制基础消息字段
	clone.BaseMessage = *(r.BaseMessage.Clone().(*protocol.BaseMessage))

	return clone
}

// =================  解析器实现 =================

// HTTPParser HTTP协议解析器
type HTTPParser struct {
	mu              sync.Mutex
	state           parseState
	buffer          []byte
	headerComplete  bool
	bodyComplete    bool
	contentLength   int64
	chunked         bool
	currentRequest  *HTTPRequest
	currentResponse *HTTPResponse

	// 配置
	maxHeaderSize  int64
	maxBodySize    int64
	maxRequestLine int64
}

type parseState int

const (
	stateRequestLine parseState = iota
	stateResponseLine
	stateHeaders
	stateBody
	stateChunkedBody
	stateComplete
)

// NewHTTPParser 创建HTTP解析器
func NewHTTPParser() *HTTPParser {
	return &HTTPParser{
		state:          stateRequestLine,
		maxHeaderSize:  8192,     // 8KB
		maxBodySize:    10485760, // 10MB
		maxRequestLine: 8192,     // 8KB
	}
}

// GetProtocolType 实现Parser接口
func (p *HTTPParser) GetProtocolType() protocol.ProtocolType {
	return protocol.ProtocolHTTP
}

// Feed 实现Parser接口
func (p *HTTPParser) Feed(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = append(p.buffer, data...)

	// 检查缓冲区大小限制
	if len(p.buffer) > int(p.maxHeaderSize+p.maxBodySize) {
		return &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeParseError,
			Message: "buffer size exceeded",
		}
	}

	return nil
}

// Parse 实现Parser接口
func (p *HTTPParser) Parse(buf buffer.Buffer) ([]protocol.Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 从缓冲区读取数据
	data := buf.ReadAll()
	if len(data) > 0 {
		p.buffer = append(p.buffer, data...)
	}

	var messages []protocol.Message

	for {
		msg, consumed, err := p.parseNext()
		if err != nil {
			return nil, err
		}

		if msg != nil {
			messages = append(messages, msg)
		}

		if consumed == 0 {
			break // 需要更多数据
		}

		// 移除已解析的数据
		p.buffer = p.buffer[consumed:]
	}

	return messages, nil
}

// parseNext 解析下一个完整消息
func (p *HTTPParser) parseNext() (protocol.Message, int, error) {
	if len(p.buffer) == 0 {
		return nil, 0, nil
	}

	switch p.state {
	case stateRequestLine:
		return p.parseRequestLine()
	case stateResponseLine:
		return p.parseResponseLine()
	case stateHeaders:
		return p.parseHeaders()
	case stateBody:
		return p.parseBody()
	case stateChunkedBody:
		return p.parseChunkedBody()
	case stateComplete:
		msg := p.completeMessage()
		p.Reset()
		return msg, 0, nil
	default:
		return nil, 0, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeInvalidState,
			Message: fmt.Sprintf("invalid parser state: %d", p.state),
		}
	}
}

// parseRequestLine 解析请求行
func (p *HTTPParser) parseRequestLine() (protocol.Message, int, error) {
	lineEnd := bytes.Index(p.buffer, []byte("\r\n"))
	if lineEnd == -1 {
		if len(p.buffer) > int(p.maxRequestLine) {
			return nil, 0, &protocol.ProtocolError{
				Type:    protocol.ProtocolHTTP,
				Code:    protocol.ErrCodeParseError,
				Message: "request line too long",
			}
		}
		return nil, 0, nil // 需要更多数据
	}

	line := string(p.buffer[:lineEnd])
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return nil, 0, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeParseError,
			Message: "invalid request line format",
		}
	}

	// 检查是否为HTTP响应
	if strings.HasPrefix(parts[0], "HTTP/") {
		p.state = stateResponseLine
		return p.parseResponseLine()
	}

	// 解析请求
	p.currentRequest = &HTTPRequest{
		Method:  parts[0],
		Path:    parts[1],
		Version: parts[2],
		URI:     parts[1],
	}

	// 解析查询参数
	if idx := strings.Index(parts[1], "?"); idx != -1 {
		p.currentRequest.Path = parts[1][:idx]
		p.currentRequest.Query = parseQuery(parts[1][idx+1:])
	}

	p.state = stateHeaders
	return nil, lineEnd + 2, nil
}

// parseResponseLine 解析响应行
func (p *HTTPParser) parseResponseLine() (protocol.Message, int, error) {
	lineEnd := bytes.Index(p.buffer, []byte("\r\n"))
	if lineEnd == -1 {
		if len(p.buffer) > int(p.maxRequestLine) {
			return nil, 0, &protocol.ProtocolError{
				Type:    protocol.ProtocolHTTP,
				Code:    protocol.ErrCodeParseError,
				Message: "response line too long",
			}
		}
		return nil, 0, nil // 需要更多数据
	}

	line := string(p.buffer[:lineEnd])
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 {
		return nil, 0, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeParseError,
			Message: "invalid response line format",
		}
	}

	statusCode, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, 0, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeParseError,
			Message: "invalid status code",
			Cause:   err,
		}
	}

	statusText := ""
	if len(parts) == 3 {
		statusText = parts[2]
	}

	p.currentResponse = &HTTPResponse{
		Version:    parts[0],
		StatusCode: statusCode,
		StatusText: statusText,
	}

	p.state = stateHeaders
	return nil, lineEnd + 2, nil
}

// parseHeaders 解析头部
func (p *HTTPParser) parseHeaders() (protocol.Message, int, error) {
	headerEnd := bytes.Index(p.buffer, []byte("\r\n\r\n"))
	if headerEnd == -1 {
		if len(p.buffer) > int(p.maxHeaderSize) {
			return nil, 0, &protocol.ProtocolError{
				Type:    protocol.ProtocolHTTP,
				Code:    protocol.ErrCodeParseError,
				Message: "headers too large",
			}
		}
		return nil, 0, nil // 需要更多数据
	}

	headerData := p.buffer[:headerEnd]
	headers := make(map[string]string)

	// 解析头部
	scanner := bufio.NewScanner(bytes.NewReader(headerData))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		colonIdx := strings.Index(line, ":")
		if colonIdx == -1 {
			continue // 跳过无效行
		}

		name := strings.TrimSpace(line[:colonIdx])
		value := strings.TrimSpace(line[colonIdx+1:])
		headers[strings.ToLower(name)] = value
	}

	// 设置头部到消息
	if p.currentRequest != nil {
		p.setRequestHeaders(p.currentRequest, headers)
	} else if p.currentResponse != nil {
		p.setResponseHeaders(p.currentResponse, headers)
	}

	p.headerComplete = true

	// 确定body解析策略
	if p.shouldParseBody() {
		if p.chunked {
			p.state = stateChunkedBody
		} else {
			p.state = stateBody
		}
	} else {
		p.state = stateComplete
	}

	return nil, headerEnd + 4, nil
}

// parseBody 解析固定长度body
func (p *HTTPParser) parseBody() (protocol.Message, int, error) {
	if p.contentLength == 0 {
		p.state = stateComplete
		return nil, 0, nil
	}

	if len(p.buffer) < int(p.contentLength) {
		return nil, 0, nil // 需要更多数据
	}

	body := make([]byte, p.contentLength)
	copy(body, p.buffer[:p.contentLength])

	if p.currentRequest != nil {
		p.currentRequest.Body = body
	} else if p.currentResponse != nil {
		p.currentResponse.Body = body
	}

	p.bodyComplete = true
	p.state = stateComplete

	return nil, int(p.contentLength), nil
}

// parseChunkedBody 解析分块编码body
func (p *HTTPParser) parseChunkedBody() (protocol.Message, int, error) {
	var body []byte
	consumed := 0
	buffer := p.buffer

	for {
		// 查找chunk size行
		lineEnd := bytes.Index(buffer, []byte("\r\n"))
		if lineEnd == -1 {
			return nil, 0, nil // 需要更多数据
		}

		// 解析chunk size
		chunkSizeLine := string(buffer[:lineEnd])
		chunkSize, err := strconv.ParseInt(strings.Split(chunkSizeLine, ";")[0], 16, 64)
		if err != nil {
			return nil, 0, &protocol.ProtocolError{
				Type:    protocol.ProtocolHTTP,
				Code:    protocol.ErrCodeParseError,
				Message: "invalid chunk size",
				Cause:   err,
			}
		}

		consumed += lineEnd + 2
		buffer = buffer[lineEnd+2:]

		if chunkSize == 0 {
			// 最后一个chunk，查找结束标记
			if len(buffer) < 2 {
				return nil, 0, nil // 需要更多数据
			}

			consumed += 2 // 跳过最终的\r\n
			break
		}

		// 检查chunk数据是否完整
		if len(buffer) < int(chunkSize)+2 {
			return nil, 0, nil // 需要更多数据
		}

		// 复制chunk数据
		chunkData := make([]byte, chunkSize)
		copy(chunkData, buffer[:chunkSize])
		body = append(body, chunkData...)

		consumed += int(chunkSize) + 2
		buffer = buffer[chunkSize+2:]
	}

	if p.currentRequest != nil {
		p.currentRequest.Body = body
	} else if p.currentResponse != nil {
		p.currentResponse.Body = body
	}

	p.bodyComplete = true
	p.state = stateComplete

	return nil, consumed, nil
}

// setRequestHeaders 设置请求头
func (p *HTTPParser) setRequestHeaders(req *HTTPRequest, headers map[string]string) {
	req.Headers = headers

	// 设置常用头部字段
	if host := headers["host"]; host != "" {
		req.Host = host
	}
	if conn := headers["connection"]; conn != "" {
		req.Connection = conn
	}
	if ct := headers["content-type"]; ct != "" {
		req.ContentType = ct
	}
	if cl := headers["content-length"]; cl != "" {
		if length, err := strconv.ParseInt(cl, 10, 64); err == nil {
			req.ContentLength = length
			p.contentLength = length
		}
	}
	if te := headers["transfer-encoding"]; te == "chunked" {
		p.chunked = true
	}

	// WebSocket相关头部
	if upgrade := headers["upgrade"]; upgrade != "" {
		req.Upgrade = upgrade
	}
	if wsKey := headers["sec-websocket-key"]; wsKey != "" {
		req.WebSocketKey = wsKey
	}
	if wsVersion := headers["sec-websocket-version"]; wsVersion != "" {
		req.WebSocketVersion = wsVersion
	}
	if wsProtocol := headers["sec-websocket-protocol"]; wsProtocol != "" {
		req.WebSocketProtocol = wsProtocol
	}
}

// setResponseHeaders 设置响应头
func (p *HTTPParser) setResponseHeaders(resp *HTTPResponse, headers map[string]string) {
	resp.Headers = headers

	// 设置常用头部字段
	if conn := headers["connection"]; conn != "" {
		resp.Connection = conn
	}
	if ct := headers["content-type"]; ct != "" {
		resp.ContentType = ct
	}
	if cl := headers["content-length"]; cl != "" {
		if length, err := strconv.ParseInt(cl, 10, 64); err == nil {
			resp.ContentLength = length
			p.contentLength = length
		}
	}
	if te := headers["transfer-encoding"]; te == "chunked" {
		p.chunked = true
	}

	// WebSocket相关头部
	if upgrade := headers["upgrade"]; upgrade != "" {
		resp.Upgrade = upgrade
	}
	if wsAccept := headers["sec-websocket-accept"]; wsAccept != "" {
		resp.WebSocketAccept = wsAccept
	}
	if wsProtocol := headers["sec-websocket-protocol"]; wsProtocol != "" {
		resp.WebSocketProtocol = wsProtocol
	}
}

// shouldParseBody 判断是否应该解析body
func (p *HTTPParser) shouldParseBody() bool {
	if p.chunked {
		return true
	}
	if p.contentLength > 0 {
		if p.contentLength > p.maxBodySize {
			return false // body太大，跳过
		}
		return true
	}
	return false
}

// completeMessage 完成消息构建
func (p *HTTPParser) completeMessage() protocol.Message {
	var msg protocol.Message

	if p.currentRequest != nil {
		p.currentRequest.Type = "http_request"
		if p.currentRequest.Body != nil {
			p.currentRequest.Payload = p.currentRequest.Body
		}
		msg = p.currentRequest
	} else if p.currentResponse != nil {
		p.currentResponse.Type = "http_response"
		if p.currentResponse.Body != nil {
			p.currentResponse.Payload = p.currentResponse.Body
		}
		msg = p.currentResponse
	}

	return msg
}

// HasPendingMessage 实现Parser接口
func (p *HTTPParser) HasPendingMessage() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state == stateComplete
}

// Reset 实现Parser接口
func (p *HTTPParser) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.state = stateRequestLine
	p.buffer = p.buffer[:0]
	p.headerComplete = false
	p.bodyComplete = false
	p.contentLength = 0
	p.chunked = false
	p.currentRequest = nil
	p.currentResponse = nil
}

// Close 实现Parser接口
func (p *HTTPParser) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = nil
	p.currentRequest = nil
	p.currentResponse = nil
	return nil
}

// GetState 实现Parser接口
func (p *HTTPParser) GetState() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	return map[string]interface{}{
		"state":           p.state,
		"buffer_length":   len(p.buffer),
		"header_complete": p.headerComplete,
		"body_complete":   p.bodyComplete,
		"content_length":  p.contentLength,
		"chunked":         p.chunked,
	}
}

// parseQuery 解析查询参数
func parseQuery(query string) map[string]string {
	result := make(map[string]string)
	if query == "" {
		return result
	}

	pairs := strings.Split(query, "&")
	for _, pair := range pairs {
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, "=", 2)
		key := parts[0]
		value := ""
		if len(parts) == 2 {
			value = parts[1]
		}

		result[key] = value
	}

	return result
}
