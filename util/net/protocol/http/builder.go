package http

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"liveChatroom/util/net/protocol"
	"strings"
	"sync"
	"time"
)

// HTTPBuilder HTTP协议构建器
type HTTPBuilder struct {
	mu sync.Mutex

	// 响应构建状态
	statusCode int
	statusText string
	version    string
	headers    map[string]string
	body       []byte

	// 配置
	serverName  string
	autoHeaders bool

	// 常量
	websocketMagic string
}

// NewHTTPBuilder 创建HTTP构建器
func NewHTTPBuilder() *HTTPBuilder {
	return &HTTPBuilder{
		version:        "HTTP/1.1",
		headers:        make(map[string]string),
		serverName:     "NetServer/1.0",
		autoHeaders:    true,
		websocketMagic: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
	}
}

// GetProtocolType 实现Builder接口
func (b *HTTPBuilder) GetProtocolType() protocol.ProtocolType {
	return protocol.ProtocolHTTP
}

// BuildMessage 实现Builder接口
func (b *HTTPBuilder) BuildMessage(msg protocol.Message) ([]byte, error) {
	switch httpMsg := msg.(type) {
	case *HTTPRequest:
		return b.buildRequest(httpMsg)
	case *HTTPResponse:
		return b.buildResponse(httpMsg)
	case protocol.Request:
		return b.BuildRequest(httpMsg.GetMethod(), httpMsg.GetPath(),
			httpMsg.GetHeaders(), httpMsg.GetPayload())
	case protocol.Response:
		return b.BuildResponse(httpMsg.GetStatusCode(), httpMsg.GetStatusText(),
			httpMsg.GetHeaders(), httpMsg.GetPayload())
	default:
		return nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeInvalidMessage,
			Message: "unsupported message type for HTTP builder",
		}
	}
}

// BuildRequest 实现Builder接口
func (b *HTTPBuilder) BuildRequest(method, path string, headers map[string]string, body []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if method == "" {
		method = "GET"
	}
	if path == "" {
		path = "/"
	}

	var result strings.Builder

	// 请求行
	result.WriteString(fmt.Sprintf("%s %s HTTP/1.1\r\n", method, path))

	// 合并头部
	allHeaders := make(map[string]string)
	if headers != nil {
		for k, v := range headers {
			allHeaders[k] = v
		}
	}

	// 自动添加头部
	if b.autoHeaders {
		if _, exists := allHeaders["User-Agent"]; !exists {
			allHeaders["User-Agent"] = "NetClient/1.0"
		}
		if _, exists := allHeaders["Accept"]; !exists {
			allHeaders["Accept"] = "*/*"
		}
		if body != nil && len(body) > 0 {
			if _, exists := allHeaders["Content-Length"]; !exists {
				allHeaders["Content-Length"] = fmt.Sprintf("%d", len(body))
			}
		}
	}

	// 写入头部
	for name, value := range allHeaders {
		result.WriteString(fmt.Sprintf("%s: %s\r\n", name, value))
	}

	result.WriteString("\r\n")

	// 写入body
	if body != nil && len(body) > 0 {
		result.Write(body)
	}

	return []byte(result.String()), nil
}

// BuildResponse 实现Builder接口
func (b *HTTPBuilder) BuildResponse(statusCode int, statusText string, headers map[string]string, body []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if statusCode == 0 {
		statusCode = 200
	}
	if statusText == "" {
		statusText = getStatusText(statusCode)
	}

	var result strings.Builder

	// 状态行
	result.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", statusCode, statusText))

	// 合并头部
	allHeaders := make(map[string]string)
	if headers != nil {
		for k, v := range headers {
			allHeaders[k] = v
		}
	}

	// 自动添加头部
	if b.autoHeaders {
		if _, exists := allHeaders["Server"]; !exists && b.serverName != "" {
			allHeaders["Server"] = b.serverName
		}
		if _, exists := allHeaders["Date"]; !exists {
			allHeaders["Date"] = time.Now().UTC().Format(time.RFC1123)
		}
		if body != nil && len(body) > 0 {
			if _, exists := allHeaders["Content-Length"]; !exists {
				allHeaders["Content-Length"] = fmt.Sprintf("%d", len(body))
			}
		}
		if _, exists := allHeaders["Connection"]; !exists {
			allHeaders["Connection"] = "keep-alive"
		}
	}

	// 写入头部
	for name, value := range allHeaders {
		result.WriteString(fmt.Sprintf("%s: %s\r\n", name, value))
	}

	result.WriteString("\r\n")

	// 写入body
	if body != nil && len(body) > 0 {
		result.Write(body)
	}

	return []byte(result.String()), nil
}

// BuildError 实现Builder接口
func (b *HTTPBuilder) BuildError(statusCode int, message string) ([]byte, error) {
	if statusCode == 0 {
		statusCode = 500
	}
	if message == "" {
		message = getStatusText(statusCode)
	}

	errorHTML := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>%d %s</title>
</head>
<body>
    <h1>%d %s</h1>
    <p>%s</p>
    <hr>
    <p><small>%s</small></p>
</body>
</html>`, statusCode, getStatusText(statusCode), statusCode, getStatusText(statusCode), message, b.serverName)

	headers := map[string]string{
		"Content-Type": "text/html; charset=utf-8",
	}

	return b.BuildResponse(statusCode, getStatusText(statusCode), headers, []byte(errorHTML))
}

// Reset 实现Builder接口
func (b *HTTPBuilder) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.statusCode = 0
	b.statusText = ""
	b.version = "HTTP/1.1"
	b.headers = make(map[string]string)
	b.body = nil
}

// Close 实现Builder接口
func (b *HTTPBuilder) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.headers = nil
	b.body = nil
	return nil
}

// =================  内部构建方法 =================

// buildRequest 构建HTTP请求
func (b *HTTPBuilder) buildRequest(req *HTTPRequest) ([]byte, error) {
	headers := req.GetHeaders()
	return b.BuildRequest(req.Method, req.Path, headers, req.Body)
}

// buildResponse 构建HTTP响应
func (b *HTTPBuilder) buildResponse(resp *HTTPResponse) ([]byte, error) {
	headers := resp.GetHeaders()
	return b.BuildResponse(resp.StatusCode, resp.StatusText, headers, resp.Body)
}

// =================  WebSocket支持方法 =================

// BuildWebSocketUpgradeResponse 构建WebSocket升级响应
func (b *HTTPBuilder) BuildWebSocketUpgradeResponse(websocketKey string, wsProtocol string) ([]byte, error) {
	if websocketKey == "" {
		return nil, &protocol.ProtocolError{
			Type:    protocol.ProtocolHTTP,
			Code:    protocol.ErrCodeInvalidMessage,
			Message: "WebSocket key cannot be empty",
		}
	}

	// 计算WebSocket Accept
	acceptKey := b.generateWebSocketAccept(websocketKey)

	headers := map[string]string{
		"Connection":           "Upgrade",
		"Upgrade":              "websocket",
		"Sec-WebSocket-Accept": acceptKey,
	}

	if wsProtocol != "" {
		headers["Sec-WebSocket-Protocol"] = wsProtocol
	}

	return b.BuildResponse(101, "Switching Protocols", headers, nil)
}

// generateWebSocketAccept 生成WebSocket Accept密钥
func (b *HTTPBuilder) generateWebSocketAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte(b.websocketMagic))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// =================  便利方法 =================

// SetServerName 设置服务器名称
func (b *HTTPBuilder) SetServerName(name string) *HTTPBuilder {
	b.serverName = name
	return b
}

// SetAutoHeaders 设置是否自动添加头部
func (b *HTTPBuilder) SetAutoHeaders(auto bool) *HTTPBuilder {
	b.autoHeaders = auto
	return b
}

// BuildOK 构建200 OK响应
func (b *HTTPBuilder) BuildOK(contentType string, body []byte) ([]byte, error) {
	headers := map[string]string{}
	if contentType != "" {
		headers["Content-Type"] = contentType
	}
	return b.BuildResponse(200, "OK", headers, body)
}

// BuildNotFound 构建404 Not Found响应
func (b *HTTPBuilder) BuildNotFound(message string) ([]byte, error) {
	if message == "" {
		message = "The requested resource was not found"
	}
	return b.BuildError(404, message)
}

// BuildBadRequest 构建400 Bad Request响应
func (b *HTTPBuilder) BuildBadRequest(message string) ([]byte, error) {
	if message == "" {
		message = "The request was malformed"
	}
	return b.BuildError(400, message)
}

// BuildInternalServerError 构建500 Internal Server Error响应
func (b *HTTPBuilder) BuildInternalServerError(message string) ([]byte, error) {
	if message == "" {
		message = "An internal server error occurred"
	}
	return b.BuildError(500, message)
}

// BuildMethodNotAllowed 构建405 Method Not Allowed响应
func (b *HTTPBuilder) BuildMethodNotAllowed(allowedMethods []string) ([]byte, error) {
	headers := map[string]string{}
	if len(allowedMethods) > 0 {
		headers["Allow"] = strings.Join(allowedMethods, ", ")
	}
	return b.BuildResponse(405, "Method Not Allowed", headers, []byte("Method Not Allowed"))
}

// BuildRedirect 构建重定向响应
func (b *HTTPBuilder) BuildRedirect(location string, permanent bool) ([]byte, error) {
	statusCode := 302 // Found (temporary)
	if permanent {
		statusCode = 301 // Moved Permanently
	}

	headers := map[string]string{
		"Location": location,
	}

	body := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>%d %s</title>
</head>
<body>
    <h1>%d %s</h1>
    <p>The document has moved <a href="%s">here</a>.</p>
</body>
</html>`, statusCode, getStatusText(statusCode), statusCode, getStatusText(statusCode), location)

	headers["Content-Type"] = "text/html; charset=utf-8"

	return b.BuildResponse(statusCode, getStatusText(statusCode), headers, []byte(body))
}

// getStatusText 获取状态码对应的文本
func getStatusText(code int) string {
	switch code {
	case 100:
		return "Continue"
	case 101:
		return "Switching Protocols"
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 202:
		return "Accepted"
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
	case 406:
		return "Not Acceptable"
	case 408:
		return "Request Timeout"
	case 409:
		return "Conflict"
	case 410:
		return "Gone"
	case 411:
		return "Length Required"
	case 413:
		return "Payload Too Large"
	case 414:
		return "URI Too Long"
	case 415:
		return "Unsupported Media Type"
	case 426:
		return "Upgrade Required"
	case 429:
		return "Too Many Requests"
	case 500:
		return "Internal Server Error"
	case 501:
		return "Not Implemented"
	case 502:
		return "Bad Gateway"
	case 503:
		return "Service Unavailable"
	case 504:
		return "Gateway Timeout"
	case 505:
		return "HTTP Version Not Supported"
	default:
		return "Unknown"
	}
}
