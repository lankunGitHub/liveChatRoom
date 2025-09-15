package protocol

import (
	"fmt"
	"liveChatroom/util/net/base/buffer"
	"sync"
)

// ProtocolType 协议类型
type ProtocolType int

const (
	ProtocolUnknown ProtocolType = iota
	ProtocolHTTP
	ProtocolWebSocket
	ProtocolTCP
	ProtocolUDP
)

// String 返回协议类型字符串
func (p ProtocolType) String() string {
	switch p {
	case ProtocolHTTP:
		return "HTTP"
	case ProtocolWebSocket:
		return "WebSocket"
	case ProtocolTCP:
		return "TCP"
	case ProtocolUDP:
		return "UDP"
	default:
		return "Unknown"
	}
}

// =================  消息接口 =================

// Message 协议消息接口
type Message interface {
	// GetType 获取消息类型
	GetType() string

	// GetPayload 获取消息载荷
	GetPayload() []byte

	// GetHeaders 获取消息头（如果有的话）
	GetHeaders() map[string]string

	// GetMetadata 获取消息元数据
	GetMetadata() map[string]interface{}

	// Clone 克隆消息
	Clone() Message
}

// Request 请求消息接口
type Request interface {
	Message
	GetMethod() string
	GetPath() string
	GetVersion() string
}

// Response 响应消息接口
type Response interface {
	Message
	GetStatusCode() int
	GetStatusText() string
	GetVersion() string
}

// =================  解析器接口 =================

// Parser 协议解析器接口
type Parser interface {
	// GetProtocolType 获取协议类型
	GetProtocolType() ProtocolType

	// Feed 喂入数据进行解析
	Feed(data []byte) error

	// Parse 从缓冲区解析消息
	Parse(buf buffer.Buffer) ([]Message, error)

	// HasPendingMessage 检查是否有待处理的完整消息
	HasPendingMessage() bool

	// Reset 重置解析器状态
	Reset()

	// Close 关闭解析器，释放资源
	Close() error

	// GetState 获取解析器状态（用于调试）
	GetState() map[string]interface{}
}

// =================  构建器接口 =================

// Builder 协议构建器接口
type Builder interface {
	// GetProtocolType 获取协议类型
	GetProtocolType() ProtocolType

	// BuildMessage 构建协议消息
	BuildMessage(msg Message) ([]byte, error)

	// BuildRequest 构建请求消息
	BuildRequest(method, path string, headers map[string]string, body []byte) ([]byte, error)

	// BuildResponse 构建响应消息
	BuildResponse(statusCode int, statusText string, headers map[string]string, body []byte) ([]byte, error)

	// BuildError 构建错误消息
	BuildError(statusCode int, message string) ([]byte, error)

	// Reset 重置构建器状态
	Reset()

	// Close 关闭构建器，释放资源
	Close() error
}

// =================  协议处理器接口 =================

// Protocol 协议处理器接口
type Protocol interface {
	// GetType 获取协议类型
	GetType() ProtocolType

	// GetParser 获取解析器实例
	GetParser() Parser

	// GetBuilder 获取构建器实例
	GetBuilder() Builder

	// CanHandle 检查是否可以处理指定的数据
	CanHandle(data []byte) bool

	// HandleUpgrade 处理协议升级
	HandleUpgrade(request Request) (Protocol, Response, error)

	// IsUpgradeRequest 检查是否为协议升级请求
	IsUpgradeRequest(request Request) bool

	// GetUpgradeTarget 获取升级目标协议类型
	GetUpgradeTarget(request Request) ProtocolType

	// Close 关闭协议处理器
	Close() error

	// Clone 克隆协议处理器（用于多连接场景）
	Clone() Protocol
}

// =================  检测器接口 =================

// Detector 协议检测器接口
type Detector interface {
	// Detect 检测协议类型
	Detect(data []byte) ProtocolType

	// AddPattern 添加协议检测模式
	AddPattern(protocolType ProtocolType, patterns []DetectionPattern)

	// RemovePattern 移除协议检测模式
	RemovePattern(protocolType ProtocolType)

	// GetConfidence 获取检测置信度
	GetConfidence(data []byte, protocolType ProtocolType) float64
}

// DetectionPattern 检测模式
type DetectionPattern struct {
	Pattern []byte  // 模式字节
	Offset  int     // 偏移量
	Mask    []byte  // 掩码（可选）
	Weight  float64 // 权重
}

// =================  注册表接口 =================

// Registry 协议注册表接口
type Registry interface {
	// Register 注册协议
	Register(protocolType ProtocolType, factory ProtocolFactory) error

	// Unregister 注销协议
	Unregister(protocolType ProtocolType) error

	// Get 获取协议实例
	Get(protocolType ProtocolType) (Protocol, error)

	// GetParser 获取解析器实例
	GetParser(protocolType ProtocolType) (Parser, error)

	// GetBuilder 获取构建器实例
	GetBuilder(protocolType ProtocolType) (Builder, error)

	// GetDetector 获取协议检测器
	GetDetector() Detector

	// List 列出所有已注册的协议
	List() []ProtocolType

	// Detect 检测并获取协议实例
	Detect(data []byte) (Protocol, ProtocolType, error)
}

// ProtocolFactory 协议工厂函数
type ProtocolFactory func() Protocol

// =================  错误定义 =================

// ProtocolError 协议错误
type ProtocolError struct {
	Type    ProtocolType
	Code    string
	Message string
	Cause   error
}

// Error 实现error接口
func (e *ProtocolError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("protocol %s error [%s]: %s, cause: %v",
			e.Type.String(), e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("protocol %s error [%s]: %s",
		e.Type.String(), e.Code, e.Message)
}

// Unwrap 实现unwrap接口
func (e *ProtocolError) Unwrap() error {
	return e.Cause
}

// 错误代码常量
const (
	ErrCodeUnknownProtocol  = "UNKNOWN_PROTOCOL"
	ErrCodeParseError       = "PARSE_ERROR"
	ErrCodeBuildError       = "BUILD_ERROR"
	ErrCodeUpgradeError     = "UPGRADE_ERROR"
	ErrCodeInvalidMessage   = "INVALID_MESSAGE"
	ErrCodeNotSupported     = "NOT_SUPPORTED"
	ErrCodeProtocolMismatch = "PROTOCOL_MISMATCH"
	ErrCodeInvalidState     = "INVALID_STATE"
	ErrCodeProtocolError    = "PROTOCOL_ERROR"
	ErrCodeInvalidFrameData = "INVALID_FRAME_DATA"
)

// =================  基础实现 =================

// BaseMessage 基础消息实现
type BaseMessage struct {
	Type     string                 `json:"type"`
	Payload  []byte                 `json:"payload"`
	Headers  map[string]string      `json:"headers"`
	Metadata map[string]interface{} `json:"metadata"`
}

// GetType 实现Message接口
func (m *BaseMessage) GetType() string {
	return m.Type
}

// GetPayload 实现Message接口
func (m *BaseMessage) GetPayload() []byte {
	return m.Payload
}

// GetHeaders 实现Message接口
func (m *BaseMessage) GetHeaders() map[string]string {
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	return m.Headers
}

// GetMetadata 实现Message接口
func (m *BaseMessage) GetMetadata() map[string]interface{} {
	if m.Metadata == nil {
		m.Metadata = make(map[string]interface{})
	}
	return m.Metadata
}

// Clone 实现Message接口
func (m *BaseMessage) Clone() Message {
	clone := &BaseMessage{
		Type:    m.Type,
		Payload: make([]byte, len(m.Payload)),
	}

	copy(clone.Payload, m.Payload)

	if m.Headers != nil {
		clone.Headers = make(map[string]string, len(m.Headers))
		for k, v := range m.Headers {
			clone.Headers[k] = v
		}
	}

	if m.Metadata != nil {
		clone.Metadata = make(map[string]interface{}, len(m.Metadata))
		for k, v := range m.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}

// =================  检测器实现 =================

// SmartDetector 智能协议检测器
type SmartDetector struct {
	mu       sync.RWMutex
	patterns map[ProtocolType][]DetectionPattern
}

// NewSmartDetector 创建智能检测器
func NewSmartDetector() Detector {
	detector := &SmartDetector{
		patterns: make(map[ProtocolType][]DetectionPattern),
	}

	// 添加默认HTTP检测模式
	detector.AddPattern(ProtocolHTTP, []DetectionPattern{
		{Pattern: []byte("GET "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("POST "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("PUT "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("DELETE "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("HEAD "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("OPTIONS "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("TRACE "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("CONNECT "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("PATCH "), Offset: 0, Weight: 1.0},
		{Pattern: []byte("HTTP/1."), Offset: 0, Weight: 1.0}, // HTTP响应
		{Pattern: []byte("HTTP/2"), Offset: 0, Weight: 1.0},  // HTTP/2
	})

	return detector
}

// Detect 实现Detector接口
func (d *SmartDetector) Detect(data []byte) ProtocolType {
	if len(data) == 0 {
		return ProtocolUnknown
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	bestType := ProtocolUnknown
	bestConfidence := 0.0

	for protocolType, patterns := range d.patterns {
		confidence := d.calculateConfidence(data, patterns)
		if confidence > bestConfidence {
			bestConfidence = confidence
			bestType = protocolType
		}
	}

	// 需要最低置信度阈值
	if bestConfidence < 0.5 {
		return ProtocolUnknown
	}

	return bestType
}

// AddPattern 实现Detector接口
func (d *SmartDetector) AddPattern(protocolType ProtocolType, patterns []DetectionPattern) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.patterns[protocolType] = patterns
}

// RemovePattern 实现Detector接口
func (d *SmartDetector) RemovePattern(protocolType ProtocolType) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.patterns, protocolType)
}

// GetConfidence 实现Detector接口
func (d *SmartDetector) GetConfidence(data []byte, protocolType ProtocolType) float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	patterns, exists := d.patterns[protocolType]
	if !exists {
		return 0.0
	}

	return d.calculateConfidence(data, patterns)
}

// calculateConfidence 计算置信度
// 同一协议的多个模式是"互斥备选项"（如 HTTP 的各个方法动词，
// 一条数据只会命中其中一个），因此命中任意一个即可判定该协议。
// 若把所有备选项权重累加为分母，单个命中时置信度恒为 1/N，
// 永远达不到 Detect 的 0.5 阈值导致检测失效。
func (d *SmartDetector) calculateConfidence(data []byte, patterns []DetectionPattern) float64 {
	for _, pattern := range patterns {
		if d.matchPattern(data, pattern) {
			return 1.0
		}
	}
	return 0.0
}

// matchPattern 匹配模式
func (d *SmartDetector) matchPattern(data []byte, pattern DetectionPattern) bool {
	if len(data) < pattern.Offset+len(pattern.Pattern) {
		return false
	}

	start := pattern.Offset
	for i, b := range pattern.Pattern {
		dataByte := data[start+i]

		// 应用掩码（如果有）
		if pattern.Mask != nil && i < len(pattern.Mask) {
			dataByte &= pattern.Mask[i]
			b &= pattern.Mask[i]
		}

		if dataByte != b {
			return false
		}
	}

	return true
}

// =================  注册表实现 =================

// SimpleRegistry 简单协议注册表
type SimpleRegistry struct {
	mu        sync.RWMutex
	factories map[ProtocolType]ProtocolFactory
	detector  Detector
}

// NewRegistry 创建协议注册表
func NewRegistry() Registry {
	return &SimpleRegistry{
		factories: make(map[ProtocolType]ProtocolFactory),
		detector:  NewSmartDetector(),
	}
}

// Register 实现Registry接口
func (r *SimpleRegistry) Register(protocolType ProtocolType, factory ProtocolFactory) error {
	if factory == nil {
		return &ProtocolError{
			Type:    protocolType,
			Code:    ErrCodeInvalidMessage,
			Message: "protocol factory cannot be nil",
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.factories[protocolType] = factory
	return nil
}

// Unregister 实现Registry接口
func (r *SimpleRegistry) Unregister(protocolType ProtocolType) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.factories, protocolType)
	return nil
}

// Get 实现Registry接口
func (r *SimpleRegistry) Get(protocolType ProtocolType) (Protocol, error) {
	r.mu.RLock()
	factory, exists := r.factories[protocolType]
	r.mu.RUnlock()

	if !exists {
		return nil, &ProtocolError{
			Type:    protocolType,
			Code:    ErrCodeUnknownProtocol,
			Message: "protocol not registered",
		}
	}

	return factory(), nil
}

// GetParser 实现Registry接口
func (r *SimpleRegistry) GetParser(protocolType ProtocolType) (Parser, error) {
	protocol, err := r.Get(protocolType)
	if err != nil {
		return nil, err
	}

	return protocol.GetParser(), nil
}

// GetBuilder 实现Registry接口
func (r *SimpleRegistry) GetBuilder(protocolType ProtocolType) (Builder, error) {
	protocol, err := r.Get(protocolType)
	if err != nil {
		return nil, err
	}

	return protocol.GetBuilder(), nil
}

// GetDetector 实现Registry接口
func (r *SimpleRegistry) GetDetector() Detector {
	return r.detector
}

// List 实现Registry接口
func (r *SimpleRegistry) List() []ProtocolType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]ProtocolType, 0, len(r.factories))
	for protocolType := range r.factories {
		types = append(types, protocolType)
	}

	return types
}

// Detect 实现Registry接口
func (r *SimpleRegistry) Detect(data []byte) (Protocol, ProtocolType, error) {
	protocolType := r.detector.Detect(data)
	if protocolType == ProtocolUnknown {
		return nil, ProtocolUnknown, &ProtocolError{
			Type:    ProtocolUnknown,
			Code:    ErrCodeUnknownProtocol,
			Message: "unable to detect protocol",
		}
	}

	protocol, err := r.Get(protocolType)
	if err != nil {
		return nil, protocolType, err
	}

	return protocol, protocolType, nil
}

// =================  全局注册表 =================

var (
	globalRegistry = NewRegistry()
	registryMutex  sync.RWMutex
)

// Register 注册协议到全局注册表
func Register(protocolType ProtocolType, factory ProtocolFactory) error {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	return globalRegistry.Register(protocolType, factory)
}

// Unregister 从全局注册表注销协议
func Unregister(protocolType ProtocolType) error {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	return globalRegistry.Unregister(protocolType)
}

// Get 从全局注册表获取协议
func Get(protocolType ProtocolType) (Protocol, error) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	return globalRegistry.Get(protocolType)
}

// GetParser 从全局注册表获取解析器
func GetParser(protocolType ProtocolType) (Parser, error) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	return globalRegistry.GetParser(protocolType)
}

// GetBuilder 从全局注册表获取构建器
func GetBuilder(protocolType ProtocolType) (Builder, error) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	return globalRegistry.GetBuilder(protocolType)
}

// DetectProtocol 检测协议类型
func DetectProtocol(data []byte) ProtocolType {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	return globalRegistry.GetDetector().Detect(data)
}

// DetectAndGet 检测并获取协议实例
func DetectAndGet(data []byte) (Protocol, ProtocolType, error) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	return globalRegistry.Detect(data)
}

// List 列出所有已注册的协议
func List() []ProtocolType {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	return globalRegistry.List()
}

// SetGlobalRegistry 设置全局注册表（用于测试或自定义）
func SetGlobalRegistry(registry Registry) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	globalRegistry = registry
}
