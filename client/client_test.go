package client

import (
	"fmt"
	"liveChatroom/message"
	"testing"
)

// MockEventHandler 模拟事件处理器
type MockEventHandler struct {
	connected         bool
	disconnected      bool
	messagesReceived  int
	messagesSent      int
	messagesDelivered int
	messagesFailed    int
	roomsJoined       int
	errors            int
}

func (h *MockEventHandler) OnConnected(serverAddr string)                       { h.connected = true }
func (h *MockEventHandler) OnDisconnected(serverAddr string, err error)         { h.disconnected = true }
func (h *MockEventHandler) OnConnectionSwitched(fromAddr, toAddr string)        {}
func (h *MockEventHandler) OnMessageReceived(envelope *message.MessageEnvelope) { h.messagesReceived++ }
func (h *MockEventHandler) OnMessageSent(envelope *message.MessageEnvelope)     { h.messagesSent++ }
func (h *MockEventHandler) OnMessageDelivered(envelope *message.MessageEnvelope) {
	h.messagesDelivered++
}
func (h *MockEventHandler) OnMessageFailed(envelope *message.MessageEnvelope, err error) {
	h.messagesFailed++
}
func (h *MockEventHandler) OnRoomJoined(roomInfo *message.RoomInfo) { h.roomsJoined++ }
func (h *MockEventHandler) OnRoomLeft(roomID uint64)                {}
func (h *MockEventHandler) OnRoomMemberJoined(userID uint64)        {}
func (h *MockEventHandler) OnRoomMemberLeft(userID uint64)          {}
func (h *MockEventHandler) OnError(err error)                       { h.errors++ }

func TestNewLiveChatClient(t *testing.T) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()

	client := NewLiveChatClient(1001, 1, config, handler)

	if client == nil {
		t.Fatal("Client should not be nil")
	}

	if client.userID != 1001 {
		t.Errorf("Expected user ID 1001, got %d", client.userID)
	}

	if client.loginID != 1 {
		t.Errorf("Expected login ID 1, got %d", client.loginID)
	}

	if client.eventHandler != handler {
		t.Error("Event handler not set correctly")
	}

	if client.codec == nil {
		t.Error("Message codec should not be nil")
	}

	if client.connSeqGenerator == nil {
		t.Error("Connection sequence generator should not be nil")
	}

	if client.reliabilityManager == nil {
		t.Error("Reliability manager should not be nil")
	}

	if client.messageProcessor == nil {
		t.Error("Message processor should not be nil")
	}
}

func TestDefaultClientConfig(t *testing.T) {
	config := DefaultClientConfig()

	if config == nil {
		t.Fatal("Config should not be nil")
	}

	if len(config.ServerAddrs) == 0 {
		t.Error("Server addresses should not be empty")
	}

	if config.ConnectTimeout <= 0 {
		t.Error("Connect timeout should be positive")
	}

	if config.MessageTimeout <= 0 {
		t.Error("Message timeout should be positive")
	}

	if config.MaxRetries <= 0 {
		t.Error("Max retries should be positive")
	}

	if config.BufferSize <= 0 {
		t.Error("Buffer size should be positive")
	}
}

func TestClientLifecycle(t *testing.T) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()

	client := NewLiveChatClient(1001, 1, config, handler)

	// 测试初始状态
	if client.IsConnected() {
		t.Error("Client should not be connected initially")
	}

	// 注意：由于没有真实的服务器，Start()会失败
	// 这里我们只测试状态管理

	// 测试统计信息
	stats := client.GetStats()
	if stats == nil {
		t.Error("Stats should not be nil")
	}

	if stats.TotalMessagesSent != 0 {
		t.Error("Initial sent messages should be 0")
	}
}

func TestMessageCreation(t *testing.T) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()

	client := NewLiveChatClient(1001, 1, config, handler)

	// 测试聊天消息创建
	chatMsg := &message.ChatMessage{
		Content:     "Test message",
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}

	connSeq := client.connSeqGenerator.Next()
	envelope, err := client.codec.CreateEnvelope(client.userID, 12345, client.loginID, connSeq, chatMsg)

	if err != nil {
		t.Errorf("Failed to create envelope: %v", err)
	}

	if envelope == nil {
		t.Error("Envelope should not be nil")
	}

	// 验证消息内容
	userID, roomID, loginID, _, err := client.codec.ExtractMessageInfo(envelope)
	if err != nil {
		t.Errorf("Failed to extract message info: %v", err)
	}

	if userID != 1001 {
		t.Errorf("Expected user ID 1001, got %d", userID)
	}

	if roomID != 12345 {
		t.Errorf("Expected room ID 12345, got %d", roomID)
	}

	if loginID != 1 {
		t.Errorf("Expected login ID 1, got %d", loginID)
	}
}

func TestReliabilityManager(t *testing.T) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()

	client := NewLiveChatClient(1001, 1, config, handler)

	// 测试可靠性管理器
	rm := client.reliabilityManager
	if rm == nil {
		t.Fatal("Reliability manager should not be nil")
	}

	// 测试统计信息
	retries, fetches, pending := rm.GetStats()
	if retries != 0 || fetches != 0 || pending != 0 {
		t.Error("Initial stats should be zero")
	}
}

func TestMessageProcessor(t *testing.T) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()

	client := NewLiveChatClient(1001, 1, config, handler)

	// 测试消息处理器
	mp := client.messageProcessor
	if mp == nil {
		t.Fatal("Message processor should not be nil")
	}

	// 测试统计信息
	buffered, seen := mp.GetStats()
	if buffered != 0 || seen != 0 {
		t.Error("Initial stats should be zero")
	}
}

func TestConnectionSequenceGenerator(t *testing.T) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()

	client := NewLiveChatClient(1001, 1, config, handler)

	gen := client.connSeqGenerator

	// 测试序列号生成
	seq1 := gen.Next()
	seq2 := gen.Next()
	seq3 := gen.Next()

	if seq1 != 1 {
		t.Errorf("Expected first sequence to be 1, got %d", seq1)
	}

	if seq2 != 2 {
		t.Errorf("Expected second sequence to be 2, got %d", seq2)
	}

	if seq3 != 3 {
		t.Errorf("Expected third sequence to be 3, got %d", seq3)
	}

	if gen.Current() != 3 {
		t.Errorf("Expected current sequence to be 3, got %d", gen.Current())
	}
}

func TestEventHandlerCallbacks(t *testing.T) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()

	client := NewLiveChatClient(1001, 1, config, handler)

	// 模拟一些事件
	if client.eventHandler != nil {
		client.eventHandler.OnConnected("test-server")
		client.eventHandler.OnError(fmt.Errorf("test error"))
	}

	// 检查事件是否被正确处理
	if !handler.connected {
		t.Error("Connected event should have been called")
	}

	if handler.errors != 1 {
		t.Errorf("Expected 1 error event, got %d", handler.errors)
	}
}

// 性能测试
func BenchmarkMessageCreation(b *testing.B) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()
	client := NewLiveChatClient(1001, 1, config, handler)

	chatMsg := &message.ChatMessage{
		Content:     "Benchmark message",
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		connSeq := client.connSeqGenerator.Next()
		_, err := client.codec.CreateEnvelope(client.userID, 12345, client.loginID, connSeq, chatMsg)
		if err != nil {
			b.Errorf("Failed to create envelope: %v", err)
		}
	}
}

func BenchmarkSequenceGeneration(b *testing.B) {
	handler := &MockEventHandler{}
	config := DefaultClientConfig()
	client := NewLiveChatClient(1001, 1, config, handler)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.connSeqGenerator.Next()
	}
}
