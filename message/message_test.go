package message

import (
	"bytes"
	"testing"
	"time"
)

func TestGlobalIDGenerator(t *testing.T) {
	generator := NewGlobalIDGenerator()

	// 测试基本ID生成
	id1 := generator.GenerateGlobalID(12345, 67890, 1, 0)
	if id1.IsZero() {
		t.Error("Generated ID should not be zero")
	}

	// 验证ID有效性
	if !id1.IsValid() {
		t.Error("Generated ID should be valid")
	}

	// 测试ID唯一性
	id2 := generator.GenerateGlobalID(12345, 67890, 1, 0)
	if id1.Equal(id2) {
		t.Error("Two consecutive IDs should be different")
	}

	// 验证ID组成部分提取
	extractedUserID := id1.ExtractUserID()
	extractedRoomID := id1.ExtractRoomID()
	extractedLoginID := id1.ExtractLoginID()

	if extractedUserID != 12345 {
		t.Errorf("Expected user ID 12345, got %d", extractedUserID)
	}
	if extractedRoomID != 67890 {
		t.Errorf("Expected room ID 67890, got %d", extractedRoomID)
	}
	if extractedLoginID != 1 {
		t.Errorf("Expected login ID 1, got %d", extractedLoginID)
	}

	// 测试时间戳
	timestamp := id1.ExtractTimestamp()
	now := time.Now().UnixMilli()
	if timestamp > now || now-timestamp > 1000 { // 允许1秒误差
		t.Errorf("Timestamp seems incorrect: %d vs %d", timestamp, now)
	}
}

func TestGlobalIDComparison(t *testing.T) {
	generator := NewGlobalIDGenerator()

	// 生成两个不同的ID
	id1 := generator.GenerateGlobalID(1, 1, 1, 0)
	time.Sleep(1 * time.Millisecond) // 确保时间戳不同
	id2 := generator.GenerateGlobalID(1, 1, 1, 0)

	// 比较测试
	if id1.Compare(id2) >= 0 {
		t.Error("id1 should be less than id2")
	}

	if id2.Compare(id1) <= 0 {
		t.Error("id2 should be greater than id1")
	}

	if id1.Compare(id1) != 0 {
		t.Error("id1 should equal itself")
	}
}

func TestGlobalIDSerialization(t *testing.T) {
	generator := NewGlobalIDGenerator()
	originalID := generator.GenerateGlobalID(54321, 98765, 2, 5)

	// 测试字节序列化
	data := originalID.ToBytes()
	if len(data) != GlobalIDLength {
		t.Errorf("Expected %d bytes, got %d", GlobalIDLength, len(data))
	}

	// 测试反序列化
	restoredID, err := FromBytes(data)
	if err != nil {
		t.Errorf("Failed to restore ID from bytes: %v", err)
	}

	if !originalID.Equal(restoredID) {
		t.Error("Restored ID should equal original ID")
	}

	// 测试字符串表示
	str := originalID.String()
	if len(str) != 32 { // 16字节 = 32个十六进制字符
		t.Errorf("Expected 32 characters in hex string, got %d", len(str))
	}
}

func TestMessageCodec(t *testing.T) {
	codec := NewMessageCodec()

	// 创建测试消息
	chatMsg := &ChatMessage{
		Content:     "Hello, World!",
		MessageType: MessageType_MESSAGE_TYPE_TEXT,
	}

	// 创建消息信封
	envelope, err := codec.CreateEnvelope(12345, 67890, 1, 1001, chatMsg)
	if err != nil {
		t.Errorf("Failed to create envelope: %v", err)
	}

	// 验证信封
	if err := codec.ValidateSpecificMessage(envelope); err != nil {
		t.Errorf("Envelope validation failed: %v", err)
	}

	// 序列化
	data, err := codec.Serialize(envelope)
	if err != nil {
		t.Errorf("Failed to serialize envelope: %v", err)
	}

	// 反序列化
	restored, err := codec.Deserialize(data)
	if err != nil {
		t.Errorf("Failed to deserialize envelope: %v", err)
	}

	// 验证内容
	if !bytes.Equal(envelope.MessageId, restored.MessageId) {
		t.Error("Message IDs should match")
	}

	if envelope.ConnectionSeqId != restored.ConnectionSeqId {
		t.Error("Connection sequence IDs should match")
	}

	// 提取消息信息
	userID, roomID, loginID, timestamp, err := codec.ExtractMessageInfo(restored)
	if err != nil {
		t.Errorf("Failed to extract message info: %v", err)
	}

	if userID != 12345 {
		t.Errorf("Expected user ID 12345, got %d", userID)
	}
	if roomID != 67890 {
		t.Errorf("Expected room ID 67890, got %d", roomID)
	}
	if loginID != 1 {
		t.Errorf("Expected login ID 1, got %d", loginID)
	}

	now := time.Now().UnixMilli()
	if timestamp > now || now-timestamp > 1000 {
		t.Errorf("Timestamp seems incorrect: %d vs %d", timestamp, now)
	}
}

func TestConnSeqGenerator(t *testing.T) {
	gen := NewConnSeqGenerator()

	// 测试初始值
	if gen.Current() != 0 {
		t.Error("Initial sequence should be 0")
	}

	// 测试递增
	seq1 := gen.Next()
	seq2 := gen.Next()

	if seq1 != 1 {
		t.Errorf("First sequence should be 1, got %d", seq1)
	}
	if seq2 != 2 {
		t.Errorf("Second sequence should be 2, got %d", seq2)
	}

	// 测试当前值
	if gen.Current() != 2 {
		t.Errorf("Current sequence should be 2, got %d", gen.Current())
	}

	// 测试重置
	gen.Reset()
	if gen.Current() != 0 {
		t.Error("Sequence should be 0 after reset")
	}
}

func TestMessageValidation(t *testing.T) {
	codec := NewMessageCodec()

	// 测试无效消息类型的验证

	// 1. 测试空的CreateRoom消息
	createRoom := &CreateRoom{
		RoomId:     0,  // 无效的房间ID
		Name:       "", // 无效的房间名
		MaxMembers: 0,  // 无效的最大成员数
	}

	envelope, err := codec.CreateEnvelope(1, 1, 1, 1, createRoom)
	if err != nil {
		t.Errorf("Failed to create envelope: %v", err)
	}

	err = codec.ValidateSpecificMessage(envelope)
	if err == nil {
		t.Error("Should have validation error for invalid CreateRoom message")
	}

	// 2. 测试空的ChatMessage
	chatMsg := &ChatMessage{
		Content: "", // 空内容应该无效
	}

	envelope2, err := codec.CreateEnvelope(1, 1, 1, 2, chatMsg)
	if err != nil {
		t.Errorf("Failed to create envelope: %v", err)
	}

	err = codec.ValidateSpecificMessage(envelope2)
	if err == nil {
		t.Error("Should have validation error for empty ChatMessage content")
	}
}

// 性能测试
func BenchmarkGlobalIDGeneration(b *testing.B) {
	generator := NewGlobalIDGenerator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generator.GenerateGlobalID(uint32(i), uint32(i%1000), uint8(i%16), uint8(i%64))
	}
}

func BenchmarkMessageSerialization(b *testing.B) {
	codec := NewMessageCodec()
	chatMsg := &ChatMessage{
		Content:     "Hello, World!",
		MessageType: MessageType_MESSAGE_TYPE_TEXT,
	}

	envelope, _ := codec.CreateEnvelope(12345, 67890, 1, 1001, chatMsg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Serialize(envelope)
		if err != nil {
			b.Errorf("Serialization failed: %v", err)
		}
	}
}
