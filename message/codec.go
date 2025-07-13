package message

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

// MessageCodec 消息编解码器
type MessageCodec struct {
	idGenerator *GlobalIDGenerator
}

// NewMessageCodec 创建消息编解码器
func NewMessageCodec() *MessageCodec {
	return &MessageCodec{
		idGenerator: NewGlobalIDGenerator(),
	}
}

// Serialize 序列化消息信封
func (c *MessageCodec) Serialize(envelope *MessageEnvelope) ([]byte, error) {
	if err := c.ValidateEnvelope(envelope); err != nil {
		return nil, fmt.Errorf("envelope validation failed: %v", err)
	}

	data, err := proto.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope: %v", err)
	}

	return data, nil
}

// Deserialize 反序列化消息信封
func (c *MessageCodec) Deserialize(data []byte) (*MessageEnvelope, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}

	envelope := &MessageEnvelope{}
	err := proto.Unmarshal(data, envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %v", err)
	}

	// 验证反序列化后的消息
	if err := c.ValidateEnvelope(envelope); err != nil {
		return nil, fmt.Errorf("deserialized envelope validation failed: %v", err)
	}

	return envelope, nil
}

// ValidateEnvelope 验证消息信封
func (c *MessageCodec) ValidateEnvelope(envelope *MessageEnvelope) error {
	if envelope == nil {
		return fmt.Errorf("envelope is nil")
	}

	// 验证连接序列号
	if envelope.ConnectionSeqId == 0 {
		return fmt.Errorf("connection_seq_id is required")
	}

	// 验证全局消息ID
	if len(envelope.MessageId) != GlobalIDLength {
		return fmt.Errorf("invalid message_id length: expected %d bytes, got %d", GlobalIDLength, len(envelope.MessageId))
	}

	globalID, err := FromBytes(envelope.MessageId)
	if err != nil {
		return fmt.Errorf("invalid message_id format: %v", err)
	}

	if !globalID.IsValid() {
		return fmt.Errorf("invalid message_id: validation failed")
	}

	// 验证消息内容
	if envelope.Message == nil {
		return fmt.Errorf("message content is required")
	}

	return nil
}

// ValidateSpecificMessage 验证具体消息类型
func (c *MessageCodec) ValidateSpecificMessage(envelope *MessageEnvelope) error {
	if err := c.ValidateEnvelope(envelope); err != nil {
		return err
	}

	switch msg := envelope.Message.(type) {
	case *MessageEnvelope_CreateRoom:
		return c.validateCreateRoom(msg.CreateRoom)
	case *MessageEnvelope_JoinRoom:
		return c.validateJoinRoom(msg.JoinRoom)
	case *MessageEnvelope_ChatMessage:
		return c.validateChatMessage(msg.ChatMessage)
	case *MessageEnvelope_MessageFetchRequest:
		return c.validateMessageFetchRequest(msg.MessageFetchRequest)
	case *MessageEnvelope_RoomMessageFetchRequest:
		return c.validateRoomMessageFetchRequest(msg.RoomMessageFetchRequest)
	case *MessageEnvelope_Heartbeat:
		return c.validateHeartbeat(msg.Heartbeat)
	default:
		// 对于ACK类型的消息和其他消息，允许通过基础验证
		return nil
	}
}

// CreateEnvelope 创建消息信封
func (c *MessageCodec) CreateEnvelope(userID, roomID uint32, loginID uint8, connSeqID uint64, messageContent interface{}) (*MessageEnvelope, error) {
	// 生成全局消息ID
	globalID := c.idGenerator.GenerateGlobalID(userID, roomID, loginID, 0) // custom字段暂时设为0

	envelope := &MessageEnvelope{
		ConnectionSeqId: connSeqID,
		MessageId:       globalID.ToBytes(),
	}

	// 设置具体消息内容
	switch msg := messageContent.(type) {
	case *AllocateRoomId:
		envelope.Message = &MessageEnvelope_AllocateRoomId{AllocateRoomId: msg}
	case *CreateRoom:
		envelope.Message = &MessageEnvelope_CreateRoom{CreateRoom: msg}
	case *JoinRoom:
		envelope.Message = &MessageEnvelope_JoinRoom{JoinRoom: msg}
	case *LeaveRoom:
		envelope.Message = &MessageEnvelope_LeaveRoom{LeaveRoom: msg}
	case *CloseRoom:
		envelope.Message = &MessageEnvelope_CloseRoom{CloseRoom: msg}
	case *ChatMessage:
		envelope.Message = &MessageEnvelope_ChatMessage{ChatMessage: msg}
	case *MessageFetchRequest:
		envelope.Message = &MessageEnvelope_MessageFetchRequest{MessageFetchRequest: msg}
	case *RoomMessageFetchRequest:
		envelope.Message = &MessageEnvelope_RoomMessageFetchRequest{RoomMessageFetchRequest: msg}
	case *Heartbeat:
		envelope.Message = &MessageEnvelope_Heartbeat{Heartbeat: msg}
	// ACK消息
	case *AllocateRoomIdAck:
		envelope.Message = &MessageEnvelope_AllocateRoomIdAck{AllocateRoomIdAck: msg}
	case *CreateRoomAck:
		envelope.Message = &MessageEnvelope_CreateRoomAck{CreateRoomAck: msg}
	case *JoinRoomAck:
		envelope.Message = &MessageEnvelope_JoinRoomAck{JoinRoomAck: msg}
	case *LeaveRoomAck:
		envelope.Message = &MessageEnvelope_LeaveRoomAck{LeaveRoomAck: msg}
	case *CloseRoomAck:
		envelope.Message = &MessageEnvelope_CloseRoomAck{CloseRoomAck: msg}
	case *ChatMessageAck:
		envelope.Message = &MessageEnvelope_ChatMessageAck{ChatMessageAck: msg}
	case *MessageFetchResponse:
		envelope.Message = &MessageEnvelope_MessageFetchResponse{MessageFetchResponse: msg}
	case *RoomMessageFetchResponse:
		envelope.Message = &MessageEnvelope_RoomMessageFetchResponse{RoomMessageFetchResponse: msg}
	case *HeartbeatAck:
		envelope.Message = &MessageEnvelope_HeartbeatAck{HeartbeatAck: msg}
	default:
		return nil, fmt.Errorf("unsupported message type: %T", messageContent)
	}

	return envelope, nil
}

// ExtractMessageInfo 提取消息信息
func (c *MessageCodec) ExtractMessageInfo(envelope *MessageEnvelope) (userID, roomID uint32, loginID uint8, timestamp int64, err error) {
	if err = c.ValidateEnvelope(envelope); err != nil {
		return 0, 0, 0, 0, err
	}

	globalID, err := FromBytes(envelope.MessageId)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to parse message_id: %v", err)
	}

	return globalID.ExtractUserID(),
		globalID.ExtractRoomID(),
		globalID.ExtractLoginID(),
		globalID.ExtractTimestamp(),
		nil
}

// 具体消息验证方法

func (c *MessageCodec) validateCreateRoom(msg *CreateRoom) error {
	if msg == nil {
		return fmt.Errorf("create_room message is nil")
	}
	if msg.RoomId == 0 {
		return fmt.Errorf("room_id is required")
	}
	if msg.Name == "" {
		return fmt.Errorf("room name is required")
	}
	if msg.MaxMembers == 0 {
		return fmt.Errorf("max_members must be greater than 0")
	}
	return nil
}

func (c *MessageCodec) validateJoinRoom(msg *JoinRoom) error {
	if msg == nil {
		return fmt.Errorf("join_room message is nil")
	}
	// password可以为空，对于无密码房间
	return nil
}

func (c *MessageCodec) validateChatMessage(msg *ChatMessage) error {
	if msg == nil {
		return fmt.Errorf("chat_message is nil")
	}
	if msg.Content == "" {
		return fmt.Errorf("message content is required")
	}
	if len(msg.Content) > 4096 { // 限制消息长度
		return fmt.Errorf("message content too long: max 4096 characters")
	}
	return nil
}

func (c *MessageCodec) validateMessageFetchRequest(msg *MessageFetchRequest) error {
	if msg == nil {
		return fmt.Errorf("message_fetch_request is nil")
	}
	if len(msg.MessageId) != GlobalIDLength {
		return fmt.Errorf("invalid message_id length")
	}
	return nil
}

func (c *MessageCodec) validateRoomMessageFetchRequest(msg *RoomMessageFetchRequest) error {
	if msg == nil {
		return fmt.Errorf("room_message_fetch_request is nil")
	}
	if msg.RoomId == 0 {
		return fmt.Errorf("room_id is required")
	}
	if msg.Limit == 0 || msg.Limit > 100 { // 限制每次拉取的消息数量
		return fmt.Errorf("limit must be between 1 and 100")
	}
	return nil
}

func (c *MessageCodec) validateHeartbeat(msg *Heartbeat) error {
	if msg == nil {
		return fmt.Errorf("heartbeat message is nil")
	}
	if msg.Timestamp == 0 {
		return fmt.Errorf("timestamp is required")
	}

	// 检查时间戳是否合理（不能距离当前时间太远）
	now := uint64(time.Now().UnixMilli())
	if msg.Timestamp > now+60*1000 { // 不能超过未来1分钟
		return fmt.Errorf("heartbeat timestamp too far in future")
	}
	if now > msg.Timestamp+60*1000 { // 不能超过过去1分钟
		return fmt.Errorf("heartbeat timestamp too old")
	}

	return nil
}

// 便捷函数
func Serialize(envelope *MessageEnvelope) ([]byte, error) {
	codec := NewMessageCodec()
	return codec.Serialize(envelope)
}

func Deserialize(data []byte) (*MessageEnvelope, error) {
	codec := NewMessageCodec()
	return codec.Deserialize(data)
}

func ValidateMessage(envelope *MessageEnvelope) error {
	codec := NewMessageCodec()
	return codec.ValidateSpecificMessage(envelope)
}
